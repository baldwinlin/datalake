#coding:utf-8
'''
AirbyteCancelForced.py
Object          : 強制取消 Airbyte 同步作業
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 基於 AirbyteExecutionImpl 設計，重用共同功能，僅支援直接輸入 job_id 和 connection_id
Parameters      :
Output          :
********************************************************************************
Modify          :
'''
from service.AirbyteCancel import *
from crypto.Aes256Crypto import *

import logging
from logger import Logger
import time
import requests
import json
import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AirbyteCancelForcedImpl(AirbyteCancel):
    def __init__(self, main_config, config, args):
        # config 是從 airbyte.conf 來的配置
        self.config = config
        self.workspace_ids = config['AIRBYTE']['WORKSPACE_IDS']
        self.airbyte_root_api = config['AIRBYTE']['AIRBYTE_ROOT_API']
        
        # 測試環境 正式執行時需要註解掉
        # self.user = config['AIRBYTE']['CLIENT_ID']
        # self.db_sec = config['AIRBYTE']['CLIENT_SECRET']

        self.sec_file = self.config.get('AIRBYTE','SEC_FILE')
        self.key_file = self.config.get('AIRBYTE','KEY_FILE')
        #正式環境 正式執行需解開註解
        self.user, self.sec_str = readSecFile(self.sec_file)
        self.salt = readSaltFile(self.key_file)
        self.db_sec = get_gpg_decrypt(self.sec_str, self.salt)

        self.args_str = args
        if self.args_str:
            self.args = json.loads(self.args_str)
        else:
            raise Exception("args 不得為空值")
    
        self.connection_name = str(self.args.get("connection_name"))
        self.poll_sec = int(self.args.get("poll_sec", 180)) 
        self.timeout_sec = int(self.args.get("timeout_sec", 3600)) 

        self.job_id = None


        # 初始化 logger 變數
        self.main_config = main_config
        self.logger_main = None
    
    def _initialize_logger(self):
        # 讀取日誌配置，優先使用 main_config，否則從 main.conf 讀取
        log_config = self.main_config
        airbyte_log_path = f"{log_config['LOG']['LOG_PATH']}/airbyte"
        

        # 創建 Logger 實例 - 
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        logger_file_name = f"[Cancel]{self.connection_name}_{timestamp}"
        Logger.Logger(airbyte_log_path, logger_file_name) # 主要日誌

        # 更新 AirbyteExecution 的 logger_main attribute
        self.logger_main = logging.getLogger(logger_file_name) # 取得主要 logger 實例

    def run(self):
        """執行強制取消流程"""
        # 初始化 logger
        self._initialize_logger()

        try:
            response = self.getConnectionId()
        except requests.exceptions.HTTPError as e:
            self.logger_main.error(e)
            return False
        except Exception as e:
            self.logger_main.error(f"取得 connectionId 失敗，非預期的錯誤: {e}")
            return False

        if response is None:
            self.logger_main.error(f"找不到 {self.connection_name} 對應的 connection Id")
            return False
        
        if response:
            self.logger_main.info(f"找到 {self.connection_name} 對應的 connection Id: {response}")

            try:
                job_id, job_status = self.getSyncingJobId()
            except requests.exceptions.HTTPError as e:
                self.logger_main.error(e)
                return False
            except Exception as e:
                self.logger_main.error(f"獲取同步作業ID，非預期的錯誤: {e}")
                return False 
            
            if job_id is None or job_status is None:
                self.logger_main.error(f"無法獲取{self.connection_name}底下的同步作業id和狀態 或是沒有同步的作業在 running 或是 pending")
                return False
            else:    
                self.logger_main.info(f"找到{job_id}, 其狀態為{job_status}")
                self.logger_main.info(f"啟動 Airbyte 強制取消 {self.connection_name} 的同步作業 - job_id: {job_id}")


            try:
                # 直接取消指定的作業
                cancel_result = self.cancelJob(self.job_id)
                if cancel_result is False:
                    self.logger_main.error(f"取消作業 {self.job_id} 失敗")
                    return False

                # 等待取消完成
                final_status = self.waitForCancellation(self.job_id)
                if final_status is None:
                    self.logger_main.error(f"無法取得作業 {self.job_id} 的最終狀態")
                    return False
                elif final_status == "cancelled":
                    self.logger_main.info(f"作業 {self.job_id} 已成功取消")
                    return True
                else:
                    self.logger_main.error(f"作業 {self.job_id} 最終狀態: {final_status}")
                    return False
                    
            except requests.exceptions.HTTPError as e:
                self.logger_main.error(e)
                return False
            except Exception as e:
                self.logger_main.error(f"取消作業失敗，非預期的錯誤: {e}")
                return False
        
    def getAccessToken(self):
        access_token_api = f"{self.airbyte_root_api}/applications/token"
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        payload = {"client_id": self.user, "client_secret": self.db_sec}
        response = requests.post(url=access_token_api, json=payload, headers=headers, timeout=20, verify=False)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"取得 access token 遇到 HTTP 錯誤，請檢查登入 airbyte 所需參數。[Airbyte]: {e}")

        self.logger_main.info(f"取得 access token 用於強制取消作業")
        token = response.json().get("access_token")
        return token
    
    def getConnectionId(self):
        offset = 0
        limit = 2000
        list_connections_api = f"{self.airbyte_root_api}/connections?workspaceIds={self.workspace_ids}&offset={offset}&limit={limit}"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.getAccessToken()}"
        }        
        self.logger_main.info(f"嘗試抓取 {self.connection_name} 對應的 connectionId....先取得 workspace 內的連線列表")
        scanned = 0
        available_names = []

        while list_connections_api:

            response = requests.get(url=list_connections_api, headers=headers, timeout=20, verify= False)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise requests.exceptions.HTTPError(f"取得 connectionId 遇到 HTTP 錯誤，請檢查登入 airbyte 所需參數。[Airbyte]: {e}")

            connections = response.json().get("data", [])
            scanned += len(connections)
            self.logger_main.info(f"workspace 在本頁擁有的連線數量: {len(connections)}，累計掃描{scanned}個連線")

            if not connections:
                return None
    
            # 使用 connection name 指定要執行的 connection
            for connection in connections:
                name = connection.get("name")
                if name:
                    available_names.append(name)
                if name == self.connection_name:
                    self.connection_id = connection.get("connectionId")
                    target_id = self.connection_id
                    return target_id

            # 如果找不到對應的 connection Id，則切換到下一頁
            if len(connections) < limit:
                break  # 沒有更多連線了，結束循環

            offset += limit
            list_connections_api = f"{self.airbyte_root_api}/connections?workspaceIds={self.workspace_ids}&offset={offset}&limit={limit}"
            self.logger_main.info(f"尚未找到 {self.connection_name} 對應的 connectionId，切換到下一頁繼續找，offset={offset}, limit={limit}")

        self.connection_id = None
        return None


    def getSyncingJobId(self):

        list_running_jobs_api = f"{self.airbyte_root_api}/jobs?connectionId={self.connection_id}&jobType=sync&status=running"
        list_pending_jobs_api = f"{self.airbyte_root_api}/jobs?connectionId={self.connection_id}&jobType=sync&status=pending"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.getAccessToken()}"
            }
        running_jobs_response = requests.get(url=list_running_jobs_api, headers=headers, verify=False)
        pending_jobs_response = requests.get(url=list_pending_jobs_api, headers=headers, verify=False)

        try:
            running_jobs_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"取得 running 同步作業 id 失敗遇到 HTTP 錯誤， [Airbyte]: {e}")

        try:
            pending_jobs_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"取得 pending 同步作業 id 失敗遇到 HTTP 錯誤， [Airbyte]: {e}")


        running_result = running_jobs_response.json().get("data", []) or []
        pending_result = pending_jobs_response.json().get("data", []) or []
    
        if running_result:
            job = running_result[0]
        elif pending_result:
            job = pending_result[0]
        else:
            return None, None
        
        job_id = job.get("jobId")
        status = job.get("status")
        self.job_id = job_id
        return job_id, status
        
       

    def cancelJob(self, job_id):
        
        """取消指定的作業"""
        cancel_job_api = f"{self.airbyte_root_api}/jobs/{job_id}"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.getAccessToken()}"
        }
        
        self.logger_main.info(f"嘗試取消作業 {job_id}...")
        response = requests.delete(url=cancel_job_api, headers=headers, timeout=20, verify=False)
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"取消作業遇到 HTTP 錯誤，job_id: {job_id} [Airbyte]: {e}")

        result = response.json()
        self.logger_main.info(f"作業 {job_id} 取消請求已發送")
        return result

    def waitForCancellation(self, job_id):
        """等待作業取消完成"""
        start = time.time()
        last_status = None
        # token = self.getAccessToken()
        
        while True:
            elapsed = time.time() - start
            if elapsed > self.timeout_sec:
                self.logger_main.warning(f"等待取消作業超時: job_id {job_id}，已經等待 {elapsed} 秒")
                self.logger_main.warning(f"進續等待取消作業: job_id {job_id}")

            # 取得作業狀態
            get_job_api = f"{self.airbyte_root_api}/jobs/{job_id}"
            headers = {
                "accept": "application/json",
                "authorization": f"Bearer {self.getAccessToken()}"
            }
            
            response = requests.get(url=get_job_api, headers=headers, timeout=20, verify=False)
            
            # 如果 token 過期，嘗試刷新
            # if response.status_code == 401:
            #     self.logger_main.warning(f"Token 過期，刷新 token...")
            #     token = self.getAccessToken()
            #     headers["authorization"] = f"Bearer {token}"
            #     response = requests.get(url=get_job_api, headers=headers, timeout=20, verify=False)
            
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise requests.exceptions.HTTPError(f"檢查作業狀態失敗遇到 HTTP 錯誤，job_id: {job_id} [Airbyte]: {e}")

            job_status = response.json().get("status")

            if job_status is None:
                return None
            
            if job_status != last_status:
                self.logger_main.info(f"作業 {job_id} 狀態更新: {job_status}")
                last_status = job_status
            
            # 如果作業已經取消或結束，返回狀態
            if job_status in ["cancelled", "failed", "succeeded", "incomplete"]:
                return job_status
            
            # 如果還在執行中，繼續等待
            if job_status in ["pending", "running"]:
                time.sleep(self.poll_sec)

    