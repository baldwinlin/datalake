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

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AirbyteCancelForcedImpl(AirbyteCancel):
    def __init__(self, main_config, config, args):
        # config 是從 airbyte.conf 來的配置
        self.config = config
        self.workspace_ids = config['AIRBYTE']['WORKSPACE_IDS']
        self.airbyte_root_api = config['AIRBYTE']['AIRBYTE_ROOT_API']
        
        #測試環境 正式執行時需要註解掉
        # self.user = config['AIRBYTE']['CLIENT_ID']
        # self.db_sec = config['AIRBYTE']['CLIENT_SECRET']

        self.sec_file = self.config.get('AIRBYTE','SEC_FILE')
        self.key_file = self.config.get('AIRBYTE','KEY_FILE')
        #正式環境 正式執行需解開註解
        self.user, self.sec_str = readSecFile(self.sec_file)
        self.salt = readSaltFile(self.key_file)
        self.db_sec = get_gpg_decrypt(self.sec_str, self.salt)

        args = json.loads(args)
        self.job_id = int(args.get("job_id"))
        self.poll_sec = int(args.get("poll_sec")) 
        self.timeout_sec = int(args.get("timeout_sec")) 

        # 初始化 logger 變數
        self.main_config = main_config
        self.logger_main = None
    
    def _initialize_logger(self):
        # 讀取日誌配置，優先使用 main_config，否則從 main.conf 讀取
        log_config = self.main_config
        airbyte_log_path = f"{log_config['LOG']['LOG_PATH']}/airbyte"
        
        # 創建 Logger 實例 - 使用相同的 airbyte_main logger
        Logger.Logger(airbyte_log_path, "airbyte_main") # 主要日誌

        # 更新 AirbyteExecution 的 logger_main attribute
        self.logger_main = logging.getLogger('airbyte_main') # 取得主要 logger 實例

    def run(self):
        """執行強制取消流程"""
        # 初始化 logger
        self._initialize_logger()

        # 驗證必要參數
        if not self.job_id:
            self.logger_main.error("必須提供 job_id 參數")
            return False
        self.logger_main.info(f"啟動 Airbyte 強制取消作業 - job_id: {self.job_id}")

        try:
            # 直接取消指定的作業
            cancel_result = self.cancelJob(self.job_id)
            
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
        token = self.getAccessToken()
        
        while True:
            elapsed = time.time() - start
            if elapsed > self.timeout_sec:
                self.logger_main.warning(f"等待取消作業超時: job_id {job_id}，已經等待 {elapsed} 秒")
                self.logger_main.warning(f"進續等待取消作業: job_id {job_id}")

            # 取得作業狀態
            get_job_api = f"{self.airbyte_root_api}/jobs/{job_id}"
            headers = {
                "accept": "application/json",
                "authorization": f"Bearer {token}"
            }
            
            response = requests.get(url=get_job_api, headers=headers, timeout=20, verify=False)
            
            # 如果 token 過期，嘗試刷新
            if response.status_code == 401:
                self.logger_main.warning(f"Token 過期，刷新 token...")
                token = self.getAccessToken()
                headers["authorization"] = f"Bearer {token}"
                response = requests.get(url=get_job_api, headers=headers, timeout=20, verify=False)
            
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

    