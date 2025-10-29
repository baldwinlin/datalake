#coding:utf-8
'''
DbtExecutionImpl.py
Object          :
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           :
Parameters      :
Output          :
********************************************************************************
Modify          :
'''
from service.AirbyteExecution import *
from crypto.Aes256Crypto import *
from dao.impl.S3DaoImpl import S3DaoImpl
from logger import Logger


import logging
import time
import requests
import json
import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AirbyteExecutionImpl(AirbyteExecution):
    def __init__(self, main_config, fc_config, args, pc_config=None):
        # config 是從 airbyte.conf 來的配置
        self.fc_config = fc_config
        if pc_config:
            self.pc_config = pc_config
        else:
            self.pc_config = None
        #airbyte config
        try:
            self.workspace_ids = fc_config['AIRBYTE']['WORKSPACE_IDS']
            self.airbyte_root_api = fc_config['AIRBYTE']['AIRBYTE_ROOT_API']
            #測試環境 正式執行時需要註解掉
            # self.user = config['AIRBYTE']['CLIENT_ID']
            # self.db_sec = config['AIRBYTE']['CLIENT_SECRET']
            #正式環境 正式執行需解開註解
            self.sec_file = self.fc_config.get('AIRBYTE','SEC_FILE')
            self.key_file = self.fc_config.get('AIRBYTE','KEY_FILE')
            self.user, self.sec_str = readSecFile(self.sec_file)
            self.salt = readSaltFile(self.key_file)
            self.db_sec = get_gpg_decrypt(self.sec_str, self.salt)
        except Exception as e:
            raise Exception(f"讀取Airbyte config錯誤: {e}")

        #s3 config
        if self.pc_config:
            try:
                self.s3_host = self.fc_config.get('S3','HOST')
                self.s3_port = self.fc_config.get('S3','PORT')
                self.s3_assess_id_file = self.fc_config.get('S3','ASSESS_ID_FILE')
                self.s3_assess_key_file = self.fc_config.get('S3','ASSESS_KEY_FILE')
                self.s3_user, self.s3_sec_str = readSecFile(self.s3_assess_id_file)
                self.s3_salt = readSaltFile(self.s3_assess_key_file)
                self.s3_sec = get_gpg_decrypt(self.s3_sec_str, self.s3_salt)
                self.bucket = self.pc_config.get('S3','BUCKET')
                self.s3_path = self.pc_config.get('S3','S3_PATH')
                if not self.s3_path.endswith('/'):
                    self.s3_path = self.s3_path + '/'
            except Exception as e:
                raise Exception(f"讀取S3 config錯誤: {e}")

            #s3 dao connect
            try:
                self.s3Dao=S3DaoImpl(self.bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
            except Exception as e:
                raise Exception(f"建立S3 DAO時發生錯誤: {e}")

        self.args_str = args
        if self.args_str:
            self.args = json.loads(self.args_str)
        else:
            raise Exception("args 不得為空值")
        
        self.connection_name = str(self.args["connection_name"])
        self.poll_sec = int(self.args.get("poll_sec", 180))
        self.timeout_sec = int(self.args.get("timeout_sec", 3600))

        self.connection_id = None
        self.source_id = None
        self.destination_id = None
        self.connection_info = None
        self.source_name = None
        self.destination_name = None
        self.job_id = None

        # 初始化 logger 變數
        self.main_config = main_config
        self.logger_main = None

    
    def _initialize_logger(self):
        # 讀取日誌配置，優先使用 main_config，否則從 main.conf 讀取
        log_config = self.main_config
        airbyte_log_path = f"{log_config['LOG']['LOG_PATH']}/airbyte"
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        logger_file_name = f"{self.connection_name}_{timestamp}"

        # 創建兩個 Logger 實例
        Logger.Logger(airbyte_log_path, logger_file_name) # 主要日誌

        # 更新 AirbyteExecution 的 logger_main 和 errorHandler attribute
        self.logger_main = logging.getLogger(logger_file_name) # 取得主要 logger 實例

    def run(self):

        # 初始化 logger
        self._initialize_logger()

        """
        # GetConnectionID：透過 connection name 取得 connection ID
        step: 嘗試抓取 connectionId，如果出現 http 錯誤或意外錯誤，會先中斷流程
        """
        try:
            response = self.getConnectionId()
        except requests.exceptions.HTTPError as e:
            self.logger_main.error(e)
            return False
        except Exception as e:
            self.logger_main.error(f"取得 connectionId 失敗，非預期的錯誤: {e}")
            return False
        
        """
        step: 驗收取得 connectionId 的結果，遇到預期內的錯誤，找不到對應的 connection Id ，或遇到成功取得 connection Id
        """
        if response is None:
            self.logger_main.error(f"找不到 {self.connection_name} 對應的 connection Id")
            return False
        if response:
            self.logger_main.info(f"找到 {self.connection_name} 對應的 connection Id: {response}")

            """
            # 組裝詳細的連線資訊：透過 GetConnectionID 的過程同時抓出 sourceId 和 destinationId，後續調用內部函式取得 sourceName 和 destinationName，組裝詳細的連線資訊
            step: 嘗試組裝連線資訊，遇到 HTTP 錯誤或意外錯誤，會先中斷流程
            """
            try:
                enriched_info = self.checkConnectionInfo()
            except requests.exceptions.HTTPError as e:
                self.logger_main.error(e)
                return False
            except Exception as e:
                self.logger_main.error(f"獲取詳細連線資訊失敗，非預期的錯誤: {e}")
                return False

            """
            step: 驗收組裝連線資訊結果，發生預期內錯誤，找不到連線資訊，或是遇到成功組裝連線資訊
            """
            if enriched_info is None:
                self.logger_main.error(f"無法獲取 {self.connection_name} 的詳細連線資訊")
                return False
            else:
                self.logger_main.info(f"取得詳細連線資訊: {enriched_info}")

            """
            # 觸發資料庫同步作業：透過 connection Id 觸發執行
            step: 嘗試觸發資料庫同步作業，遇到 HTTP 錯誤或意外錯誤，會先中斷流程
            """
            try:
                job_id = self.triggerSync()
            except requests.exceptions.HTTPError as e:
                self.logger_main.error(e)
                return False
            except Exception as e:
                self.logger_main.error(f"觸發同步失敗，非預期的錯誤: {e}")
                return False
            
            """
            step: 驗收觸發資料庫同步結果，遇到預期內錯誤，無法觸發同步作業，或遇到成功觸發
            """
            if job_id is None:
                self.logger_main.error(f"觸發同步失敗: {self.connection_name}, 無法取得 job_id")
                return False
            self.logger_main.info(f"觸發同步成功: {self.connection_name}, job_id: {job_id}")


            """
            # 監聽資料庫同步狀態：透過 job Id 觸發監聽
            step: 嘗試監聽資料庫同步狀態，遇到 HTTP 錯誤或意外錯誤，會先中斷流程
            """
            try:
                sync_status = self.waitSync()
            except requests.exceptions.HTTPError as e:
                self.logger_main.error(e)
                return False
            except Exception as e:
                self.logger_main.error(f"監聽同步狀態失敗，非預期的錯誤: {e}")
                return False
            """
            step: 驗收監聽資料庫同步結果，遇到預期內錯誤或執行成功
            """
            if sync_status:
                try:
                    self.validate_sync_result()
                except requests.exceptions.HTTPError as e:
                    self.logger_main.error(e)
                    return False
                except Exception as e:
                    self.logger_main.error(f"驗證同步結果失敗，非預期的錯誤: {e}")
                    return False

            if sync_status is None:
                self.logger_main.error(f"監聽同步狀態失敗: {self.connection_name}, 無法取得同步狀態")
                return False
            elif sync_status == "failed":
                self.logger_main.error(f"同步失敗: {self.connection_name}, job_id: {job_id}")
                return False
            elif sync_status == "cancelled":
                self.logger_main.error(f"同步被取消: {self.connection_name}, job_id: {job_id}")
                return False
            elif sync_status == "incomplete":
                self.logger_main.error(f"同步不完整: {self.connection_name}, job_id: {job_id}")
                return False
            elif sync_status == "succeeded":
                self.logger_main.info(f"同步成功: {self.connection_name}, job_id: {job_id}")
                return True


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

        self.logger_main.info(f"每次呼叫都拿一次 access token")
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
                    self.connection_info = connection
                    self.source_id = connection.get("sourceId")
                    self.destination_id = connection.get("destinationId")
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

        # return self.connection_id

    def _get_source_name(self) -> str:
        # 根據 sourceId 取得 source 名稱
        get_source_api = f"{self.airbyte_root_api}/sources/{self.source_id}"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.getAccessToken()}"
        }
        
        self.logger_main.info(f"嘗試抓取 sourceId {self.source_id} 對應的 source_name....")
        response = requests.get(url=get_source_api, headers=headers, timeout=30, verify=False)
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"取得 source name 遇到 HTTP 錯誤，請檢查登入 airbyte 所需參數。[Airbyte]: {e}")

        source_name = response.json().get("name")
        if source_name:
            self.source_name = source_name
            return source_name
        if source_name is None:
            return None 

    def _get_destination_name(self) -> str:
        # 根據 destinationId 取得 destination 名稱
        get_destination_api = f"{self.airbyte_root_api}/destinations/{self.destination_id}"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.getAccessToken()}"
        }
        self.logger_main.info(f"嘗試抓取 destinationId {self.destination_id} 對應的 destination_name....")
        response = requests.get(url=get_destination_api, headers=headers, timeout=30, verify=False)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"取得 destination name 遇到 HTTP 錯誤，請檢查登入 airbyte 所需參數。[Airbyte]: {e}")

        destination_name = response.json().get("name")
        if destination_name:
            self.destination_name = destination_name
            return destination_name
        if destination_name is None:
            return None

    def checkConnectionInfo(self):
        enriched_data = self.connection_info.copy()

        source_name = self._get_source_name()
        if source_name is None:
            self.logger_main.error(f"無法獲取 {self.connection_name} 的 source 名稱")   
            return None
        self.logger_main.info(f"抓取到 source_name: {source_name} (source_id: {self.source_id})")
        enriched_data["source_name"] = source_name

        destination_name = self._get_destination_name()
        if destination_name is None:
            self.logger_main.error(f"無法獲取 {self.connection_name} 的 destination 名稱")
            return None
        self.logger_main.info(f"抓取到 destination_name: {destination_name} (destination_id: {self.destination_id})")   
        enriched_data["destination_name"] = destination_name

        if self.source_id:
            del enriched_data["sourceId"]
        if self.destination_id:
            del enriched_data["destinationId"]
        
        self.connection_info = enriched_data
        return enriched_data

    def triggerSync(self):
        # 建立同步 Job
        trigger_sync_api = f"{self.airbyte_root_api}/jobs"
        payload = {"connectionId": self.connection_id, "jobType": "sync"}
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Bearer {self.getAccessToken()}"
        }
        self.logger_main.info(f"開始觸發同步: {self.connection_name}")
        response = requests.post(url=trigger_sync_api,headers=headers,json=payload, timeout=30, verify=False)
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"觸發同步失敗遇到 HTTP 錯誤，請檢查登入 airbyte 所需參數 和 connection Id，或是錯誤訊息 [Airbyte]: {e}")

        job_id = response.json().get("jobId")
        if job_id:
            self.job_id = job_id
            return job_id
        if job_id is None:
            return None

    def waitSync(self):
        # 等待同步完成
        start = time.time()
        last_status = None

        # 在開始時取得一次 token
        # token = self.getAccessToken()
        
        while True:
            elapsed = time.time() - start
            self.logger_main.info(f"等待同步完成，已經等待了 {elapsed} 秒...")
            if elapsed > self.timeout_sec:
                self.logger_main.warning(f"等待同步超時: {self.connection_name}, job_id: {self.job_id}，已經等待 {elapsed} 秒，超過設定的 timeout_sec {self.timeout_sec} 秒")
                self.logger_main.warning(f"繼續監聽同步狀態，直到同步完成")

            # 取得最新的 job 狀態
            list_jobs_api = f"{self.airbyte_root_api}/jobs/{self.job_id}"
            headers = {
                "accept": "application/json",
                "authorization": f"Bearer {self.getAccessToken()}"
            }
            self.logger_main.info(f"正在檢查 job 狀態: {self.job_id}")
            response = requests.get(url=list_jobs_api, headers=headers, timeout=20, verify=False)
           
            # 如果 token 過期，嘗試刷新
            # if response.status_code == 401:
            #     self.logger_main.warning(f"Token 過期，刷新 token...")
            #     new_token = self.getAccessToken()
            #     headers["authorization"] = f"Bearer {new_token}"
            #     response = requests.get(url=list_jobs_api, headers=headers, timeout=20, verify=False)
            
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise requests.exceptions.HTTPError(f"監聽失敗遇到 HTTP 錯誤，請檢查 airbyte 連線參數 和 job_id 有效性， [Airbyte]: {e}")


            job_status = response.json().get("status")
            if job_status is None:
                return None
            
            if job_status != last_status:
                self.logger_main.info(f"同步狀態更新: {job_status} (job_id: {self.job_id})")
                last_status = job_status
            
            # 處理最終狀態（直接返回）
            if job_status in ["succeeded", "failed", "cancelled", "incomplete"]:
                return job_status

            # 處理進行中的狀態
            if job_status in ["pending", "running"]:
                self.logger_main.info(f"同步狀態: {job_status}...")
                time.sleep(self.poll_sec)

    def validate_sync_result(self):
        result_api = f"{self.airbyte_root_api}/jobs/{self.job_id}"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.getAccessToken()}"
        }

        response = requests.get(url=result_api, headers=headers, timeout=20, verify=False)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(f"獲取同步結果失敗遇到 HTTP 錯誤，請檢查 airbyte 連線參數 和 job_id 有效性， [Airbyte]: {e}")
        
        result_data = response.json()
        self.logger_main.info(f"完整 Job Response: {json.dumps(result_data, indent=2)}")

    def validate_sync_result_s3(self):
        self.logger_main.info(f"讀取 S3 檔案列表驗證同步結果")
        try:
            s3_prefix = str(self.s3_path)
            search_key = str(self.s3_path + "*.jsonl")
            self.logger_main.info(f"欲查詢的檔案路徑: {s3_prefix}, 檔案名稱模式: {search_key}")
            file_objs_list = self.s3Dao.listFilesWithoutFolder(search_key, s3_prefix)
            if len(file_objs_list) == 0:
                self.logger_main.info(f"S3 檔案列表為空")
                return "S3_EMPTY"
            else:
                self.logger_main.info(f"S3 檔案列表: {file_objs_list}")
                return "S3_NOT_EMPTY"
        except Exception as e:
            raise Exception(f"取得S3檔案列表失敗: {e}")
