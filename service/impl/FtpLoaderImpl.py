#coding:utf-8
'''
FtpLoaderImpl.py
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
import logging
from logger import Logger
import datetime

from service.AirbyteExecution import *
from crypto.Aes256Crypto import *

from service.FtpLoader import *
from dao.impl.FtpDaoImpl import FtpDaoImpl  #FTP DAO 實現
from util.Reformatter import *
from util.Compressor import *
from util.Validator import *
from util.CleanTempFIle import *
from dao.impl.S3DaoImpl import S3DaoImpl

import os
import shutil
import time
import json
import fnmatch

class FtpLoaderImpl(FtpLoader):
    def __init__(self,main_config, fc_config, pc_config, args):
        self.fc_config = fc_config
        self.pc_config = pc_config
        self.main_config = main_config
        self.args_str = args
        if self.args_str:
            self.args = json.loads(self.args_str)
            self.batch_date =str(self.args.get('batch_date'))
        else:
            raise Exception("args 不得為空值")

        self.logger_main = None
        self.logger_prefix = self.pc_config["LOG"].get("LOG_PREFIX")
        if not self.logger_prefix:
            raise Exception("LOG_PREFIX 不得為空值")
        self.log_level = self.main_config["LOG"].get("LOG_LEVEL", "INFO").upper()
        self.expected_row_length = 0
        self.total_rows_count = 0
        self.controller_file_rows_count = 0
        self.file_counts = 0

        """建立暫存工作目錄"""
        try:
            #正式環境 正式執行需解開註解
            # temp_path = self.main_config.get('LOG','TEMP_PATH')
            #測試需解開註解
            temp_path = self.main_config.get('LOG','TEMP_BASE_PATH')
            self.temp_operation_folder_name= self.pc_config['SOURCE']["WORK_SUB_DIR"]
            self.temp_operation_folder_path = os.path.join(temp_path, self.temp_operation_folder_name)
        except Exception as e:
            raise Exception(f"讀取TEMP path錯誤: {e}")
        if os.path.exists(self.temp_operation_folder_path):
            try:
                CleanTempFile.remove_temp_operation_directory(self.temp_operation_folder_path)
            except Exception as e:
                raise Exception(f"刪除暫存目錄時發生錯誤: {e}")
        try:
            if not os.path.exists(self.temp_operation_folder_path):
                os.makedirs(self.temp_operation_folder_path)  # 建立此次執行時的資料夾
            self.temp_download_path = os.path.join(self.temp_operation_folder_path, "downloads")
            self.temp_processing_path = os.path.join(self.temp_operation_folder_path, "processing")
            self.temp_upload_path = os.path.join(self.temp_operation_folder_path, "uploads")
        except Exception as e:
            raise Exception(f"建立暫存目錄時發生錯誤: {e}")

        if not os.path.exists(self.temp_download_path):
            os.makedirs(self.temp_download_path)
        if not os.path.exists(self.temp_processing_path):
            os.makedirs(self.temp_processing_path)
        if not os.path.exists(self.temp_upload_path):
            os.makedirs(self.temp_upload_path)

        """設定Ftp連線參數"""
        try:
            self.host = self.fc_config.get('FTP','IP')
            self.port = int(self.fc_config.get('FTP','PORT'))
            self.ftp_type = self.fc_config.get('FTP','TYPE')
            #測試時需要解開註解
            # self.user = self.fc_config.get('FTP','USER')
            # self.sec = self.fc_config.get('FTP','SEC')
            #正式環境 正式執行需解開註解
            self.ftp_sec_file = self.fc_config.get('FTP','SEC_FILE')
            self.ftp_key_file = self.fc_config.get('FTP','KEY_FILE')
            self.user, self.sec_str = readSecFile(self.ftp_sec_file)
            self.ftp_salt = readSaltFile(self.ftp_key_file)
            self.sec = get_gpg_decrypt(self.sec_str, self.ftp_salt)
        except Exception as e:
            raise Exception(f"讀取FTP config錯誤: {e}")

        try:
            self.s3_host = self.fc_config.get('S3','HOST')
            self.s3_port = self.fc_config.get('S3','PORT')
            self.s3_assess_id_file = self.fc_config.get('S3','ASSESS_ID_FILE')
            self.s3_assess_key_file = self.fc_config.get('S3','ASSESS_KEY_FILE')
            self.s3_user, self.s3_sec_str = readSecFile(self.s3_assess_id_file)
            self.s3_salt = readSaltFile(self.s3_assess_key_file)
            self.s3_sec = get_gpg_decrypt(self.s3_sec_str, self.s3_salt)
        except Exception as e:
            raise Exception(f"讀取S3 config錯誤: {e}")

        """來源檔案資料處理"""
        try:
            self.source_path = self.pc_config.get('SOURCE','PATH')
            self.name_pattern = self.pc_config.get('SOURCE','NAME_PATTERN')
            self.source_encoding = self.pc_config.get('SOURCE',"ENCODING").lower()
            if self.source_encoding == "":
                raise Exception("SOURCE ENCODING 不得為空值")
            elif self.source_encoding not in ["big5", "utf-8"]:
                raise Exception(f"編碼 {self.source_encoding} 不支援")
            self.header = self.pc_config.get('SOURCE','HEADER')
            if self.header not in ["Y", "N"]:
                raise Exception("HEADER 設定只接受 Y,N 設定值")
            
            #檔案分隔處理
            self.delimiter = self.pc_config.get('SOURCE','DELIMITER')
            self.col_size_file = self.pc_config.get('SOURCE','COL_SIZE_FILE')
            if self.col_size_file:
                if os.path.exists(self.col_size_file):
                    pass
                else:
                    raise Exception(f"COL_SIZE_FILE 檔案不存在 {self.col_size_file}")
            
            #控制檔讀取處理
            self.controller_file = self.pc_config.get('SOURCE','CTL_FILE')
            if self.controller_file not in ["Y", "N"]:
                raise Exception("CTL_FILE 設定只接受 Y,N 設定值")
            if self.controller_file == "Y":
                self.controller_file_name_pattern = self.pc_config.get('SOURCE','CTL_FILE_NAME_PATTERN')
                self.controller_file_delimiter = self.pc_config.get('SOURCE','CTL_FILE_DELIMITER')
                if self.controller_file_delimiter == "":
                    self.controller_file_delimiter = None
                if self.batch_date == "" or self.batch_date == "None" or len(self.batch_date) != 8:
                    raise Exception("有控制檔，但尚未正確設定 Batch Date")
            else:
                self.controller_file_name_pattern = None
                self.controller_file_delimiter = None
        except Exception as e:
            raise Exception(f"讀取SOURCE config錯誤: {e}")
        
        """目標檔案資料位置"""
        try:
            self.s3_bucket = self.pc_config.get('TARGET','S3_BUCKET')
            self.target_path = self.pc_config.get('TARGET','PATH')
            if not self.target_path.endswith('/'):
                self.target_path = self.target_path + '/'
            self.target_encoding = self.pc_config.get('TARGET','ENCODING').lower()
            if self.target_encoding == "":
                raise Exception("TARGET ENCODING 不得為空值")
            elif self.target_encoding not in ["big5", "utf-8"]:
                raise Exception(f"編碼 {self.target_encoding} 不支援")
        except Exception as e:
            raise Exception(f"讀取TARGET config錯誤: {e}")

        """設定解壓縮處理"""
        try:
            self.zip_sec_file = self.pc_config.get('ZIP','SEC_FILE')
            self.zip_key_file = self.pc_config.get('ZIP','KEY_FILE')
            if self.zip_sec_file and self.zip_key_file:
                self.zip_user, self.zip_sec_str = readSecFile(self.zip_sec_file)
                self.zip_salt = readSaltFile(self.zip_key_file)
                self.zip_sec = get_gpg_decrypt(self.zip_sec_str, self.zip_salt)
            else:
                self.zip_user = None
                self.zip_sec = None
        except Exception as e:
            raise Exception(f"讀取ZIP config錯誤: {e}")
        
        try:
            self.ftp_dao = FtpDaoImpl(
                ftp_type=self.ftp_type, 
                host=self.host, 
                port=self.port, 
                user=self.user, 
                sec=self.sec,
                logger=None)  # 暫時設為 None，等 logger 初始化後再更新
        except Exception as e:
            raise Exception(f"建立FTP DAO時發生錯誤: {e}")
        
    def _initialize_logger(self):
        log_config = self.main_config
        ftp_log_path = f"{log_config['LOG']['LOG_PATH']}/ftp_get"
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        
        logger_file_name = f"{self.logger_prefix}_{timestamp}"
        Logger.Logger(ftp_log_path, logger_file_name)
        self.logger_main = logging.getLogger(logger_file_name)
        self.ftp_dao.logger = self.logger_main # 更新 ftp_dao 的 logger

    def errorExit(self, error_message):
        self.logger_main.error(error_message)
        exit(1)  

    def run(self):
        """ 建立 logger """
        self._initialize_logger()
        self.logger_main.info("連線 FTP/SFTP Server 完成。")
        self.logger_main.setLevel(getattr(logging, self.log_level, logging.INFO))

        
        """列出的檔案"""       
        self.logger_main.info(f"開始列出 FTP/SFTP Server 的檔案.....")
        files_list = self.getFtpFileList()
        if not files_list:
            self.errorExit("沒有找到符合條件的檔案")
        self.logger_main.info(f"完成找到指定的檔案列表: {files_list}")

        """下載檔案"""
        self.logger_main.info(f"開始下載檔案至暫存區.....")
        download_files_list = self.downloadFtpFile(files_list)
        if not download_files_list:
            self.errorExit("沒有成功下載指定的檔案")
        self.logger_main.info(f"完成下載檔案至暫存區: {download_files_list}")
        
        """壓縮檔解壓縮"""
        if self.name_pattern.lower().endswith((".zip", ".7z", ".tar", ".gz", ".tgz", ".tar.gz")):
            self.logger_main.info(f"開始解壓縮檔案.....")
            unzip_files_list  = self.unzipFile(download_files_list)
            download_files_list = unzip_files_list 
            self.logger_main.info(f"解壓縮檔案完成，檔案暫存至處理區，解壓縮後的檔案列表: {unzip_files_list}")
        else:
            for file_name in download_files_list:
                shutil.copy(os.path.join(self.temp_download_path, file_name), os.path.join(self.temp_processing_path, file_name))
            self.logger_main.info(f"無需解壓縮，檔案暫存至處理區，檔案列表: {download_files_list}")

        self.file_counts = len(download_files_list)
        if self.controller_file == "Y":
            self.file_counts -= 1
        self.logger_main.debug(f"本次執行檔案數量: {self.file_counts}")
        
        """驗證控制檔日期"""
        if self.controller_file == "Y":
            self.logger_main.debug(f"開始檢查控制檔日期.....")
            controller_file_name = self._checkBatchDate(download_files_list)
            if controller_file_name is None:
                self.errorExit(f"控制檔日期檢查失敗可能設定檔有誤或是沒有控制檔")
            self.logger_main.debug(f"控制檔日期檢查 {controller_file_name} 配置正確")
            self.logger_main.debug(f"完成檢查控制檔日期")

        """驗證下載檔案行數"""
        self.logger_main.debug(f"開始檢查下載檔案行數.....")
        if self.controller_file == "Y":
            self.total_rows_count, self.controller_file_rows_count = self._getFileRowsCount(download_files_list, have_controller_file="Y")
            self.logger_main.info(f"檢查控制檔行數完成，控制檔行數: {self.controller_file_rows_count}，下載的檔案總行數: {self.total_rows_count}")
        else:
            self.total_rows_count = self._getFileRowsCount(download_files_list, have_controller_file="N")
            self.logger_main.info(f"檢查下載檔案總行數完成，下載檔案總行數: {self.total_rows_count}")

        if self.controller_file == "Y":
            self._checkRowsCountMessage(self.total_rows_count, "檔案下載與控制檔驗證")

            if self.header == "Y":
                self.logger_main.debug(f"扣除標題欄位行數後檢查檔案行數完成，檔案行數正確")
            elif self.header == "N":
                self.logger_main.debug(f"無標題欄位行數。檢查檔案行數完成，檔案行數正確")
        
        """過濾掉控制檔"""
        if self.controller_file == "Y":
            self.logger_main.debug(f"開始過濾掉控制檔.....")
            self.logger_main.debug(f"過濾掉控制檔...")
        
        processing_files_list = self._fileFilter(download_files_list)
        self.logger_main.info(f"需轉換的檔案列表: {processing_files_list}")
        
        if self.controller_file == "Y":
            self.logger_main.debug(f"完成過濾掉控制檔")
                    

        """解碼驗證"""
        self.logger_main.debug(f"開始檢查檔案編碼.....")
        result = self._checkDecodeValid(processing_files_list)
        if result == True:
            self.logger_main.debug(f"檔案編碼檢查完成，檔案列表: {processing_files_list}")

        """移除欄位標題"""
        if self.header == "Y":
            self.logger_main.info(f"開始移除欄位標題.....")
            result = self.removeHeaderLine(processing_files_list)
            if result == True:
                self.logger_main.info(f"完成移除欄位標題行")
                self.logger_main.debug(f"開始檢查移除欄位標題後的檔案行數.....")
                proccessing_rows_count = self._getFileRowsCount(processing_files_list, have_controller_file="N")
                self._checkRowsCountMessage(proccessing_rows_count, "移除欄位標題")
                self.logger_main.debug(f"移除欄位標題後行數檢查完成")
       
        """檔案資料長度檢查（固定長度檔案）"""
        if self.col_size_file:
            self.logger_main.debug(f"開始檔案每筆資料長度檢查.....")
            result = self._checkRowsLength(processing_files_list)
            if result is True:
                self.logger_main.debug(f"驗證每筆資料長度完成")

        """檔案分隔"""
        self.logger_main.info(f"開始檔案分隔處理.....")
        if self.col_size_file:
            if self.delimiter:
                self.logger_main.info(f"指定使用分隔符號{self.delimiter}進行固定欄位寬度檔案分隔")
            else:
                self.delimiter = ","
                self.logger_main.info(f"無指定使用的分隔符號，預設{self.delimiter}進行固定欄位寬度檔案分隔")
        else:
            self.logger_main.info(f"無指定COL_SIZE_FILE，不需要插入分隔符號")
        
        if self.col_size_file and self.delimiter:
            result = self.insertDelimiter(processing_files_list)
            if result:
                self.logger_main.info(f"置入分隔符號完成")
                self.logger_main.debug(f"開始檢查置入分隔符號後的檔案行數.....")
                proccessing_rows_count = self._getFileRowsCount(processing_files_list, have_controller_file="N")
                self._checkRowsCountMessage(proccessing_rows_count, "置入分隔符號")
                self.logger_main.debug(f"置入分隔符號後行數檢查完成")

        """檔案轉碼"""
        reformated_files_list = []
        if self.source_encoding != self.target_encoding:
            self.logger_main.info(f"開始檔案轉碼處理.....")
            self.logger_main.info(f"轉換編碼從 {self.source_encoding} 到 {self.target_encoding}")
            result = self.reformatEncoding(processing_files_list)
            reformated_files_list = result
            if result:
                self.logger_main.debug(f"開始檢查轉換編碼後的檔案行數.....")
                proccessing_rows_count = self._getFileRowsCount(processing_files_list, have_controller_file="N")
                self._checkRowsCountMessage(proccessing_rows_count, "轉換編碼")
        elif self.source_encoding == self.target_encoding:
            self.logger_main.info(f"編碼為 {self.target_encoding}，不需要轉換")
            reformated_files_list = processing_files_list
            
        """複製檔案到上傳目錄"""
        for file in reformated_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file)
            temp_upload_file_path = os.path.join(self.temp_upload_path, file)
            shutil.copy(temp_processing_file_path, temp_upload_file_path)

        """上傳檔案到S3"""
        error_upload_files_list = []
        self.logger_main.info(f"開始上傳檔案到S3 ，上傳 bucket：{self.s3_bucket}，上傳路徑：{self.target_path}")
        for file_name in reformated_files_list:
            try:
                self.writeToS3(file_name)
            except Exception as e:
                self.logger_main.error(f"上傳檔案失敗 {e}")
                error_upload_files_list.append(file_name)
        
        if len(error_upload_files_list) > 0:
            self.errorExit(f"上傳檔案失敗錯誤表 {error_upload_files_list}")

        self.logger_main.info(f"開始清空暫存目錄.....")
        CleanTempFile.remove_temp_operation_directory(self.temp_operation_folder_path)
        self.logger_main.info("清空暫存目錄完成。") 
        
        """FTP/SFTP 連線關閉"""
        try:
            self.close()
        except Exception as e:
            self.errorExit(f"關閉 FTP/SFTP 連線失敗 {e}")
        self.logger_main.info("FTP/SFTP 連線已關閉。")
        return True

    def getFtpFileList(self):
        try:
            files = self.ftp_dao.listFiles(self.source_path, self.name_pattern, self.batch_date)
        except Exception as e:
            self.errorExit(f"列出 FTP/SFTP Server 的檔案失敗 {e}")
        return files

    def downloadFtpFile(self, files_list):
        download_files_list = []
        error_download_files_list = []
        
        for file_name in files_list:
            try:
                self.ftp_dao.downloadFile(file_name, self.source_path, self.temp_download_path)
                download_files_list.append(file_name)  # 只有成功下載的檔案才加入
            except Exception as e:
                self.logger_main.error(f"下載檔案 {file_name} 失敗 {e}")
                error_download_files_list.append(file_name)
        if len(error_download_files_list) > 0:
            self.errorExit(f"下載檔案失敗 {error_download_files_list}")
        return download_files_list

    def unzipFile(self, download_files_list):
        unzip_files_list = [] 
        error_unzip_files_list = []
        for file_name in download_files_list:
            download_file_path = os.path.join(self.temp_download_path, file_name)
            try:
                unzip_files = Compressor.decompress(download_file_path, self.temp_processing_path, self.zip_sec)
                for unzip_file in unzip_files:
                    unzip_files_list.append(unzip_file)
            except Exception as e:
                self.logger_main.error(f"解壓縮檔案 {file_name} 失敗 {e}")
                error_unzip_files_list.append(file_name)
        if len(error_unzip_files_list) > 0:
            self.errorExit(f"解壓縮檔案失敗 {error_unzip_files_list}")
        return unzip_files_list 

    def removeHeaderLine(self, processing_files_list):
        error_files_list = []
        for file_name in processing_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file_name)
            try:
                Reformatter.remove_header(temp_processing_file_path, temp_processing_file_path, self.source_encoding)
            except Exception as e:
                self.logger_main.error(f"移除欄位標題行失敗 {file_name} {e}")
                error_files_list.append(file_name)
       
        if len(error_files_list) > 0:
            self.errorExit(f"移除欄位標題行失敗 {error_files_list}")

        return True

    def insertDelimiter(self, processing_files_list):
        error_files_list = []
        for file in processing_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file)
            try:
                Reformatter.insert_delimiter_with_sizes_file(temp_processing_file_path, self.col_size_file, temp_processing_file_path, self.delimiter)
            except Exception as e:
                self.logger_main.error(f"插入分隔符號 {file} 失敗: {e}")
                error_files_list.append(file)
        
        if len(error_files_list) > 0:
            self.errorExit(f"插入分隔符號失敗的檔案列表 {error_files_list}")
        else:
            return True
                
    def reformatEncoding(self, processing_files_list):
        error_files_list = []
        reformated_files_list = []
        for file in processing_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file)
            
            """轉換編碼為指定編碼"""
            try:
                Reformatter.encoding_to_target_encoding(temp_processing_file_path, self.source_encoding, temp_processing_file_path, self.target_encoding)
                reformated_files_list.append(file)
                self.logger_main.info(f"檔案 {file} 完成轉換編碼")
            except Exception as e:
                self.logger_main.error(f"轉換編碼 {file} 失敗: {e}")
                error_files_list.append(file)
        
        if len(error_files_list) > 0:
            self.errorExit(f"轉換編碼失敗的檔案列表 {error_files_list}")
        else:
            self.logger_main.info("轉換編碼完成")
            return reformated_files_list
    
    def writeToS3(self, file):
        file_path = os.path.join(self.temp_upload_path, file)
        s3_file_path = os.path.join(self.target_path, file)
        try:
            s3Dao = S3DaoImpl(self.s3_bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
            s3Dao.uploadFile(file_path, s3_file_path)
            s3_prefix = self.target_path
            search_key = s3_file_path
            uploaded_file = s3Dao.listFilesWithoutFolder(search_key, s3_prefix)
        except Exception as e:
            raise Exception(f"上傳檔案 {file} 失敗: {e}")
            
        self.logger_main.info(f"檔案上傳結果: {uploaded_file}")

    def close(self):
        self.ftp_dao.close()

    def _getFileRowsCount(self, file_list, have_controller_file):
        file_rows_counts_info = []
        total_rows_count = 0
        error_file_list = []

        for file_name in file_list:
            try:
                result = Validator.get_file_line_count(file_name, self.temp_processing_path, self.controller_file_delimiter, self.controller_file_name_pattern)
                file_rows_counts_info.append(result)
                if result[0] == "檔案":
                    total_rows_count += result[2]
                    self.logger_main.info(f"檔案行數檢查，{result[0]}: {result[1]}，{result[3]}")
                elif have_controller_file =="Y" and result[0] == "檢核檔":
                    controller_file_rows_count = result[2]
            except Exception as e:
                self.logger_main.error(f"計算檔案行數失敗 {file_name} {e}")
                error_file_list.append(file_name)

        if len(error_file_list) > 0:
            self.errorExit(f"計算檔案行數失敗 {error_file_list}")
        
        if have_controller_file == "Y":
            return total_rows_count, controller_file_rows_count
        else:
            return total_rows_count
    
    def _checkRowsCountMessage(self, rows_count, process_state):
        if process_state == "檔案下載與控制檔驗證":
            total_rows_count = rows_count
            if self.header == "Y":
                total_rows_count -= self.file_counts
                if total_rows_count != self.controller_file_rows_count:
                    self.errorExit(f"檔案行數不符，預期筆數：{self.controller_file_rows_count}，實際筆數(扣標題欄位數量)：{total_rows_count}")
            elif self.header == "N":
                if total_rows_count != self.controller_file_rows_count:
                    self.errorExit(f"檔案行數不符，預期筆數：{self.controller_file_rows_count}，實際筆數：{self.total_rows_count}")
            return True
        elif process_state in ["移除欄位標題", "置入分隔符號", "轉換編碼"]:
            if self.controller_file == "Y":
                if rows_count == self.controller_file_rows_count:
                    self.total_rows_count = rows_count
                    self.logger_main.info(f"和控制檔預期行數比對正確，處理後的檔案行數總和: {self.total_rows_count}")
                else:
                    self.errorExit(f"和控制檔預期行數比對不符，預期筆數：{self.controller_file_rows_count}，實際筆數：{rows_count}，出錯流程：{process_state}")
            elif self.controller_file == "N":
                expected_rows_count = self.total_rows_count
                if process_state == "移除欄位標題":
                    expected_rows_count -= self.file_counts
                if rows_count == expected_rows_count:
                    self.total_rows_count = rows_count
                    self.logger_main.info(f"和總行數比對正確，處理後的檔案行數總和: {self.total_rows_count}")
                else:
                    self.errorExit(f"和總行數比對不符，預期筆數：{expected_rows_count}，實際筆數：{rows_count}，出錯流程：{process_state}")

    def _checkBatchDate(self, download_files_list):
        for file_name in download_files_list:
            try:
                is_controller_file = FilenameProcessor.is_controller_file(file_name, self.controller_file_name_pattern)
            except Exception as e:
                self.errorExit(f"檢查檔案是否為控制檔失敗 {e}")

            if is_controller_file:
                controller_file_path = os.path.join(self.temp_processing_path, file_name)
                try:
                    result = Validator.check_header_batch_date(controller_file_path, self.controller_file_delimiter, self.batch_date)
                    return file_name
                except Exception as e:
                    self.errorExit(f"檢查控制檔日期失敗 {e}")

    def _fileFilter(self, download_files_list):
        processing_files_list = []
        if self.controller_file == "Y":
            for file_name in download_files_list:
                try:
                    is_controller_file = FilenameProcessor.is_controller_file(file_name, self.controller_file_name_pattern)
                except Exception as e:
                    self.errorExit(f"過濾控制檔名稱失敗 {file_name} {e}")
                if is_controller_file:
                    continue
                processing_files_list.append(file_name)
        else:
            processing_files_list = download_files_list.copy()
        return processing_files_list

    def _checkDecodeValid(self, processing_files_list):
        problematic_files_list = []
        error_files_list = []
        for file_name in processing_files_list:
            try:
                file_path = os.path.join(self.temp_processing_path, file_name)
                problematic_lines = Validator.checking_decoding(file_path, self.source_encoding)
                if len(problematic_lines) > 0:
                    self.logger_main.error(f"檔案 {file_name} 有 {len(problematic_lines)} 行資料有解碼問題，標題欄位是第0行，請根據以下行數檢查資料{problematic_lines} ")
                    problematic_files_list.append(file_name)
            except Exception as e:
                self.logger_main.error(f"檢查檔案編碼失敗 {file_name} {e}")
                error_files_list.append(file_name)
       
        if len(error_files_list) > 0:
            self.errorExit(f"檢查檔案編碼失敗 {error_files_list}") 
        
        if len(problematic_files_list) > 0 :
            self.errorExit(f"檢查檔案編碼完畢，以下是有錯誤編碼的檔案 {problematic_files_list}")
        
        return True

    def _checkRowsLength(self, processing_files_list):

        problematic_row_length_file_list = []
        error_files_list = []
        for file_name in processing_files_list:
            try: 
                temp_processing_file_path = os.path.join(self.temp_processing_path, file_name)
                error_lines = Validator.checking_row_length(temp_processing_file_path, self.col_size_file)
                if error_lines is not True:
                    problem_file = (file_name, error_lines)
                    problematic_row_length_file_list.append(problem_file)
            except Exception as e:
                self.logger_main.error(f"檢查檔案每筆長度出現錯誤 {file_name} {e}")
                error_files_list.append(file_name)
        
        if len(error_files_list) > 0:
            self.errorExit(f"檢查檔案每筆長度失敗 {error_files_list}")
        
        if len(problematic_row_length_file_list) > 0:
            self.errorExit(f"出現資料長度不符合規範的檔案，請確認出錯位置 {problematic_row_length_file_list}")
        elif len(problematic_row_length_file_list) == 0:
            return True