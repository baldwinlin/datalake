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


class FtpLoaderImpl(FtpLoader):
    def __init__(self,main_config, config, args):
        self.config = config
        self.args = args

        """設定 FTP 和 SFTP 共有 attribution"""
        self.host = config['FTP']['FTP_IP']
        self.port = int(config['FTP']['FTP_PORT'])
        self.user = config['FTP']['FTP_USER']
        #測試時需要解開註解
        self.sec = config['FTP']['FTP_SEC']
        self.ftp_type = config['FTP']['FTP_TYPE']
        #正式環境 正式執行需解開註解
        # self.ftp_sec_file = config['FTP']['FTP_SEC_FILE']
        # self.ftp_key_file = config['FTP']['FTP_SEC_KEY']
        # self.user, self.sec_str = readSecFile(self.ftp_sec_file)
        # self.ftp_salt = readSaltFile(self.ftp_key_file)
        # self.sec = get_gpg_decrypt(self.sec_str, self.ftp_salt)

        """S3 相關參數"""
        self.s3_host = config['S3']['HOST']
        self.s3_port = config['S3']['PORT']
        self.s3_bucket = config['S3']['BUCKET']
        #測試時需要解開註解
        self.s3_user = config['S3']['ASSESS_ID_FILE']
        self.s3_sec = config['S3']['ASSESS_KEY_FILE']
        #正式環境 正式執行需解開註解
        # self.s3_assess_id_file = config['S3']['ASSESS_ID_FILE']
        # self.s3_assess_key_file = config['S3']['ASSESS_KEY_FILE']
        # self.s3_user, self.s3_sec_str = readSecFile(self.s3_assess_id_file)
        # self.s3_salt = readSaltFile(self.s3_assess_key_file)
        # self.s3_sec = get_gpg_decrypt(self.s3_sec_str, self.s3_salt)

        """ FILE 相關參數"""
        self.source_path = config['FILE']['SOURCE_PATH']
        self.target_path = config['FILE']['TARGET_PATH']
        self.name_pattern = config['FILE']['NAME_PATTERN']
        self.encoding = config['FILE']["ENCODING"]
        self.header = config['FILE']['HEADER']
        #置換參數
        self.date = str(args.get('date'))

        """是否需要分隔欄寬"""
        self.delimiter = config['FILE']['DELIMITER']
        if self.delimiter == "N":
            self.col_size_file = config['FILE']['COL_SIZE_FILE']
        else:
            self.col_size_file = "N"

        """ CONTROLLER 相關參數，檢查檔案行數"""
        self.controller_file = config['CONTROLLER']['CONTROLLER_FILE']
        if self.controller_file == "Y":
            self.controller_file_name_pattern = config['CONTROLLER']['CONTROLLER_FILE_NAME_PATTERN']
            self.controller_file_delimiter = config['CONTROLLER']['CONTROLLER_FILE_DELIMITER']
        else:
            self.controller_file_name_pattern = None
            self.controller_file_delimiter = None
        self.controller_file_rows_count = None

        """檢查檔案行數與單筆寬度"""
        self.expected_row_length = 0
        self.total_rows_count = 0

        """ZIP 相關參數"""
        self.zip_type = config['ZIP']['ZIP_TYPE']
        #測試時需要解開註解
        self.zip_sec = config['ZIP']['SEC']
        #正式環境 正式執行需解開註解
        # self.zip_sec_file = config['ZIP']['SEC_FILE']
        # self.zip_key_file = config['ZIP']['KEY_FILE']
        # self.zip_user, self.zip_sec_str = readSecFile(self.zip_sec_file)
        # self.zip_salt = readSaltFile(self.zip_key_file)
        # self.zip_sec = get_gpg_decrypt(self.zip_sec_str, self.zip_salt)

        """檔案暫存區"""
        self.temp_base_path = config['TEMP']["TEMP_BASE_PATH"]
        self.temp_operation_folder_name= config['TEMP']["TEMP_OPERATION_FOLDER_NAME"]
        self.temp_operation_folder_path = os.path.join(self.temp_base_path, self.temp_operation_folder_name)
        
        """初始化清空此次運作的暫存資料夾，避免 debug 結束後，資料夾的內容存在，導致影響執行結果"""
        CleanTempFile.remove_temp_operation_directory(self.temp_operation_folder_path)
        time.sleep(3)
        
        """建立此次執行時的資料夾"""
        if not os.path.exists(self.temp_operation_folder_path):
            os.makedirs(self.temp_operation_folder_path)  # 建立此次執行時的資料夾
        self.temp_download_path = os.path.join(self.temp_operation_folder_path, "downloads")
        self.temp_processing_path = os.path.join(self.temp_operation_folder_path, "processing")
        self.temp_upload_path = os.path.join(self.temp_operation_folder_path, "uploads")
        
        if not os.path.exists(self.temp_download_path):
            os.makedirs(self.temp_download_path)
        if not os.path.exists(self.temp_processing_path):
            os.makedirs(self.temp_processing_path)
        if not os.path.exists(self.temp_upload_path):
            os.makedirs(self.temp_upload_path)

        """logger 變數"""
        self.main_config = main_config
        self.logger_main = None
        
        self.ftp_dao = FtpDaoImpl(
            ftp_type=self.ftp_type, 
            host=self.host, 
            port=self.port, 
            user=self.user, 
            sec=self.sec,
            logger=None)  # 暫時設為 None，等 logger 初始化後再更新
        

    def _initialize_logger(self):
        log_config = self.main_config
        ftp_log_path = f"{log_config['LOG']['LOG_PATH']}/ftp_loader"
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        processed_name_pattern = self.ftp_dao._process_name_pattern(self.name_pattern, self.date)
        logger_file_name = f"{processed_name_pattern}_{timestamp}"

        Logger.Logger(ftp_log_path, logger_file_name)
        self.logger_main = logging.getLogger(logger_file_name)
        self.ftp_dao.logger = self.logger_main # 更新 ftp_dao 的 logger

    def run(self):
        """ 執行連線和建立 logger """
        self._initialize_logger()
        self.logger_main.info("連線 FTP/SFTP Server 完成。")

        
        """列出的檔案"""       
        try:
            files_list = self.getFtpFileList()
        except Exception as e:
            self.logger_main.error(f"列出 FTP/SFTP Server 的檔案失敗: {e}")
            return False
        if not files_list:
            self.logger_main.info("沒有找到符合條件的檔案")
            return False
        self.logger_main.info(f"指定的檔案列表: {files_list}")

        
        download_files_list = []
        """下載檔案"""
        for file_name in files_list:
            try:
                self.downloadFtpFile(file_name)
            except Exception as e:
                self.logger_main.error(f"下載檔案 {file_name} 失敗: {e}")
                return False
            download_files_list.append(file_name)
        self.logger_main.info(f"完成下載檔案至暫存區: {download_files_list}")
        

        """如果下載的檔案是壓縮檔先解壓縮"""
        if self.name_pattern.lower().endswith((".zip", ".7z", ".tar", ".gz", ".tgz", ".tar.gz")):
            try:
                result = self.unzipFile(download_files_list)
            except Exception as e:
                self.logger_main.error(f"解壓縮檔案失敗: {e}")
                return False
            download_files_list = result
            self.logger_main.info(f"解壓縮檔案完成，檔案暫存至處理區，解壓縮後的檔案列表: {result}")
        else:
            for file_name in download_files_list:
                shutil.copy(os.path.join(self.temp_download_path, file_name), os.path.join(self.temp_processing_path, file_name))
            self.logger_main.info(f"檔案暫存至處理區，檔案列表: {download_files_list}")
        

        #檢查設定檔設置的 Batch Date
        if self.controller_file == "Y":
            try:
                result = self.checkBatchDate(download_files_list)
            except Exception as e:
                self.logger_main.error(f"檢查設定檔日期失敗 {e}")
                return False
            if result is True:
                self.logger_main.info(f"設定檔日期檢查 {file_name} 配置正確")

        #驗證檔案下載後行數是否正確，或是紀錄下載後的總數
        try:
            self.checkFileRows(download_files_list)
        except Exception as e:
            self.logger_main.error(f"檢查檔案行數失敗: {e}")
        
        
        #過濾掉設定檔，設定檔不需要進入 Reformat 流程和上傳至 S3
        try:
            processing_files_list = self.filteFIles(download_files_list)
        except Exception as e:
            self.logger_main.error(f"過濾設定檔遇到意外錯誤 {e}")
            return False
        self.logger_main.info(f"需轉換的檔案列表: {processing_files_list}")


        #檢查檔案是否可以正常解碼，並且記錄有問題的檔案名稱跟行位置
        try:
            result = self.checkDecodeValid(processing_files_list)
        except Exception as e:
            self.logger_main.error(f"檢查檔案編碼有效性出現意外錯誤 {e}")
            return False
        if result == True:
            self.logger_main.info(f"檔案編碼檢查完成，檔案列表: {processing_files_list}")
        elif result != True:
            self.logger_main.error(f"檔案編碼檢查完成，有錯誤的編碼檔案 {result}")
            return False

        #移除 header，避免影響檔案行數檢查，檔案保存於 temp_processing_file_path
        if self.header == "Y":
            try:
                self.removeHeaderLine(processing_files_list)
            except Exception as e:
                raise Exception(f"{file_name} 移除欄位標題行失敗: {e}")
            self.logger_main.info(f"{file_name} 完成移除欄位標題行")
       

        #有配置COL_SIZE_FILE 需要確認檔案資料是否都是正確的
        if self.col_size_file != "N":
            try:
                result = self.checkRowsLength(processing_files_list)
            except Exception as e:
                self.logger_main.error("檢查每筆資料長度出現意外錯誤")
            if result is True:
                self.logger_main.info(f"驗證每筆資料長度完成，開始插入分隔符號")
            elif result is not True:
                self.logger_main.error(f"有資料長度不符合規範，請檢查：{result}")
                return False

        #開始進行檔案分隔跟轉碼
        reformated_files_list = []
        for file_name in processing_files_list:
            try:
                self.reformatFile(file_name)
            except Exception as e:
                self.logger_main.error(f"檔案轉換失敗: {e}")
                return False
            reformated_files_list.append(file_name)
        self.logger_main.info(f"檔案轉換完成，轉換後的檔案列表: {reformated_files_list}")

       
        #檢查轉換後的檔案行數 
        self.total_rows_count = 0
        for file_name in reformated_files_list:
            result = Validator.get_file_line_count(file_name, self.temp_upload_path, controller_file_delimiter = None, controller_file_name_pattern = None)
            self.total_rows_count += result[2]
        
        if self.controller_file == "Y":
            if self.total_rows_count != self.controller_file_rows_count:
                self.logger_main.error(f"轉換後的檔案行數不符，預期筆數：{self.controller_file_rows_count}，實際筆數：{self.total_rows_count}")
                return False
        elif self.controller_file == 'N':
            self.logger_main.info(f"轉換後的檔案行數檢查完成，轉換後的檔案行數總和: {self.total_rows_count}")
    
        """上傳檔案到S3"""
        for file_name in reformated_files_list:
            try:
                self.writeToS3(file_name)
            except Exception as e:
                self.logger_main.error(f"上傳檔案 {file_name} 失敗: {e}")
                return False

        # time.sleep(10)
        # CleanTempFile.remove_temp_operation_directory(self.temp_operation_folder_path)
        # self.logger_main.info("清空暫存目錄完成。") 

        self.close()
        self.logger_main.info("FTP/SFTP 連線已關閉。")
        return True

    def getFtpFileList(self):
        files = self.ftp_dao.listFiles(self.source_path, self.name_pattern, self.date)
        return files

    def downloadFtpFile(self, file_name):
        self.ftp_dao.downloadFile(file_name, self.source_path, self.temp_download_path)

    def unzipFile(self, download_files_list):
        unzip_files_list = [] 
        for file_name in download_files_list:
            download_file_path = os.path.join(self.temp_download_path, file_name)
            unzip_files = Compressor.decompress(download_file_path, self.temp_processing_path, self.zip_sec)
            if not unzip_files:
                raise Exception(f"{file_name} 解壓縮後沒有檔案")
            for unzip_file in unzip_files:
                unzip_files_list.append(unzip_file)
        return unzip_files_list 

    def checkBatchDate(self, download_files_list):
        for file_name in download_files_list:
            if self.ftp_dao._match_name_pattern(file_name, self.controller_file_name_pattern):
                controller_file_path = os.path.join(self.temp_processing_path, file_name)
                result = Validator.check_header_batch_date(controller_file_path, self.controller_file_delimiter, self.date)
            else:
                continue
        return result
        
    def checkFileRows(self, download_files_list):
        """讀出下載檔案行數"""
        file_rows_counts_info = []
        for file_name in download_files_list:
            try:
                result = Validator.get_file_line_count(file_name, self.temp_processing_path, self.controller_file_delimiter, self.controller_file_name_pattern)
                file_rows_counts_info.append(result)
                if result[0] == "檢核檔":
                    self.controller_file_rows_count = result[2] #紀錄正確筆數至Attribute利於後續驗證
            except Exception as e:
                self.logger_main.error(f"檢查檔案行數失敗: {e}")
                return False
        self.logger_main.info(f"檔案行數檢查完成，下載檔案行數紀錄：{file_rows_counts_info}")

        """檢查檔案行數是否符合設定檔設置的筆數"""
        if self.controller_file == "Y":
            try:
                result = Validator.check_file_line_count(file_rows_counts_info, self.header)
            except Exception as e:
                self.logger_main.error(f"檔案行數檢查失敗: {e}")
                return False
            if result == True:
                self.logger_main.info("核對明細檔和設定檔筆數正確")
        elif self.controller_file == 'N':
            for kind, fname, rows_count in file_rows_counts_info:
                self.total_rows_count += rows_count
                if self.header == "Y" and  kind == "檔案":
                    self.total_rows_count -= 1
            self.logger_main.info(f"檔案行數檢查完成，下載檔案行數總和: {self.total_rows_count}")

    def filteFIles(self, download_files_list):
        processing_files_list = []
        if self.controller_file == "Y":
            for file_name in download_files_list:
                if self.ftp_dao._match_name_pattern(file_name, self.controller_file_name_pattern):
                    continue
                else:
                    processing_files_list.append(file_name)
        else:
            processing_files_list = download_files_list.copy()
        return processing_files_list

    def checkDecodeValid(self, processing_files_list):
        problematic_files_list = []
        for file_name in processing_files_list:
            try:
                file_path = os.path.join(self.temp_processing_path, file_name)
                problematic_lines = Reformatter.checking_decoding_valid(file_path, self.encoding)
                if len(problematic_lines) > 0:
                    self.logger_main.error(f"檔案 {file_name} 有 {len(problematic_lines)} 行資料有解碼問題，請根據以下行數檢查資料 {problematic_lines} ")
                    problematic_files_list.append(file_name)
            except Exception as e:
                raise Exception(f"檢查檔案編碼失敗: {e}")
        
        if len(problematic_files_list) > 0 :
            return problematic_files_list
        return True

    def removeHeaderLine(self, processing_files_list):
        for file_name in processing_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file_name)
            Reformatter.remove_header(temp_processing_file_path, temp_processing_file_path, self.encoding)

    def checkRowsLength(self, processing_files_list):
        problematic_row_length_file_list = []
        for file_name in processing_files_list:
            try: 
                temp_processing_file_path = os.path.join(self.temp_processing_path, file_name)
                error_lines = Reformatter.checking_file_valid(temp_processing_file_path, self.col_size_file, self.encoding)
            except Exception as e:
                raise Exception("檢查檔案每筆長度出現錯誤")
            if error_lines is not True :
                problem_file = (file_name, error_lines)
                problematic_row_length_file_list.append(problem_file)
        if len(problematic_row_length_file_list) > 0:
            return problematic_row_length_file_list
        elif len(problematic_row_length_file_list) == 0:
            return True

    def reformatFile(self, file):
        temp_processing_file_path = os.path.join(self.temp_processing_path, file)
        temp_upload_file_path = os.path.join(self.temp_upload_path, file)

        """檢查檔案是否已經為已經有分隔符號的檔案，如果是則不需要插入分隔符號，檔案暫存於 temp_processing_file_path"""
        if self.delimiter != "N":
            self.logger_main.info(f"{file} 已經有分隔符號，不需要插入分隔符號")

        elif self.col_size_file != "N":  #代表一定有長度檔，需要進行檔案分隔
            try:
                Reformatter.insert_delimiter_with_sizes_file(temp_processing_file_path, self.col_size_file, temp_processing_file_path,)
            except Exception as e:
                raise Exception(f"插入分隔符號 {file} 失敗: {e}")
            
            self.logger_main.info(f"{file} 插入分隔符號完成")

        """轉換編碼為 utf-8"""
        if self.encoding == "big5":
            try:
                Reformatter.encoding_to_uft_8(temp_processing_file_path, self.encoding, temp_processing_file_path)
            except Exception as e:
                raise Exception(f"轉換編碼 {file} 失敗: {e}")
            self.logger_main.info(f" {file} 轉換編碼完成")
        elif self.encoding == "utf-8":
            self.logger_main.info(f"{file} 編碼為 utf-8，不需要轉換")
        else:
            raise Exception(f" {file} 編碼 {self.encoding} 不支援")

        """將檔案保存至 temp_upload_file_path"""
        self.logger_main.info(f"檔案 {file} 完成轉換")
        shutil.copy(temp_processing_file_path, temp_upload_file_path)

    def writeToS3(self, file):
        file_path = os.path.join(self.temp_upload_path, file)
        s3_file_path = os.path.join(self.target_path, file)

        try:
            s3Dao = S3DaoImpl(self.s3_bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
            s3Dao.uploadFile(file_path, s3_file_path)
            uploaded_file = s3Dao.listFiles(file)
        except Exception as e:
            raise Exception(f"上傳檔案 {file} 失敗: {e}")
        
        self.logger_main.info(f"檔案上傳結果: {uploaded_file}")

    def close(self):
        self.ftp_dao.close()