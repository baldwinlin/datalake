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


class FtpLoaderImpl(FtpLoader):
    def __init__(self,main_config, fc_config, pc_config, args):
        self.fc_config = fc_config
        self.pc_config = pc_config
        self.main_config = main_config
        self.args = json.loads(args)
        self.date =str(self.args.get('batch_date'))
        self.logger_main = None
        self.logger_prefix = self.pc_config["LOG"].get("LOG_PREFIX")
        if not self.logger_prefix:
            raise Exception("LOG_PREFIX 不得為空值")
        self.log_level = self.main_config["LOG"].get("LOG_LEVEL", "INFO").upper()
        self.expected_row_length = 0
        self.total_rows_count = 0
        self.controller_file_rows_count = 0

        """建立暫存工作目錄"""
        try:
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
            self.host = self.fc_config.get('FTP','FTP_IP')
            self.port = int(self.fc_config.get('FTP','FTP_PORT'))
            self.ftp_type = self.fc_config.get('FTP','FTP_TYPE')
            #測試時需要解開註解
            self.user = self.fc_config.get('FTP','FTP_USER')
            self.sec = self.fc_config.get('FTP','FTP_SEC')
            #正式環境 正式執行需解開註解
            # self.ftp_sec_file = self.fc_config.get('FTP','FTP_SEC_FILE')
            # self.ftp_key_file = self.fc_config.get('FTP','FTP_SEC_KEY')
            # self.user, self.sec_str = readSecFile(self.ftp_sec_file)
            # self.ftp_salt = readSaltFile(self.ftp_key_file)
            # self.sec = get_gpg_decrypt(self.sec_str, self.ftp_salt)
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
            self.encoding = self.pc_config.get('SOURCE',"ENCODING").lower()
            if self.encoding == "":
                raise Exception("ENCODING 不得為空值")
            elif self.encoding not in ["big5", "utf-8"]:
                raise Exception(f"編碼 {self.encoding} 不支援")
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
                if self.date == "" or self.date == "None" or len(self.date) != 8:
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
        except Exception as e:
            raise Exception(f"讀取TARGET config錯誤: {e}")

        """設定解壓縮處理"""
        try:
            self.zip_type = self.pc_config.get('ZIP','ZIP_TYPE')
            self.zip_sec_file = self.pc_config.get('ZIP','ZIP_SEC_FILE')
            self.zip_key_file = self.pc_config.get('ZIP','ZIP_SEC_KEY_FILE')
            if self.zip_sec_file and self.zip_key_file:
                self.zip_user, self.zip_sec_str = readSecFile(self.zip_sec_file)
                self.zip_salt = readSaltFile(self.zip_key_file)
                self.zip_sec = get_gpg_decrypt(self.zip_sec_str, self.zip_salt)
            else:
                self.zip_user = None
                self.zip_sec = None
            # self.zip_sec ="123456"
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
        ftp_log_path = f"{log_config['LOG']['LOG_PATH']}/ftp_loader"
        
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
        files_list = self.getFtpFileList()
        if not files_list:
            self.errorExit("沒有找到符合條件的檔案")
        self.logger_main.info(f"指定的檔案列表: {files_list}")

        """下載檔案"""
        download_files_list = self.downloadFtpFile(files_list)
        if not download_files_list:
            self.errorExit("沒有成功下載指定的檔案")
        self.logger_main.info(f"完成下載檔案至暫存區: {download_files_list}")
        

        """壓縮檔解壓縮"""
        if self.name_pattern.lower().endswith((".zip", ".7z", ".tar", ".gz", ".tgz", ".tar.gz")):
            unzip_files_list  = self.unzipFile(download_files_list)
            download_files_list = unzip_files_list 
            self.logger_main.info(f"解壓縮檔案完成，檔案暫存至處理區，解壓縮後的檔案列表: {unzip_files_list}")
        else:
            for file_name in download_files_list:
                shutil.copy(os.path.join(self.temp_download_path, file_name), os.path.join(self.temp_processing_path, file_name))
            self.logger_main.info(f"檔案暫存至處理區，檔案列表: {download_files_list}")
        
        """驗證控制檔日期"""
        if self.controller_file == "Y":
            controller_file_name = self.checkBatchDate(download_files_list)
            if controller_file_name is None:
                self.errorExit(f"控制檔日期檢查失敗可能設定檔有誤或是沒有控制檔")
            self.logger_main.info(f"控制檔日期檢查 {controller_file_name} 配置正確")

        """驗證下載檔案行數"""
        self.checkFileRows(download_files_list)
        
        """過濾掉控制檔"""
        processing_files_list = self.filteFIles(download_files_list)
        self.logger_main.info(f"需轉換的檔案列表: {processing_files_list}")

        """解碼驗證"""
        result = self.checkDecodeValid(processing_files_list)
        if result == True:
            self.logger_main.info(f"檔案編碼檢查完成，檔案列表: {processing_files_list}")
        
        """解碼後檢查檔案行數"""
        self.processCheckFileRows(processing_files_list, self.header)
        self.logger_main.info(f"解碼驗證的檔案行數檢查完成，檔案行數總和: {self.total_rows_count}")

        """移除欄位標題"""
        if self.header == "Y":
            result = self.removeHeaderLine(processing_files_list)
            if result == True:
                self.logger_main.info(f"完成移除欄位標題行")
                """移除欄位後後檢查檔案行數"""
                self.processCheckFileRows(processing_files_list, "N")
                self.logger_main.info(f"移除欄位標題，檔案行數檢查完成，轉換後的檔案行數總和: {self.total_rows_count}")
       
        """檔案資料長度檢查（固定長度檔案）"""
        if self.col_size_file:
            result = self.checkRowsLength(processing_files_list)
            if result is True:
                self.logger_main.info(f"驗證每筆資料長度完成，開始插入分隔符號")

        """檔案分隔"""
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
                self.processCheckFileRows(processing_files_list, "N")
                self.logger_main.info(f"置入分隔符號後行數檢查完成，檔案行數總和: {self.total_rows_count}")



        """檔案轉碼"""
        reformated_files_list = []
        if self.encoding == "big5":
            result = self.reformatFile(processing_files_list)
            reformated_files_list = result
            if result:
                self.processCheckFileRows(processing_files_list, "N")
                self.logger_main.info(f"轉換編碼後，行數檢查完成，檔案行數總和: {self.total_rows_count}")
        elif self.encoding == "utf-8":
            reformated_files_list = processing_files_list
            self.logger_main.info(f"編碼為 utf-8，不需要轉換")


        for file in reformated_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file)
            temp_upload_file_path = os.path.join(self.temp_upload_path, file)
            shutil.copy(temp_processing_file_path, temp_upload_file_path)


     
        """上傳檔案到S3"""
        error_upload_files_list = []
        for file_name in reformated_files_list:
            try:
                self.writeToS3(file_name)
            except Exception as e:
                self.logger_main.error(f"上傳檔案失敗 {e}")
                error_upload_files_list.append(file_name)
        
        if len(error_upload_files_list) > 0:
            self.errorExit(f"上傳檔案失敗錯誤表 {error_upload_files_list}")

        time.sleep(10)
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
            files = self.ftp_dao.listFiles(self.source_path, self.name_pattern, self.date)
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

    def checkBatchDate(self, download_files_list):
        for file_name in download_files_list:
            try:
                is_controller_file = FilenameProcessor.is_controller_file(file_name, self.controller_file_name_pattern)
            except Exception as e:
                self.errorExit(f"檢查檔案是否為控制檔失敗 {e}")

            if is_controller_file:
                controller_file_path = os.path.join(self.temp_processing_path, file_name)
                try:
                    result = Validator.check_header_batch_date(controller_file_path, self.controller_file_delimiter, self.date)
                    return file_name
                except Exception as e:
                    self.errorExit(f"檢查控制檔日期失敗 {e}")
    
    def checkFileRows(self, download_files_list):
        """讀出下載檔案行數"""
        file_rows_counts_info = []
        file_counts = 0
        with_header_expected_rows_count = 0
        for file_name in download_files_list:
            try:
                result = Validator.get_file_line_count(file_name, self.temp_processing_path, self.header, self.controller_file_delimiter, self.controller_file_name_pattern)
                file_rows_counts_info.append(result)
               
                if result[0] == "檢核檔":
                    with_header_expected_rows_count += result[2]
                    self.controller_file_rows_count = result[2] #紀錄正確筆數至Attribute利於後續驗證
                if result[0] == "檔案":
                    self.logger_main.info(f"檔案行數檢查完成，{result[0]}: {result[1]}下載檔案行數紀錄，{result[3]}")
                    file_counts += 1

            except Exception as e:
                self.errorExit(f"計算檔案行數失敗 {file_name} {e}")
        
        if self.header == "Y" and self.controller_file == "Y":
            with_header_expected_rows_count += file_counts
            self.logger_main.info(f"控制檔預期筆數：{with_header_expected_rows_count}")
        elif self.header == 'N' and self.controller_file == "Y":
            self.logger_main.info(f"控制檔預期筆數：{with_header_expected_rows_count}")
        

        

        """檢查檔案行數是否符合控制檔設置的筆數"""
        if self.controller_file == "Y":
            try:
                result = Validator.check_file_line_count(file_rows_counts_info)
            except Exception as e:
                self.errorExit(f"檔案行數檢查失敗 {e}")
            if result == True:
                self.logger_main.info("核對明細檔和控制檔筆數正確")
        elif self.controller_file == 'N':
            for kind, fname, rows_count, _ in file_rows_counts_info:
                self.total_rows_count += rows_count
            self.logger_main.info(f"檔案行數檢查完成，下載檔案行數總和: {self.total_rows_count}")
    
    def processCheckFileRows(self, processing_files_list, header_line):
        error_files_list = []
        proccessing_rows_count = 0
        file_counts = 0
        """讀出下載檔案行數"""
        file_rows_counts_info = []
        for file_name in processing_files_list:
            try:
                result = Validator.get_file_line_count(file_name, self.temp_processing_path, header_line, None, None)
                file_rows_counts_info.append(result)
                proccessing_rows_count += result[2]
                file_counts += 1
                self.logger_main.info(f"檔案行數檢查完成，{result[0]}: {result[1]}下載檔案行數紀錄，{result[3]}")
            except Exception as e:
                self.logger_main.error(f"檢查檔案行數失敗 {file_name} {e}")
                error_files_list.append(file_name)

        if len(error_files_list) > 0:
            self.errorExit(f"計算檔案行數失敗 {error_files_list}")

        """檢查檔案行數是否符合控制檔設置的筆數"""
        if self.controller_file == "Y":
            if proccessing_rows_count != self.controller_file_rows_count:
                self.errorExit(f"檔案行數不符，預期筆數：{self.controller_file_rows_count}，實際筆數：{proccessing_rows_count}")
        elif self.controller_file == 'N':
            if proccessing_rows_count != self.total_rows_count:
                self.errorExit(f"檔案行數不符，預期筆數：{self.total_rows_count}，實際筆數：{proccessing_rows_count}")
        
        

        if header_line == "Y":
            with_header_counts = proccessing_rows_count + file_counts
            self.total_rows_count = with_header_counts
        elif header_line == "N":
            self.total_rows_count = proccessing_rows_count
        

    def filteFIles(self, download_files_list):
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

    def checkDecodeValid(self, processing_files_list):
        problematic_files_list = []
        error_files_list = []
        for file_name in processing_files_list:
            try:
                file_path = os.path.join(self.temp_processing_path, file_name)
                problematic_lines = Validator.checking_decoding(file_path, self.encoding)
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

    def removeHeaderLine(self, processing_files_list):
        error_files_list = []
        for file_name in processing_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file_name)
            try:
                Reformatter.remove_header(temp_processing_file_path, temp_processing_file_path, self.encoding)
            except Exception as e:
                self.logger_main.error(f"移除欄位標題行失敗 {file_name} {e}")
                error_files_list.append(file_name)
       
        if len(error_files_list) > 0:
            self.errorExit(f"移除欄位標題行失敗 {error_files_list}")

        return True

    def checkRowsLength(self, processing_files_list):
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

    def insertDelimiter(self, processing_files_list):
        error_files_list = []
        for file in processing_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file)

            """檢查檔案是否已經為已經有分隔符號的檔案，如果是則不需要插入分隔符號，檔案暫存於 temp_processing_file_path"""
            try:
                Reformatter.insert_delimiter_with_sizes_file(temp_processing_file_path, self.col_size_file, temp_processing_file_path, self.delimiter)
            except Exception as e:
                self.logger_main.error(f"插入分隔符號 {file} 失敗: {e}")
                error_files_list.append(file)
        
        if len(error_files_list) > 0:
            self.errorExit(f"插入分隔符號失敗的檔案列表 {error_files_list}")
        else:
            self.logger_main.info(f"{file} 插入分隔符號完成")
            return True
                
    def reformatFile(self, processing_files_list):
        error_files_list = []
        reformated_files_list = []
        for file in processing_files_list:
            temp_processing_file_path = os.path.join(self.temp_processing_path, file)
        
            """轉換編碼為 utf-8"""
            try:
                Reformatter.encoding_to_uft_8(temp_processing_file_path, self.encoding, temp_processing_file_path)
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
            uploaded_file = s3Dao.listFiles(f"*{file}")
        except Exception as e:
            raise Exception(f"上傳檔案 {file} 失敗: {e}")
            
        self.logger_main.info(f"檔案上傳結果: {uploaded_file}")

    def close(self):
        self.ftp_dao.close()