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

from service.FtpLoader import *
from dao.impl.FtpDaoImpl import FtpDaoImpl  #FTP DAO 實現
from util.Reformatter import *
from util.Compressor import *

import paramiko
import os
from ftplib import FTP
from typing import Optional

import shutil



class FtpLoaderImpl(FtpLoader):
    def __init__(self,main_config, config, args):
        self.config = config
        self.args = args

        """設定 FTP 和 SFTP 共有 attribution"""
        self.host = config['FTP']['FTP_IP']
        self.port = int(config['FTP']['FTP_PORT'])
        self.user = config['FTP']['FTP_USER']
        self.password = config['FTP']['FTP_PASSWORD']
        self.ftp_type = config['FTP']['FTP_TYPE']
        """ FILE 相關參數"""
        self.source_path = config['FILE']['SOURCE_PATH']
        self.target_path = config['FILE']['TARGET_PATH']
        self.name_pattern = config['FILE']['NAME_PATTERN']
        """ Reformat 相關參數"""
        self.delimiter = config['FILE']['DELIMITER']
        self.col_size_file = config['FILE']['COL_SIZE_FILE']
        self.new_line_character = config['FILE']['NEW_LINE_CHARACTER']
        self.encoding = config['FILE']["ENCODING"]
        self.header = config['FILE']['HEADER']
        """檔案暫存區"""
        self.temp_download_path = main_config['LOCAL']["TEMP_DOWNLOAD_PATH"]
        self.temp_processing_path = main_config['LOCAL']['TEMP_PROCESSING_PATH']
        self.temp_upload_path = main_config['LOCAL']['TEMP_UPLOAD_PATH']
        
        
        self.ftp_dao = FtpDaoImpl(
            ftp_type=self.ftp_type, 
            host=self.host, 
            port=self.port, 
            user=self.user, 
            password=self.password)
        
        #置換參數
        self.date = str(args.get('date'))

    def run(self):
        """ 執行連線 """
        print("FTP/SFTP 連線已建立。")

        """列出的檔案"""
        files_list = self.getFtpFileList()
        if files_list:
            for name, size in files_list:
                print(f"檔案名稱: {name}, 大小: {size} bytes")
                downloaded_file = self.downloadFtpFile(name, size)
                file_path = os.path.join(self.temp_download_path, downloaded_file)
                if name.lower().endswith((".zip", ".7z", ".tar", ".gz", ".tgz", ".tar.gz")):
                    print(f"{name} 壓縮檔需要解壓縮")
                    unzip_files = self.unzipFile(file_path, self.temp_processing_path)
                    for unzip_file in unzip_files:
                        print(f"解壓縮後的檔案名稱：{unzip_file}")
                        self.reformatFile(unzip_file)
                else:
                    temp_processing_path = os.path.join(self.temp_processing_path, downloaded_file)
                    shutil.copy(file_path, temp_processing_path)
                    self.reformatFile(downloaded_file)
        else:
            print("沒有找到符合條件的檔案")

        self.close()
        print("FTP/SFTP 連線已關閉。")
        
    def getFtpFileList(self):
        files = self.ftp_dao.listFiles(self.source_path, self.name_pattern, self.date)
        return files

    def downloadFtpFile(self, name, size):
        downloaded = self.ftp_dao.downloadFile(name, size, self.source_path, self.temp_download_path)
        return downloaded

    '''
        zip_type: get from ftp config file
    '''
    def unzipFile(self, file_path, temp_processing_path):
        unzip_files_list = Compressor.decompress(file_path, temp_processing_path, None)
        return unzip_files_list
    '''
        @delimiter/fix_size_file: get from hive config file.
    '''
    def reformatFile(self, file):
        temp_processing_path = os.path.join(self.temp_processing_path, file)
        temp_path = os.path.join(self.temp_upload_path, file)

        if self.encoding == "big5":
            print(f"檔案 {file} encode 是 big5 需先進行轉換")
            Reformatter.encoding_to_uft_8(temp_processing_path, self.encoding, temp_processing_path)

        if self.header == "Y":
            print(f"檔案 {file} 有含 header，需要先移除 header")
            Reformatter.remove_header(temp_processing_path, temp_processing_path)
    
        if self.delimiter:
            print(f"檔案 {file} 已經分隔過，不需要插入分隔符號 ")
            shutil.copy(temp_processing_path, temp_path)
        elif self.col_size_file:
            print(f"檔案 {file} 需要插入分隔符號")
            Reformatter.insert_delimiter_with_sizes_file(temp_processing_path, self.col_size_file, temp_path)
            print(f"檔案 {file} 完成插入分隔符號")


    '''
        @db_name/@table_name: get from htp config file
    '''
    def writeToHive(self, ftp_file_list, db_connection, db_name, table_name):
        pass

    '''
        @object_store_path/@access_id/@access_key: get form ftp config file
    '''
    def writeToS3(self, ftp_file_list, object_store_path, access_id, access_key):
        pass

    def close(self):
        self.ftp_dao.close()