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

import paramiko
from ftplib import FTP
from typing import Optional



class FtpLoaderImpl(FtpLoader):
    def __init__(self, config, args):
        self.config = config
        self.args = args

        """設定 FTP 和 SFTP 共有 attribution"""
        self.host = config['FTP']['FTP_IP']
        self.port = int(config['FTP']['FTP_PORT'])
        self.user = config['FTP']['FTP_USER']
        self.password = config['FTP']['FTP_PASSWORD']
        self.ftp_type = config['FTP']['FTP_TYPE']
        """FILE 相關參數"""
        self.source_path = config['FILE']['SOURCE_PATH']
        self.target_path = config['FILE']['TARGET_PATH']
        self.name_pattern = config['FILE']['NAME_PATTERN']
        
        self.ftp_dao = FtpDaoImpl(
            ftp_type=self.ftp_type, 
            host=self.host, 
            port=self.port, 
            user=self.user, 
            password=self.password, 
            name_pattern=self.name_pattern, 
            args=args)

    def run(self):
        """ 執行連線 """
        print("FTP/SFTP 連線已建立。")

        """列出的檔案"""
        files_list = self.getFtpFileList()
        if files_list:
            for name, size in files_list:
                print(f"檔案名稱: {name}, 大小: {size} bytes")
                downloaded_file = self.downloadFtpFile(name, size)
                print(downloaded_file)
        else:
            print("沒有找到符合條件的檔案")

        self.close()
        print("FTP/SFTP 連線已關閉。")
        
    def getFtpFileList(self):
        files = self.ftp_dao.listFiles(self.source_path)
        return files

    def downloadFtpFile(self, name, size):
        downloaded = self.ftp_dao.downloadFile(name, size, self.source_path, self.target_path)
        return downloaded

    '''
        zip_type: get from ftp config file
    '''
    def unzipFile(self, ftp_file_list, zip_type):
        pass

    '''
        @delimiter/fix_size_file: get from hive config file.
    '''
    def reformatFile(self, ftp_file_list, delimiter, fix_size_file):
        pass

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