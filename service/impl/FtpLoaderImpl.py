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
from dao.FileDao import FileDao  # 抽象
# from dao.impl.FtpDaoImpl import FtpDaoImpl 



class FtpLoaderImpl(FtpLoader):
    def __init__(self, dao: FileDao):
        self.ftp_dao = dao
    
    def run(self):
        """ 執行連線 """
        self.getFtpConnection()
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
        
    def getFtpConnection(self):
        self.ftp_dao.connect()

    def getFtpFileList(self):
        files = self.ftp_dao.listFiles()
        return files

    def downloadFtpFile(self, name, size):
        downloaded = self.ftp_dao.downloadFile(name, size)
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