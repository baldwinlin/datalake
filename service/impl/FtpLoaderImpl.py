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

import configparser
from service.FtpLoader import *

class FtpLoaderImpl(FtpLoader):
    def __init__(self, config, args):
        self.config = config
        self.args = args

    def run(self):
        print("FTP Loader running ...")
        print("FTP IP = ", self.config['FTP']['FTP_IP'])

    def getFtpConnection(self, ftp_config):
        pass

    '''
        @ftp_path/name_pattern: read from the ftp config file.
        @date_string: from command argument
        return: ftp_file_list
    '''
    def getFtpFileList(self, ftp_path, name_pattern, date_string):
        pass

    '''
        @work_path: read from config file
    '''
    def downloadFtpFile(self, ftp_connection, ftp_file_list, work_path):
        pass

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