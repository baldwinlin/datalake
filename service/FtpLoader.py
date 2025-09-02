#coding:utf-8
'''
FtpLoader.py
Object          : 取得FTP檔案並寫入Hive table或S3 Object Store.
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1. 指定FTP路徑，取得對應Pattern的檔案(ex: aaa*_${date}.CSV，可置換日期變數
                  2. 檔案可能需要解壓縮(可能有密碼)
                  3.1. 下載檔案存入S3 Object Store定路徑或(3.2.)
                  3.2. 寫入Hive table,此檔案格式可能需要:
                       a. 分隔符號解析(ex: '|', '!~', '\u0006')
                       b. 固定長度解析
Parameters      : 1. FTP config file(FTP Host/FTP path/檔案名稱Pattern/解壓型式/解壓密碼)
                  1.1 Date
                  2. Object Store config file(Object Store path/file name/Access ID/Access Key)
                  3. Hive config file(IP/Port/DB name/table name/分隔符號/固定長度)
Output          :
********************************************************************************
Modify          :
'''
import abc

class FtpLoader(abc.ABC):


    # @abc.abstractmethod
    # def __init__(self):
    #     return NotImplemented

    '''
        FTP Loader main flow
    '''
    @abc.abstractmethod
    def run(self):
        return NotImplemented

    '''
        @ftp_config: read from the ftp config file.
        return: ftp connection object.
    '''
    # @abc.abstractmethod
    # def getFtpConnection(self):
    #     return NotImplemented

    '''
        @ftp_path/name_pattern: read from the ftp config file.
        @date_string: from command argument
        return: ftp_file_list
    '''
    @abc.abstractmethod
    def getFtpFileList(self):
        return NotImplemented

    '''
        @work_path: read from config file
    '''
    @abc.abstractmethod
    def downloadFtpFile(self, name, size):
        return NotImplemented

    '''
        zip_type: get from ftp config file
    '''
    @abc.abstractmethod
    def unzipFile(self, ftp_file_list, zip_type):
        return NotImplemented

    '''
        @delimiter/fix_size_file: get from hive config file.
    '''
    @abc.abstractmethod
    def reformatFile(self, ftp_file_list, delimiter, fix_size_file):
        return NotImplemented

    # '''
    #     @db_name/@table_name: get from htp config file
    # '''
    # @abc.abstractmethod
    # def writeToHive(self, ftp_file_list, db_connection, db_name, table_name):
    #     return NotImplemented

    '''
        @object_store_path/@access_id/@access_key: get form ftp config file
    '''
    @abc.abstractmethod
    def writeToS3(self, ftp_file_list, object_store_path, access_id, access_key):
        return NotImplemented


    @abc.abstractmethod
    def close(self):
        return NotImplemented