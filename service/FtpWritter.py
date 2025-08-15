#coding:utf-8
'''
FtpLoader.py
Object          : 從Hive table或S3 Object Store取得檔案並上傳至FTP路徑.
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1.1 指定Object Store file或(1.2)
                  1.2 指定Hive table並產出檔案，格式如下︰
                      a. 檔案名稱Pattern(ex: aaa*_${date}.CSV)，可置換日期變數
                      b. 可選擇檔案編碼(UTF-8，BIG5...)
                      c. 換行符號(\n, \r\n)
                      d. 可提供表頭
                      e. 分隔符號設定(ex: '|', '!~', '\u0006') 或 固定長度設定
                      f. 檔案可壓縮(可帶密碼)
                  2. 將檔上傳至FTP路徑
Parameters      : 1. FTP config file(FTP Host/FTP path/檔案名稱Pattern/解壓型式/解壓密碼)
                  1.1 Date
                  2. Object Store config file(Object Store path/file name/Access ID/Access Key)
                  3. Hive config file(IP/Port/DB name/table name/分隔符號/固定長度)
Output          :
********************************************************************************
Modify          :
'''

import abc

class FtpWritter(abc.ABC):

    @abc.abstractmethod
    def __init__(self):
        return NotImplemented

    '''
        FTP Writter main flow
    '''
    @abc.abstractmethod
    def run(self):
        return NotImplemented


    '''
        @ftp_confif_file: from command argument
        return: ftp_file_name
    '''
    @abc.abstractmethod
    def getFileFromHive(self, ftp_confif_file):
        return NotImplemented

    '''
        @ftp_confif_file: from command argument
        @ftp_file_name: the file from Hive or S3
    '''
    @abc.abstractmethod
    def writeToFtp(self, htp_config_file, ftp_file_name):
        return NotImplemented

    '''
        Use boto3 module, to be confirmed.
    '''
    @abc.abstractmethod
    def getS3File(self):
        return NotImplemented

    '''
        Use boto3 module, to be confirmed.
        @object_store_path/@access_id/@access_key: get form FTP config file
    '''
    @abc.abstractmethod
    def writeToS3(self, ftp_file_list, object_store_path, access_id, access_key):
        return NotImplemented