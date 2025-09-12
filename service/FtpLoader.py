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
    '''
        FTP Loader main flow
    '''
    @abc.abstractmethod
    def run(self):
        return NotImplemented

    @abc.abstractmethod
    def getFtpFileList(self):
        return NotImplemented

    @abc.abstractmethod
    def downloadFtpFile(self, file_list):
        return NotImplemented

    @abc.abstractmethod
    def unzipFile(self, file_list):
        return NotImplemented

    @abc.abstractmethod
    def removeHeaderLine(self, file_list):
        return NotImplemented

    @abc.abstractmethod
    def insertDelimiter(self, file_list):
        return NotImplemented

    @abc.abstractmethod
    def reformatEncoding(self, file_list):
        return NotImplemented

    @abc.abstractmethod
    def writeToS3(self, file):
        return NotImplemented

    @abc.abstractmethod
    def close(self):
        return NotImplemented