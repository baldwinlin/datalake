#coding:utf-8
'''
FtpDaoImpl.py
Object          : FTP file access - oupload files to target path and download files from source path
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

from dao.FileDao import FileDao

class FtpDaoImpl(FileDao):
    def __init__(self):
        self.test_txt = 'test'

    def downloadFiles(self, source_path, file_list):
        return NotImplemented

    def uploadFiles(self, target_path, file_list):
        return NotImplemented

if __name__ == '__main__':
    ftpDao = FtpDaoImpl()
    print(ftpDao.test_txt)