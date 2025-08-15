#coding:utf-8
'''
S3FileDaoImpl.py
Object          : S3 file access - oupload files to target path and download files from source path
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

class S3FileDaoImpl(FileDao):

    def downloadFiles(self, source_path, file_list):
        return NotImplemented

    def uploadFiles(self, target_path, file_list):
        return NotImplemented