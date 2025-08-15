#coding:utf-8
'''
FileDao.py
Object          : File access - oupload files to target path and download files from source path
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

import abc

class FileDao(abc.ABC):

    def __init__(self):
        '''
        Initialize the Ffile connection
        '''
        return NotImplemented

    def downloadFiles(self, source_path, file_list):
        return NotImplemented

    def uploadFiles(self, target_path, file_list):
        return NotImplemented