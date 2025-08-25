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
from typing import List

# ========== 抽象介面 ==========
class FileDao(abc.ABC):

    @abc.abstractmethod
    def connect(self, ftp_type, host, port, user, password, timeout, passive):
        pass
    
    @abc.abstractmethod
    def listFiles(self) -> List[str]:
        pass

    @abc.abstractmethod
    def downloadFile(self, name, size) -> None:
        pass

    @abc.abstractmethod
    def uploadFile(self, local_path: str, remote_path: str) -> None:
        pass

    @abc.abstractmethod
    def close(self):
        pass