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
    def listFiles(self, pattern: str) -> List[str]:
        pass

    @abc.abstractmethod
    def downloadFile(self, remote_path: str, local_path: str) -> None:
        pass

    @abc.abstractmethod
    def uploadFile(self, local_path: str, remote_path: str) -> None:
        pass