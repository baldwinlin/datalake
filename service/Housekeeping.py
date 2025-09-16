#coding:utf-8
'''
Housekeeping.py
Object          : 資料庫清理
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1. 提供需清理的Hive db 和 Table name 或是 S3 Object Store path 和 file name，以及需要保留的天數，跟今天的Batch Date
                  2. 依據輸入的 Batch Date 找出超過保留天數的資料，並進行清理，例如 Batch Date 為 202509010，需要保留 7 天，則需要清理 20250903 之前的資料
                  3. Hive 清理，依據輸入的 Hive db 和 Table name，進行清理，table 會有一個欄位是 batch_date，需要清理超過保留天數的資料
                  4. S3 Object Store 清理，依據輸入的 S3 Object Store path 和 file name，進行清理，file name 會包含 Batch Date，需要清理超過保留天數的資料
Parameters      :
Output          :
********************************************************************************
Modify          :
'''

import abc

class Housekeeping(abc.ABC):

    @abc.abstractmethod
    def __init__(self):
        return NotImplemented

    @abc.abstractmethod
    def run(self, housekeeping_config_file):
        return NotImplemented


