#coding:utf-8
'''
UploadCheck.py
Object          : 上架檢核
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1. 依據指定的資料表與檢核表，依據指定欄位檢核資料筆數是否正確
                  2. 資料筆數不一致會失敗告警
                  3. 0筆告警(可設定忽略)
Parameters      :
Output          :
********************************************************************************
Modify          :
'''

import abc

class UploadCheck(abc.ABC):

    @abc.abstractmethod
    def __init__(self):
        return NotImplemented

    @abc.abstractmethod
    def run(self, ftp_config_file, sql_file, date):
        return NotImplemented