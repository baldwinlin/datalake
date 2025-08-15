#coding:utf-8
'''
FtpLoader.py
Object          : 執行SQL
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1. 指定SQL file
                  2. 帶入日期參數至SQL file的變數
                  3. 指定SQL執行環境(HIVE/DDAE)
Parameters      : 1. SQL file
                  2. Date(YYYYMMDD)
                  3. Hive config file(IP/Port/DB name)
Output          :
********************************************************************************
Modify          :
'''

import abc

class SqlExecution(abc.ABC):

    '''
        @ftp_config_file/@sql_file/@date : from command argument
    '''
    @abc.abstractmethod
    def executeSql(self, ftp_config_file, sql_file, date):
        return NotImplemented