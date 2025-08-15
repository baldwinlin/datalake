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

import configparser
import json

from crypto.Aes256Crypto import *
from service.SqlExecution import *
from dao.impl.SqlDaoImpl import SqlDaoImpl

class SqlExecutionImpl(SqlExecution):

    def __init__(self, config, args_str, sql_file):
        self.config = config
        self.args_str = args_str
        self.host = self.config['DB']['HOST']
        self.port = self.config['DB']['PORT']
        self.db_sec_file = self.config['DB']['SEC_FILE']
        self.db_sec_key_file = self.config['DB']['KEY_FILE']
        self.db_name = self.config['DB']['DB_NAME']
        self.driver = self.config['DB']['DRIVER']
        self.sql_file = sql_file
        self.user, self.sec_str = readSecFile(self.db_sec_file)
        self.salt = readSaltFile(self.db_sec_key_file)
        #self.db_sec = aes256Decrypt(self.sec_str, bytes.fromhex(self.salt))
        self.db_sec = get_gpg_decrypt(self.sec_str, self.salt)


    def run(self):
        print("SQL Execution running ...")

        try:
            with open(self.sql_file, "r") as file:
                sql_str = file.read()
        except Exception as e:  # Catching a more general exception for demonstration
            print(f"An error occurred during file writing: {e}")

        #Replace SQL arguments
        args_dict = json.loads(self.args_str)

        for key,value in args_dict.items():
            new_sql_str = sql_str.replace(key, value)

        print("SQL scripts: ", new_sql_str)

        sqlDao = SqlDaoImpl(self.host, self.port, self.user, self.db_sec, self.db_name, self.driver)
        sqlDao.executeSql(new_sql_str)


    '''
        @ftp_config_file/@sql_file/@date : from command argument
    '''
    def executeSql(self, ftp_config_file, sql_file, date):
        pass