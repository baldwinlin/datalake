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
from dao.impl.JdbcDaoImpl import JdbcDaoImpl
import logging
from logger import Logger
from exception.dataLakeUtilsErrorHandler import dataLakeUtilsErrorHandler

class SqlExecutionImpl(SqlExecution):

    def __init__(self, config, args_str, sql_file):
        self.config = config
        self.args_str = args_str

        try:
            self.host = self.config.get('DB','HOST')
            self.port = self.config.get('DB','PORT')
            self.db_sec_file = self.config.get('DB','SEC_FILE')
            self.db_sec_key_file = self.config.get('DB','KEY_FILE')
            self.db_name = self.config.get('DB','DB_NAME')
            self.driver = self.config.get('DB','DRIVER')
        except Exception as e:
            raise Exception(f"讀取DB config錯誤: {e}")


        self.sql_file = sql_file
        self.user, self.sec_str = readSecFile(self.db_sec_file)
        self.salt = readSaltFile(self.db_sec_key_file)
        #self.db_sec = aes256Decrypt(self.sec_str, bytes.fromhex(self.salt))
        self.db_sec = get_gpg_decrypt(self.sec_str, self.salt)
        self.logger_main = None
        self.errorHandler = None

    def setLog(self, logger_main, errorHandler):
        self.logger_main = logger_main
        self.errorHandler = errorHandler


    def run(self):
        self.logger_main.info("Run SQL Exception ...")

        #Read SQL file to SQL string
        try:
            with open(self.sql_file, "r") as file:
                sql_str = file.read()
        except Exception as e:  # Catching a more general exception for demonstration
            self.logger_main.error(f"讀取SQL file錯誤: {e}")
            self.errorHandler.exceptionWriter(f"[連線資料庫錯誤] {e}")
            exit(1)


        #Replace SQL arguments
        args_dict = json.loads(self.args_str)

        for key,value in args_dict.items():
            new_sql_str = sql_str.replace(key, value)

        #print("SQL scripts: ", new_sql_str)
        self.logger_main.info(f'SQL scripts: \n{new_sql_str}')

        # sqlDao = SqlDaoImpl(self.host, self.port, self.user, self.db_sec, self.db_name, self.driver)
        # sqlDao.executeSql(new_sql_str)

        #Connect DB and execute SQL
        if(self.driver == 'hive2'):
            driver_class = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
            jdbc_url = f"jdbc:hive2://{self.host}:{self.port}/{self.db_name};auth=LDAP"
            driver_jar = "./kyuubi-hive-jdbc-shaded-1.10.2.jar"
        elif(self.driver == 'mysql'):
            driver_class = "com.mysql.cj.jdbc.Driver"
            jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.db_name}"
            driver_jar = "C:\\Users\\Baldwin\\PycharmProjects\\mysql-connector-j-9.4.0.jar"

        dao = JdbcDaoImpl(driver_class, jdbc_url, self.user, self.db_sec, driver_jar)

        try:
            dao.connect()
            self.logger_main.info('資料庫連線完成')
        except Exception as e:
            self.logger_main.error('資料庫連線失敗')
            self.errorHandler.exceptionWriter(f"[連線資料庫錯誤] {e}")
            exit(1)

        try:
            dao.executeSql(new_sql_str)
            self.logger_main.info('執行SQL完成')
        except Exception as e:
            self.logger_main.error('執行SQL失敗')
            self.errorHandler.exceptionWriter(f"[執行SQL錯誤] {e}")
            exit(1)

        return True