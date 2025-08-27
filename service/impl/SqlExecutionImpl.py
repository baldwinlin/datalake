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
from pathlib import Path
from datetime import datetime
import subprocess


class SqlExecutionImpl(SqlExecution):

    def __init__(self, main_config, config, args_str, sql_file):
        self.config = config
        self.main_config = main_config
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

        try:
            self.driver_path = self.main_config.get('DB_DRIVER','DRIVER_PATH')
        except Exception as e:
            raise Exception(f"讀取DB dreiver path錯誤: {e}")

        try:
            self.log_path = self.main_config.get('LOG','LOG_PATH')
            self.log_name = ""
        except Exception as e:
            raise Exception(f"讀取LOG path錯誤: {e}")

        try:
            self.temp_path = self.main_config.get('LOG','TEMP_PATH')
        except Exception as e:
            raise Exception(f"讀取TEMP path錯誤: {e}")

        try:
            self.beeline_path = self.main_config.get('DB_DRIVER','BEELINE_PATH')
        except Exception as e:
            raise Exception(f"讀取beeline path錯誤: {e}")


        self.sql_file = sql_file
        self.user, self.sec_str = readSecFile(self.db_sec_file)
        self.salt = readSaltFile(self.db_sec_key_file)
        #self.db_sec = aes256Decrypt(self.sec_str, bytes.fromhex(self.salt))
        self.db_sec = get_gpg_decrypt(self.sec_str, self.salt)
        self.logger_main = None
        self.errorHandler = None
        self.logger_sql = None
        self.sql_file_name = ""

    def setLog(self, logger_main, errorHandler):
        self.logger_main = logger_main
        self.errorHandler = errorHandler

    def run_linux_command(self, command):
        """
        執行 Linux command (可多行)，回傳 (status, stdout, stderr)
        """
        process = subprocess.run(
            command,
            shell=True,
            text=True,
            capture_output=True
        )
        return process.returncode, process.stdout.strip(), process.stderr.strip()

    def getLogFilePath(self):
        log_path = Path(self.log_path)
        sql_file = Path(self.sql_file)

        # 取得 sql 檔名（不含副檔名）
        filename = sql_file.stem  # create_01
        self.sql_file_name = sql_file.name

        # 日期
        today = datetime.today().strftime("%Y%m%d")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # 取得 sql 檔所在的子目錄名稱 (ddl)
        subdir = sql_file.parent.name

        # 建立 log 完整路徑
        log_dir = log_path / "sql" / subdir
        log_name = f"{filename}_{timestamp}"

        # 確保目錄存在
        #log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir, log_name

    def executeBeeline(self, sql_temp_file):
        cmd = f"cd {self.beeline_path}\n"
        cmd += f"./beeline -u \"jdbc:hive2://{self.host}:{self.port}/default;auth=LDAP\" -n '{self.user}' -p '{self.db_sec}' -f {sql_temp_file}"
        # print("cmd = ", cmd)
        process = subprocess.run(
            cmd,
            shell=True,  # 允許 shell 指令
            text=True,  # 以字串回傳，而不是 bytes
            capture_output=True  # 抓 stdout/stderr
        )
        self.logger_sql.info(f"[執行SQL結果] {process.returncode}")
        # print("Return code:", process.returncode)
        # print("STDOUT:\n", process.stdout)
        # print("STDERR:\n", process.stderr)
        if (process.returncode):
            self.logger_sql.error(f"[執行SQL錯誤] {process.stderr}")
            exit(1)
        else:
            self.logger_sql.info(f"[執行SQL完成]")
        return True

    def run(self):

        #Create module log
        log_dir,log_name = self.getLogFilePath()
        Logger.Logger(log_dir, log_name)  # 模組日誌
        self.logger_sql = logging.getLogger(log_name)

        self.logger_sql.info("[Run SQL Execution]")

        # Read SQL file to SQL string
        self.logger_sql.info(f"SQL file = {self.sql_file}")
        self.logger_sql.info(f"SQL args = {self.args_str}")
        try:
            with open(self.sql_file, "r") as file:
                sql_str = file.read()
        except Exception as e:  # Catching a more general exception for demonstration
            self.logger_sql.error(f"[讀取SQL file錯誤] {e}")
            self.errorHandler.exceptionWriter(f"[讀取SQL file錯誤] {e}")
            exit(1)


        #Replace SQL arguments
        if(self.args_str is not None):
            args_dict = json.loads(self.args_str)

            for key,value in args_dict.items():
                sql_str = sql_str.replace(key, value)

        sql_temp_file = f"{self.temp_path}{self.sql_file_name}"
        with open(sql_temp_file, "w", encoding="UTF-8") as f:
            f.write(sql_str)
        #print("SQL scripts: ", sql_str)
        self.logger_sql.info(f'Execute SQL file: {sql_temp_file}')

        if (self.driver.lower() == 'hive2'):
            return self.executeBeeline(sql_temp_file)

        elif(self.driver.lower() == 'mysql'):
            driver_class = "com.mysql.cj.jdbc.Driver"
            jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.db_name}"
            driver_jar = f"{self.driver_path}\\mysql-connector-j-9.4.0.jar"

            dao = JdbcDaoImpl(driver_class, jdbc_url, self.user, self.db_sec, driver_jar)

            try:
                dao.connect()
                self.logger_sql.info('資料庫連線完成')
            except Exception as e:
                self.logger_sql.error(f'資料庫連線失敗 {e}')
                self.errorHandler.exceptionWriter(f"[連線資料庫錯誤] {e}")
                exit(1)

            try:
                dao.executeSql(sql_str)
                self.logger_sql.info('執行SQL完成')
            except Exception as e:
                self.logger_sql.error(f'執行SQL錯誤 {e}')
                self.errorHandler.exceptionWriter(f"[執行SQL錯誤] {e}")
                exit(1)

            return True


'''       

        #Connect DB and execute SQL
        if(self.driver == 'hive2'):
            driver_class = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
            jdbc_url = f"jdbc:hive2://{self.host}:{self.port}/{self.db_name};auth=LDAP"
            driver_jar = f"{self.driver_path}/kyuubi-hive-jdbc-shaded-1.10.2.jar"
        elif(self.driver == 'mysql'):
            driver_class = "com.mysql.cj.jdbc.Driver"
            jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.db_name}"
            driver_jar = f"{self.driver_path}\\mysql-connector-j-9.4.0.jar"

        dao = JdbcDaoImpl(driver_class, jdbc_url, self.user, self.db_sec, driver_jar)

        try:
            dao.connect()
            self.logger_sql.info('資料庫連線完成')
        except Exception as e:
            self.logger_sql.error(f'資料庫連線失敗 {e}')
            self.errorHandler.exceptionWriter(f"[連線資料庫錯誤] {e}")
            exit(1)

        try:
            dao.executeSql(sql_str)
            self.logger_sql.info('執行SQL完成')
        except Exception as e:
            self.logger_sql.error(f'執行SQL錯誤 {e}')
            self.errorHandler.exceptionWriter(f"[執行SQL錯誤] {e}")
            exit(1)

        return True
        
'''