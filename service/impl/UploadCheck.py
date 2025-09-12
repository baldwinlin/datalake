#coding:utf-8
'''
UploadCheckImpl.py
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

import logging
from service.UploadCheck import *
import json
from pathlib import Path
from datetime import datetime
from logger import Logger
from exception.dataLakeUtilsErrorHandler import dataLakeUtilsErrorHandler
from dao.impl.JdbcDaoImpl import JdbcDaoImpl
from crypto.Aes256Crypto import *
import jaydebeapi
import traceback

class UploadCheckImpl(UploadCheck):

    def __init__(self, main_config, fc_config, pc_config, args):
        self.fc_config = fc_config
        self.main_config = main_config
        self.args_str = args
        self.logger_main = None
        self.errorHandler = None
        try:
            self.args_dict = json.loads(self.args_str)
            self.batch_date = self.args_dict["BATCH_DATE"]
        except Exception as e:
            raise Exception(f"[讀取args錯誤] {e}")

        self.log_level = main_config["LOG"].get("LOG_LEVEL", "INFO").upper()

        try:
            temp_path = self.main_config.get('LOG', 'TEMP_PATH')
            self.temp_path = Path(temp_path) / "uc"
        except Exception as e:
            raise Exception(f"[讀取TEMP path錯誤] {e}")

        try:
            self.driver_path = self.main_config.get('DB_DRIVER', 'DRIVER_PATH')
        except Exception as e:
            raise Exception(f"[讀取DB dreiver path錯誤] {e}")

        try:
            self.host = self.fc_config.get('DB', 'HOST')
            self.port = self.fc_config.get('DB', 'PORT')
            self.db_sec_file = self.fc_config.get('DB', 'SEC_FILE')
            self.db_key_file = self.fc_config.get('DB', 'KEY_FILE')
            self.db_name = self.fc_config.get('DB', 'DB_NAME')
            self.driver = self.fc_config.get('DB', 'DRIVER')
            self.db_user, self.db_sec_str = readSecFile(self.db_sec_file)
            self.db_salt = readSaltFile(self.db_key_file)
            self.db_sec = get_gpg_decrypt(self.db_sec_str, self.db_salt)
        except Exception as e:
            raise Exception(f"[讀取DB config錯誤] {e}")

        self.log_prefix = pc_config.get('LOG', 'LOG_PREFIX', fallback='uploadcheck_')
        # create logger
        try:
            self.log_path = self.main_config.get('LOG', 'LOG_PATH')
            self.log_name = ""
        except Exception as e:
            raise Exception(f"[讀取LOG path錯誤] {e}")
        self.logger = self.createLog()

        #Get Source info
        try:
            self.src_db = pc_config.get('SOURCE', 'DATABASE')
            self.src_table = pc_config.get('SOURCE', 'TABLE')
            self.check_col = pc_config.get('SOURCE', 'CHECK_COL')
            self.src_ctl_table = pc_config.get('SOURCE', 'CTL_TABLE')
            self.src_ignoer_zero = pc_config.get('SOURCE', 'IGNORE_ZERO', fallback='N')
        except Exception as e:
            raise Exception(f"[讀取SOURCE config錯誤] {e}")

        # Get Target info
        try:
            self.tg_db = pc_config.get('TARGET', 'DATABASE')
            self.tg_table = pc_config.get('TARGET', 'TABLE')
            self.tg_ctl_table = pc_config.get('TARGET', 'CTL_TABLE')
        except Exception as e:
            raise Exception(f"[TARGET config錯誤] {e}")


    def createLog(self):
        log_path = Path(self.log_path)

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        #組合log name
        log_name = f"{self.log_prefix}_{timestamp}"

        # 建立 log 完整路徑
        log_dir = log_path / "uc"
        Logger.Logger(log_dir, log_name)  # 模組日誌
        return logging.getLogger(log_name)

    def errorExit(self, error_message, error_traceback = ''):
        self.logger.debug(error_traceback)
        self.logger.error(error_message)
        #self.errorHandler.exceptionWriter(error_message)
        exit(1)

    def connectDb(self):
        if (self.driver.lower() == 'hive2'):
            driver_class = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
            jdbc_url = f"jdbc:hive2://{self.host}:{self.port}/{self.db_name};auth=LDAP"
            driver_jar = f"{self.driver_path}/kyuubi-hive-jdbc-shaded-1.10.2.jar"
        elif (self.driver.lower() == 'mysql'):
            driver_class = "com.mysql.cj.jdbc.Driver"
            jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.db_name}"
            driver_jar = f"{self.driver_path}\\mysql-connector-j-9.4.0.jar"

        dao = JdbcDaoImpl(driver_class, jdbc_url, self.db_user, self.db_sec, driver_jar)

        try:
            dao.connect()
            self.logger.info('[資料庫連線完成]')
            return dao
        except Exception as e:
            self.errorExit(f'[資料庫連線失敗] {e}')

    def insertData(self, conn, source_table, target_table):
        """
        從來源表格的欄位名稱來組成 INSERT INTO ... SELECT ... 語法，並執行。

        :param conn: 已經建立好的 jaydebeapi 連線物件
        :param source_table: 來源表格名稱
        :param target_table: 目標表格名稱
        """
        cursor = None
        try:
            cursor = conn.cursor()

            # 1. 查詢來源表格，並取得欄位名稱
            # 這裡只取一筆資料來取得欄位資訊，不需要載入所有資料
            sql_get_columns = f"SELECT * FROM {source_table} LIMIT 1"
            cursor.execute(sql_get_columns)

            # 取得欄位名稱
            column_names = [desc[0] for desc in cursor.description]

            # 2. 動態產生 INSERT INTO ... SELECT ... 的 SQL 語法
            columns_str = ', '.join(column_names)
            sql_insert_select = f"INSERT INTO {target_table} ({columns_str}) SELECT {columns_str} FROM {source_table}"

            self.logger.info(f'[Insert SQL] {sql_insert_select}')

            # 3. 執行單一 SQL 語法
            cursor.execute(sql_insert_select)
            affected_rows = cursor.rowcount
            #self.logger.info(f'[Insert資料完成，寫入筆數: {affected_rows}]') #Hive無法回傳執行筆數
            self.logger.info(f'[Insert資料完成]')

        except Exception as e:
            self.errorExit(f"[Insert資料失敗] {e}")
        finally:
            if cursor:
                cursor.close()

    def compareTableSchema(self, conn, source_table, target_table):
        """
        比對來源表格與目標表格的 schema（欄位名稱與型態）是否一致。

        :param conn: 已經建立好的 jaydebeapi 連線物件
        :param source_table: 來源表格名稱
        :param target_table: 目標表格名稱
        :return: (bool, str) - True表示schema一致，False表示不一致，並回傳說明訊息
        """
        cursor = None
        try:
            cursor = conn.cursor()

            # 1. 取得來源表格的 schema
            sql_source = f"SELECT * FROM {source_table} LIMIT 1"
            cursor.execute(sql_source)
            source_schema = cursor.description

            # 2. 取得目標表格的 schema
            sql_target = f"SELECT * FROM {target_table} LIMIT 1"
            cursor.execute(sql_target)
            target_schema = cursor.description

            # 3. 比對欄位數量
            if len(source_schema) != len(target_schema):
                return False, f"欄位數量不一致。來源表格有 {len(source_schema)} 個欄位，目標表格有 {len(target_schema)} 個。"

            # 4. 比對每個欄位的名稱和型態
            for i in range(len(source_schema)):
                source_col = source_schema[i]
                target_col = target_schema[i]

                # 比較欄位名稱 (第0個元素)
                if source_col[0] != target_col[0]:
                    return False, f"欄位名稱不一致。來源表格的第 {i + 1} 個欄位是 '{source_col[0]}', 但目標表格是 '{target_col[0]}'."

                # 比較欄位型態 (第1個元素，代表資料型態的內部代碼)
                if source_col[1] != target_col[1]:
                    return False, f"欄位型態不一致。來源表格的 '{source_col[0]}' 欄位型態為 '{source_col[1]}', 但目標表格為 '{target_col[1]}'."

            return True, "欄位名稱與型態皆一致。"

        except jaydebeapi.DatabaseError as e:
            return False, f"資料庫錯誤: {e}"
        except Exception as e:
            return False, f"發生錯誤: {e}"
        finally:
            if cursor:
                cursor.close()

    def run(self):
        self.logger.setLevel(getattr(logging, self.log_level, logging.INFO))
        self.logger.info("[Run Upload Check]")
        #Connect DB
        try:
            dao = self.connectDb()
        except Exception as e:
            self.errorExit(f'[資料庫連線失敗] {e}')

        #Compare table schema
        src_table = f'{self.src_db}.{self.src_table}'
        tg_table = f'{self.tg_db}.{self.tg_table}'
        match, msg = self.compareTableSchema(dao.conn, src_table, tg_table)
        if not match:
            self.errorExit(f"[Table schema比對失敗] {msg}")
        else:
            self.logger.info("[Table schema比對完成]")

        #Read source control table
        check_cnt = 0
        sql = "select * from {}.{} where lower(table_name) = '{}' and {} = '{}' order by batch_time desc".format(
            self.src_db, self.src_ctl_table, self.src_table.lower(), self.check_col, self.batch_date )
        self.logger.debug(f"[SQL] {sql}")
        check_row = None
        try:
            cursor = dao.conn.cursor()
            cursor.execute(sql)
            rs = cursor.fetchmany(1)
            if rs:
                check_row = rs[0]
            else:
                self.errorExit(f"[找不到{self.batch_date}的檢核資料]")
            self.logger.info("[Control table] " + str(check_row))
            check_cnt = int(check_row[0])
        except Exception as e:
            error_traceback = traceback.format_exc()
            self.errorExit(f'[讀取control table失敗] {e}', error_traceback)

        finally:
            if cursor:
                cursor.close()

        if self.src_ignoer_zero.lower() == 'n' and check_cnt == 0 :
            self.errorExit(f"[{self.batch_date}的檢核筆數為零]")

        #Check count for source table
        total_cnt = 0
        sql = "select count(*) from {}.{} where {} = '{}'".format(self.src_db, self.src_table, self.check_col, self.batch_date)
        self.logger.debug(f"[SQL] {sql}")
        try:
            cursor = dao.conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchmany(1)
            total_cnt = rows[0][0]
            self.logger.info(f"[來源筆數: {total_cnt}]")
        except Exception as e:
            error_traceback = traceback.format_exc()
            self.errorExit(f'[檢查筆數失敗] {e}', error_traceback)
        finally:
            if cursor:
                cursor.close()

        if check_cnt != total_cnt:
            errmsg = f'[筆數不一致，預計: {check_cnt} 實際: {total_cnt} ]'
            self.errorExit(errmsg)

        #Insert data from source table to target table
        source_table = f'{self.src_db}.{self.src_table}'
        target_table = f'{self.tg_db}.{self.tg_table}'
        if total_cnt > 0:
            self.insertData(dao.conn, source_table, target_table)

        #Write target control table
        sql = "insert into {}.{} values(?, ?, ?, ?)".format(self.tg_db, self.tg_ctl_table)
        try:
            cursor = dao.conn.cursor()
            cursor.execute(sql, check_row)
        except Exception as e:
            self.errorExit(f'[寫入{self.tg_ctl_table}失敗] {e}')
        finally:
            if cursor:
                cursor.close()