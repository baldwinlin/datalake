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

from service.Housekeeping import *
from crypto.Aes256Crypto import *
from dao.impl.JdbcDaoImpl import JdbcDaoImpl
from dao.impl.S3DaoImpl import S3DaoImpl
from util.FilenameProcessor import FilenameProcessor


import json
from logger import Logger
from datetime import datetime, timedelta
import logging
import re

class HouseKeepingImpl(Housekeeping):
    def __init__(self, main_config, fc_config, pc_config, args):
        self.main_config = main_config
        self.fc_config = fc_config
        self.pc_config = pc_config
        self.args = json.loads(args)
        self.batch_date =str(self.args.get('batch_date'))
        
        #logger
        self.logger_main = None
        self.log_level = self.main_config["LOG"].get("LOG_LEVEL", "INFO").upper()
        self.logger_prefix = self.pc_config["LOG"].get("LOG_PREFIX")
        if not self.logger_prefix:
            raise Exception("LOG_PREFIX 不得為空值")

        #s3 config
        try:
            self.s3_host = self.fc_config.get('S3','HOST')
            self.s3_port = self.fc_config.get('S3','PORT')
            self.s3_assess_id_file = self.fc_config.get('S3','ASSESS_ID_FILE')
            self.s3_assess_key_file = self.fc_config.get('S3','ASSESS_KEY_FILE')
            self.s3_user, self.s3_sec_str = readSecFile(self.s3_assess_id_file)
            self.s3_salt = readSaltFile(self.s3_assess_key_file)
            self.s3_sec = get_gpg_decrypt(self.s3_sec_str, self.s3_salt)

        except Exception as e:
            raise Exception(f"讀取S3 config錯誤: {e}")
        
        try:
            self.driver_path = self.main_config.get('DB_DRIVER','PG_DRIVER_PATH')
        except Exception as e:
            raise Exception(f"[讀取DB dreiver path錯誤] {e}")
        
        #hive config
        try:
            self.hive_host = self.fc_config.get('HIVE', 'HOST')
            self.hive_port = self.fc_config.get('HIVE', 'PORT')
            self.hive_driver = self.fc_config.get('HIVE', 'DRIVER')
            self.hive_sec_file = self.fc_config.get('HIVE', 'SEC_FILE')
            self.hive_key_file = self.fc_config.get('HIVE', 'KEY_FILE')
            self.hive_user, self.hive_sec_str = readSecFile(self.hive_sec_file)
            self.hive_salt = readSaltFile(self.hive_key_file)
            self.hive_sec = get_gpg_decrypt(self.hive_sec_str, self.hive_salt)
            
            if self.hive_driver.lower() == 'postgresql':
                self.hive_user = "datalake"
                self.hive_sec = "123456"
            print(f"hive_sec: {self.hive_sec}")
            print(f"hive_user: {self.hive_user}")
            
        except Exception as e:
            raise Exception(f"[讀取Hive config錯誤] {e}")

        #cleanup config
        try:
            self.cleanup_type = self.pc_config.get('CLEANUP','TYPE')
            if self.cleanup_type.lower() == "s3":
                self.bucket = self.pc_config.get('CLEANUP','BUCKET')
                self.s3_path = self.pc_config.get('CLEANUP','S3_PATH')
                self.file_pattern = self.pc_config.get('CLEANUP','FILE_PATTERN')
                try:
                    self.s3Dao=S3DaoImpl(self.bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
                except Exception as e:
                    raise Exception(f"建立S3 DAO時發生錯誤: {e}")
            elif self.cleanup_type.lower() == "hive":
                self.hive_name = self.pc_config.get('CLEANUP','HIVE_DB')
                self.hive_table = self.pc_config.get('CLEANUP','HIVE_TABLE')
                self.hive_date_column = self.pc_config.get('CLEANUP','DATE_COLUMN')
                if self.hive_date_column is None:
                    raise Exception(f"DATE_COLUMN 不得為空值")
            else:
                raise Exception(f"CLEANUP type {self.cleanup_type} 未定義或不支援")

            self.retention_days = self.pc_config.get('CLEANUP','RETENTION_DAYS')

        except Exception as e:
            raise Exception(f"讀取CLEANUP config錯誤: {e}")
        

    def _initialize_logger(self):
        log_config = self.main_config
        housekeeping_log_path = f"{log_config['LOG']['LOG_PATH']}/housekeeping"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        logger_file_name = f"{self.logger_prefix}_{timestamp}"

        Logger.Logger(housekeeping_log_path, logger_file_name)
        self.logger_main = logging.getLogger(logger_file_name)

    def errorExit(self, error_message):
        self.logger_main.error(error_message)
        exit(1)  


    def run(self):
        self._initialize_logger()
        self.logger_main.info("Housekeeping 開始執行")
        self.logger_main.setLevel(getattr(logging, self.log_level, logging.INFO))

        if self.cleanup_type.lower() == "s3":
            try:
                self.CleanupS3()
            except Exception as e:
                self.errorExit(f"清理S3檔案失敗: {e}")
        elif self.cleanup_type.lower() == "hive":
            try:
                self.CleanupHive()
            except Exception as e:
                self.errorExit(f"清理Hive檔案失敗: {e}")
        else:
            raise Exception(f"CLEANUP type {self.cleanup_type} 未定義或不支援")

        self.logger_main.info("Housekeeping 執行完成")
        return True

    def CleanupS3(self):
        file_list = self.GetS3FileList()
        self.logger_main.info(f"S3檔案列表: {file_list}")
        need_keep, need_clean = self.OrganizeS3FileList(file_list)
        self.logger_main.info(f"需要保留的檔案列表: {need_keep}")
        self.logger_main.info(f"需要清理的檔案列表: {need_clean}")
        if len(need_clean) > 0:
            self.s3Dao.deleteFiles(need_clean)
        else:
            self.logger_main.info(f"S3 無需要清理的檔案")
            return True
        self.logger_main.info(f"清理S3檔案完成")
      
    def GetS3FileList(self):
        try:
            target_name_pattern = FilenameProcessor._process_name_pattern(self.file_pattern, self.batch_date)
            target_path = str(self.s3_path + "/" + target_name_pattern)
            self.logger_main.info(f"置換後的檔案名稱模式: {target_path}")
            file_list = self.s3Dao.listFiles(target_path)

        except Exception as e:
            raise Exception(f"取得S3檔案列表失敗: {e}")
        return file_list
    
    def OrganizeS3FileList(self, file_list):
        base_date = datetime.strptime(self.batch_date, "%Y%m%d").date()
        keeping_dates_list = []
        for i in range(int(self.retention_days)):
            keeping_dates_list.append((base_date - timedelta(days=i)).strftime("%Y%m%d"))
        self.logger_main.info(f"需要保留的日期列表: {keeping_dates_list}")
        keeping_set = set(keeping_dates_list)
        need_keep, need_clean = [], []
        
        for key in file_list:
            date = self._extract_date_from_key(key)
            if date is None:
                self.logger_main.warning(f"檔名無法解析日期，跳過: {key}")
                continue
            elif date in keeping_set:
                need_keep.append(key)
            elif date > self.batch_date:
                need_keep.append(key)
            else:
                need_clean.append(key)

        return need_keep, need_clean

    def _extract_date_from_key(self, key: str) -> str | None:
        DATE_RE = re.compile(r'(?P<date>\d{8})(?:_C)?(?:\.[A-Za-z0-9]+)?$')
        name = key.split("/")[-1]
        m = DATE_RE.search(name)
        if not m:
            return None
        return m.group("date")

    def CleanupHive(self):
        self.logger_main.info(f"清理Hive分區")
        hive_dao = self.ConnectHiveDb()
        self.DeletePartitions(hive_dao)
        self.logger_main.info(f"清理Hive分區完成")
        # all_partitions = self.ShowHiveAllPartitions(hive_dao)
        # self.logger_main.info(f"Hive所有分區: {all_partitions}")

        # need_keep, need_clean = self.OrganizePartitionsToDates(all_partitions)
        # self.logger_main.info(f"需要保留的分區: {need_keep}")
        # self.logger_main.info(f"需要清理的分區: {need_clean}")
        
        # if len(need_clean) > 0:
        #     self.DeletePartitions(need_clean, hive_dao)
        # else:
        #     self.logger_main.info(f"Hive 無需要清理的分區")
        #     return True
        # self.logger_main.info(f"清理Hive分區完成")

        # if self.hive_driver.lower() == 'postgresql':
        #     all_partitions = self.ShowHiveAllPartitions(hive_dao)
        #     self.logger_main.info(f"清理完後的Hive所有分區: {all_partitions}")

    
    def ConnectHiveDb(self):
        if (self.hive_driver.lower() == 'hive2'):
            driver_class = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
            jdbc_url = f"jdbc:hive2://{self.hive_host}:{self.hive_port}/{self.hive_name};auth=LDAP"
            driver_jar = f"{self.driver_path}/kyuubi-hive-jdbc-shaded-1.10.2.jar"
        elif (self.hive_driver.lower() == 'mysql'):
            driver_class = "com.mysql.cj.jdbc.Driver"
            jdbc_url = f"jdbc:mysql://{self.hive_host}:{self.hive_port}/{self.hive_name}"
            driver_jar = f"{self.driver_path}\\mysql-connector-j-9.4.0.jar"
        elif (self.hive_driver.lower() == 'postgresql'):
            driver_class = "org.postgresql.Driver"
            jdbc_url = f"jdbc:postgresql://{self.hive_host}:{self.hive_port}/{self.hive_name}"
            driver_jar = f"{self.driver_path}/postgresql-42.7.7.jar"

        hive_dao = JdbcDaoImpl(driver_class, jdbc_url, self.hive_user, self.hive_sec, driver_jar)

        try:
            hive_dao.connect()
            self.logger_main.info('[資料庫連線完成]')
            return hive_dao
        except Exception as e:
            self.errorExit(f'[資料庫連線失敗] {e}')

    def DeletePartitions(self, hive_dao):
        if self.hive_driver.lower() == 'hive2':
            date_column = self.hive_date_column
            base_date = datetime.strptime(self.batch_date, "%Y%m%d").date()
            cutoff_date = base_date - timedelta(days=int(self.retention_days))
            cutoff_str = cutoff_date.strftime("%Y%m%d")
            partition = [f"PARTITION ({date_column} <= '{cutoff_str}') PURGE;"]
            sql =  f"ALTER TABLE {self.hive_name}.{self.hive_table} DROP IF EXISTS " + ", ".join(partition)
            
            self.logger_main.info(f"[Hive] 預計執行的 SQL: {sql}")
            # hive_dao.executeSql(sql)
  
        # elif self.hive_driver.lower() == 'postgresql':
        #     for date in need_clean:
        #         self.logger_main.info(f"清理Hive分區: {self.hive_table}_{date}")
        #         sql = f"DROP TABLE IF EXISTS {self.hive_table}_{date}"
        #         hive_dao.executeSql(sql)
    

    def ShowHiveAllPartitions(self, hive_dao):
        if self.hive_driver.lower() == 'postgresql':
            sql = f"""
                SELECT
                c.relname                              AS child,               -- 子分區表名 (e.g. test_20250915)
                pg_get_expr(c.relpartbound, c.oid)     AS bound                -- 分區邊界 (e.g. FOR VALUES FROM (...) TO (...))
                FROM pg_class c
                JOIN pg_inherits i   ON i.inhrelid = c.oid
                JOIN pg_class p      ON p.oid      = i.inhparent
                JOIN pg_namespace n  ON n.oid      = p.relnamespace
                WHERE p.relname = '{self.hive_table}'
                AND n.nspname = ANY(current_schemas(true))   -- 走當前 search_path（你已把 datalake 的 search_path 設 public）
                ORDER BY child;
                """
            rows = hive_dao.executeQuery(sql)
            return rows
    
    def OrganizePartitionsToDates(self, rows):
        base_date = datetime.strptime(self.batch_date, "%Y%m%d").date()
        keeping_dates_list = []
        for i in range(int(self.retention_days)):
            keeping_dates_list.append((base_date - timedelta(days=i)).strftime("%Y%m%d"))
        self.logger_main.info(f"需要保留的日期列表: {keeping_dates_list}")
        keeping_set = set(keeping_dates_list)
        need_keep, need_clean = [], []
        for row in rows:
            date = self._pg_partitions_to_dates(row)
            if date in keeping_set:
                need_keep.append(date)
            elif date > self.batch_date:
                need_keep.append(date)
            else:
                need_clean.append(date)
        return need_keep, need_clean

    def _pg_partitions_to_dates(self, row):
        """
        rows 例：[
        ('test_20250915', "FOR VALUES FROM ('2025-09-15') TO ('2025-09-16')"),
        ...
        ]
        回傳：['20250915', '20250916', '20250917']
        """
        BOUND_RE = re.compile(r"FROM\s*\('(\d{4}-\d{2}-\d{2})'\)\s*TO\s*\('(\d{4}-\d{2}-\d{2})'\)", re.IGNORECASE)
        
        m = BOUND_RE.search(row[1])
        if not m:
            return None
        start = datetime.strptime(m.group(1), "%Y-%m-%d").strftime("%Y%m%d")
        return start
