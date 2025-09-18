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
import time
from zoneinfo import ZoneInfo

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
            self.driver_path = self.main_config.get('DB_DRIVER','DRIVER_PATH')
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
            
        except Exception as e:
            raise Exception(f"[讀取Hive config錯誤] {e}")

        #cleanup config
        try:
            self.cleanup_type = self.pc_config.get('CLEANUP','TYPE')
            print(self.cleanup_type)
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
                self.hive_date_column = self.pc_config.get('CLEANUP','DATE_COLUMN').strip()
                if not self.hive_date_column:
                    raise Exception(f"DATE_COLUMN 不得為空值")
                self.hive_date_format = self.pc_config.get('CLEANUP','DATE_FORMAT').strip()
                if not self.hive_date_format:
                    raise Exception(f"DATE_FORMAT 不得為空值")
                elif self.hive_date_format not in ["%Y%m%d", "%Y-%m-%d"]:
                    raise Exception(f"DATE_FORMAT 只接受 %Y%m%d 或 %Y-%m-%d 設定值")
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

        self.logger_main.info("Housekeeping 執行完成")
        return True

    def CleanupS3(self):
        file_list, file_date_list = self.GetS3FileList()
        if not file_list:
            self.logger_main.info(f"S3 查找無符合檔案名稱模式的檔案")
        else:
            self.logger_main.info(f"指定的路徑 {self.s3_path} 下的S3檔案列表: {file_list}")
            need_keep, need_clean = self.OrganizeS3FileList(file_date_list)
            self.logger_main.info(f"需要保留的檔案列表: {need_keep}")
            self.logger_main.info(f"需要清理的檔案列表: {need_clean}")
            if len(need_clean) > 0:
                result = self.s3Dao.deleteFiles(need_clean)
                if result:
                    self.logger_main.info(f"清理S3檔案完成")
                    self.logger_main.info(f"清理 S3 檔案後進行的檢查")
                    self.logger_main.info(f"等待 20 秒後進行檢查")
                    time.sleep(20)
                    self.CheckS3FileList()
                else:
                    self.errorExit(f"清理 S3 檔案失敗")
            else:
                self.logger_main.info(f"S3 無需要清理的檔案")
      
    def GetS3FileList(self):
        try:
            target_path = str(self.s3_path + "/" + self.file_pattern)
            self.logger_main.info(f"欲查詢的檔案路徑與名稱模式: {target_path}")
            file_objs_list = self.s3Dao.listFilesWithDate(target_path)
            file_list = [file_obj[0] for file_obj in file_objs_list]
            file_date_list = [(file_obj[0], file_obj[1]) for file_obj in file_objs_list]

        except Exception as e:
            raise Exception(f"取得S3檔案列表失敗: {e}")
        return file_list, file_date_list
    
    def OrganizeS3FileList(self, file_date_list):
        base_date = datetime.strptime(self.batch_date, "%Y%m%d").date()
        keeping_dates_list = []
        str_keeping_dates_list = []
        for i in range(int(self.retention_days)):
            keeping_dates_list.append((base_date - timedelta(days=i)))
            str_keeping_dates_list.append((base_date - timedelta(days=i)).strftime("%Y%m%d"))
        self.logger_main.info(f"需要保留的日期列表: {str_keeping_dates_list}")
        keeping_set = set(keeping_dates_list)
        need_keep, need_clean = [], []
        
        for key, date in file_date_list:
            if date is None:
                self.logger_main.warning(f"檔名無法解析日期，跳過: {key}")
                continue
            elif date in keeping_set:
                self.logger_main.debug(f"需要保留的檔案，日期在保留天數內: {key}, {date}")
                need_keep.append(key)
            elif date > base_date:
                self.logger_main.debug(f"需要保留的檔案，日期在未來: {key}, {date}")
                need_keep.append(key)
            else:
                self.logger_main.debug(f"需要清理的檔案，日期在過去: {key}, {date}")
                need_clean.append(key)
        return need_keep, need_clean

    def CheckS3FileList(self):
        base_date = datetime.strptime(self.batch_date, "%Y%m%d").date()
        out_of_date = (base_date - timedelta(days=int(self.retention_days)))

        keeping_file_list, keeping_file_date_list = self.GetS3FileList()
        if keeping_file_list and keeping_file_date_list:
            self.logger_main.info(f"清理 S3 檔案後的檔案列表: {keeping_file_list}")
            not_clear_files = []
            for file, date in keeping_file_date_list:
                if date <= out_of_date:
                    self.logger_main.debug(f"檢查出不需要保留的檔案，日期不在保留天數內，也不是未來日期: {file}, {date}")
                    not_clear_files.append(file)
            if not_clear_files:
                self.errorExit(f"清理失敗，仍然存在檔案: {not_clear_files}")
            else:
                self.logger_main.info(f"清理完後的S3檔案與需要清理的檔案無重疊")
        else:
            self.logger_main.info(f"清理完後的S3檔案為空")
        self.logger_main.info(f"檢查 S3 檔案完成")

    def CleanupHive(self):
        self.logger_main.info(f"清理Hive分區")
        hive_dao = self.ConnectHiveDb()
        all_partitions = self.ShowHiveAllPartitions(hive_dao)
        if not all_partitions:
            self.logger_main.info(f"指定的 Hive 資料庫 {self.hive_name} Table {self.hive_table} 無任何分區資料")
        else:
            self.logger_main.info(f"Hive所有分區: {all_partitions}")
            need_keep, need_clean = self.OrganizePartitionsToDates(all_partitions)
            self.logger_main.info(f"需要保留的分區: {need_keep}")
            self.logger_main.info(f"需要清理的分區: {need_clean}")

            if len(need_clean) > 0:
                result = self.DeletePartitions(need_clean, hive_dao)
                if result:
                    self.logger_main.info(f"清理Hive分區完成")
                    self.logger_main.info(f"等待 20 秒後進行檢查")
                    time.sleep(20)
                    self.CheckHivePartitions(hive_dao)
                else:
                    keep_partitions = self.ShowHiveAllPartitions(hive_dao)
                    self.errorExit(f"清理失敗，Hive現在的分區: {keep_partitions}")
            else:
                self.logger_main.info(f"Hive 無需要清理的分區")
                return True
 
    def ConnectHiveDb(self):
        if (self.hive_driver.lower() == 'hive2'):
            driver_class = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
            jdbc_url = f"jdbc:hive2://{self.hive_host}:{self.hive_port}/{self.hive_name};auth=LDAP"
            driver_jar = f"{self.driver_path}/kyuubi-hive-jdbc-shaded-1.10.2.jar"

        hive_dao = JdbcDaoImpl(driver_class, jdbc_url, self.hive_user, self.hive_sec, driver_jar)

        try:
            hive_dao.connect()
            self.logger_main.info('[資料庫連線完成]')
            return hive_dao
        except Exception as e:
            self.errorExit(f'[資料庫連線失敗] {e}')
    
    def ShowHiveAllPartitions(self, hive_dao):
        if self.hive_driver.lower() == 'hive2':
            sql = f"SHOW PARTITIONS {self.hive_name}.{self.hive_table}"
            all_partitions = hive_dao.executeQuery(sql)
            return all_partitions

    def OrganizePartitionsToDates(self, partitions):
        base_date = datetime.strptime(self.batch_date, self.hive_date_format).date()
        keeping_dates_list = []
        str_keeping_dates_list = []
        for i in range(int(self.retention_days)):
            keeping_dates_list.append((base_date - timedelta(days=i)))
        self.logger_main.info(f"需要保留的日期列表: {str_keeping_dates_list}")
        keeping_set = set(keeping_dates_list)
        need_keep, need_clean = [], []
        
        for partition in partitions:
            partition_str = partition[0]
            parsed_partition_dict = dict(seg.split("=", 1) for seg in partition_str.split("/"))
            
            if self.hive_date_column not in parsed_partition_dict:
                self.errorExit(f"分區欄位缺乏 {self.hive_date_column} 欄位")
            
            str_date = parsed_partition_dict[self.hive_date_column]  
            try:
                parsed_date = datetime.strptime(str_date, self.hive_date_format).date()
            except ValueError:
                    self.errorExit(f"分區日期格式與指定格式 {self.hive_date_format} 不符")
            
            if parsed_date in keeping_set or parsed_date > self.batch_date:
                need_keep.append(partition)
            else:
                need_clean.append(partition)
        return need_keep, need_clean

    def DeletePartitions(self, need_clean_partitions, hive_dao):
        if self.hive_driver.lower() == 'hive2':
            date_column = self.hive_date_column
            batch_size = 2
            for i in range(0, len(need_clean_partitions), batch_size):
                partitions_chunk = need_clean_partitions[i:i+batch_size]
                self.logger_main.info(f"批次：{i}，需要清理的分區: {partitions_chunk}")
                
                partition_specs = []
                for partition_str in partitions_chunk:
                    parsed_partition_dict = dict(seg.split("=", 1) for seg in partition_str.split("/"))

                    if self.hive_date_column not in parsed_partition_dict:
                        self.errorExit(f"分區欄位缺乏 {self.hive_date_column} 欄位")
                    
                    inside_partition = ", ".join(f"{k}='{v}'" for k, v in parsed_partition_dict.items())
                    partition_specs.append(f"PARTITION ({inside_partition})")

                sql = f"ALTER TABLE {self.hive_name}.{self.hive_table} DROP IF EXISTS " + ", ".join(partition_specs) + " PURGE"
                self.logger_main.info(f"[Hive]批次：{i}，預計執行的 SQL: {sql}")
                hive_dao.executeSql(sql)
            return True
    
    def CheckHivePartitions(self, hive_dao):
        base_date = datetime.strptime(self.batch_date, self.hive_date_format).date()
        out_of_date = (base_date - timedelta(days=int(self.retention_days)))
        
        keep_partitions = self.ShowHiveAllPartitions(hive_dao)
        if keep_partitions:
            self.logger_main.info(f"清理完後的Hive所有分區: {keep_partitions}")
            not_clear_partitions = []
            for partition in keep_partitions:
                partition_str = partition[0]
                parsed_partition_dict = dict(seg.split("=", 1) for seg in partition_str.split("/"))

                if self.hive_date_column not in parsed_partition_dict:
                    self.errorExit(f"分區欄位缺乏 {self.hive_date_column} 欄位")
                str_date = parsed_partition_dict[self.hive_date_column]  
                try:
                    parsed_date = datetime.strptime(str_date, self.hive_date_format).date()
                except ValueError:
                        self.errorExit(f"分區日期格式與指定格式 {self.hive_date_format} 不符")
                if parsed_date <= out_of_date:
                    not_clear_partitions.append(partition)
            
            if not_clear_partitions:
                self.errorExit(f"清理失敗，仍然存在分區: {not_clear_partitions}")
            else:
                self.logger_main.info(f"清理完後的Hive所有分區與需要清理的分區無重疊")
        else:
            self.logger_main.info(f"清理完後的Hive所有分區為空")
        self.logger_main.info(f"檢查 Hive 分區完成")