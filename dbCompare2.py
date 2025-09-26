#coding:utf-8
'''
dbSchemaCompare.py
Object          : Compare schemas and data between two database
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           :
Parameters      :
Output          :
********************************************************************************
'''

import configparser
import argparse
import os
from crypto.Aes256Crypto import *
import jaydebeapi
import json
import logging
from typing import Tuple, Dict, List
from logger import Logger
import datetime
import time

class DbCompare():
    def __init__(self, db_config, main_config, fc_args):
        self.db_config = db_config
        self.main_config = main_config
        # 嚴格檢查 fc_args 是否為空字串
        if isinstance(fc_args, str) and fc_args.strip():
            try:
                params = json.loads(fc_args)
            except Exception:
                raise Exception("fc_args 格式不正確")
        else:
            raise Exception("fc_args 不得為空字串，請檢查參數，至少包含src_db_name, tg_db_name, compare_date")
        self.args = params
        
        
        try:
            self.driver_path = self.db_config.get('DB_DRIVER', 'DRIVER_PATH')
        except Exception as e:
            raise Exception(f"讀取DB driver path錯誤: {e}")

        #Read source db config
        try:    
            self.src_host = self.db_config.get('SOURCE', 'HOST')
            self.src_port = self.db_config.get('SOURCE', 'PORT')
            self.src_db_sec_file = self.db_config.get('SOURCE', 'SEC_FILE')
            self.src_db_key_file = self.db_config.get('SOURCE', 'KEY_FILE')
            self.src_driver = self.db_config.get('SOURCE', 'DRIVER')
            self.src_db_user, self.src_db_sec_str = readSecFile(self.src_db_sec_file)
            self.src_db_salt = readSaltFile(self.src_db_key_file)
            self.src_db_sec = get_gpg_decrypt(self.src_db_sec_str, self.src_db_salt)
        except Exception as e:
            raise Exception(f"讀取source db config錯誤: {e}")

        # Read target db config
        try:
            self.tg_host = self.db_config.get('TARGET', 'HOST')
            self.tg_port = self.db_config.get('TARGET', 'PORT')
            self.tg_db_sec_file = self.db_config.get('TARGET', 'SEC_FILE')
            self.tg_db_key_file = self.db_config.get('TARGET', 'KEY_FILE')
            self.tg_driver = self.db_config.get('TARGET', 'DRIVER')
            self.tg_db_user, self.tg_db_sec_str = readSecFile(self.tg_db_sec_file)
            self.tg_db_salt = readSaltFile(self.tg_db_key_file)
            self.tg_db_sec = get_gpg_decrypt(self.tg_db_sec_str, self.tg_db_salt)
        except Exception as e:
            raise Exception(f"讀取target db config錯誤: {e}")


        try:
            # 嚴格檢查參數
            self.compare_date = str(self.args.get('compare_date', '')).strip() or None
            self.src_db_name = str(self.args.get('src_db_name', '')).strip() or None
            self.tg_db_name  = str(self.args.get('tg_db_name', '')).strip() or None
            self.compare_table = (self.args.get('compare_table') or '').strip() or None
            self.sample_cnt   = int(self.args.get('sample_cnt', 5)) or None
            self.partition_date  = str(self.args.get('partition_date') or '').strip() or None
            self.partition_field = str(self.args.get('partition_field') or '').strip() or None
            self.retry_sleep_time = int(self.args.get('retry_sleep_time', 30))

            if self.partition_date and not self.partition_field:
                raise Exception("partition_date 存在，partition_field 不得為空，請檢查參數，至少包含partition_date, partition_field")
            if self.partition_field and not self.partition_date:
                raise Exception("partition_field 存在，partition_date 不得為空，請檢查參數，至少包含partition_date, partition_field")
            if not self.compare_date or not self.src_db_name or not self.tg_db_name:
                raise Exception("compare_date 不得為空，請檢查參數，至少包含src_db_name, tg_db_name, compare_date")
        except Exception as e:
            raise Exception(f"讀取檢核參數出現錯誤: {e}")

        try:
            self.src_conn = self.connectDb(self.src_driver, self.src_host, self.src_port, self.src_db_name,
                                        self.src_db_user, self.src_db_sec)
        except Exception as e:
            raise Exception(f"連線source db錯誤: {e}")
        try:
            self.tg_conn = self.connectDb(self.tg_driver, self.tg_host, self.tg_port, self.tg_db_name,
                                        self.tg_db_user, self.tg_db_sec)
        except Exception as e:
            raise Exception(f"連線target db錯誤: {e}")

        self.logger_main = None
        self.log_level = self.main_config["LOG"].get("LOG_LEVEL", "INFO").upper()

    def _initialize_logger(self, fun):
        log_config = self.main_config
        compare_log_path = f"{log_config['LOG']['LOG_PATH']}/compare"
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        logger_file_name = f"Compare_DB_{fun}_{timestamp}"
        Logger.Logger(compare_log_path, logger_file_name)
        self.logger_main = logging.getLogger(logger_file_name)

    def errorExit(self, error_message):
        self.logger_main.error(error_message)
        exit(1)  

    def connectDb(self, driver, host, port, db_name, user, sec):
        if (driver == 'hive2'):
            driver_class = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
            jdbc_url = f"jdbc:hive2://{host}:{port}/{db_name};auth=LDAP"
            driver_jar = f"{self.driver_path}/kyuubi-hive-jdbc-shaded-1.10.2.jar"

        conn = jaydebeapi.connect(driver_class, jdbc_url, [user, sec], driver_jar)
        return conn

    def compareTableCount(self, table_name):
        result = {"count": "NA"}
        # try:

        partition_clause = (f"PARTITION ({self.partition_field} = '{self.partition_date}')") if self.partition_date and self.partition_field else ""
        sql = f"SELECT count(*) FROM {table_name} {partition_clause}" 
        with self.src_conn.cursor() as src_cursor, self.tg_conn.cursor() as tg_cursor:
            src_cursor.execute(sql)
            rs = src_cursor.fetchone()
            src_cnt = rs[0] if rs else 0
            tg_cursor.execute(sql)
            rs = tg_cursor.fetchone()
            tg_cnt = rs[0] if rs else 0
        if src_cnt != tg_cnt:
            self.logger_main.error(f"[Count] 筆數不一致: 來源資料筆數: {src_cnt} - 目標資料筆數: {tg_cnt}")
            result["count"] = "N"
            return False, result
        else:
            self.logger_main.info(f"[Count] 筆數一致: 來源資料筆數: {src_cnt} - 目標資料筆數: {tg_cnt}")
            result["count"] = "Y"
            return True, result
        
        # except jaydebeapi.ProgrammingError as e:
        #     self.logger_main.warning(f"[Count] 資料庫錯誤，請檢查 Table 是否存在: {e}")
        #     result["count"] = "X"
        #     return False, result
        # except jaydebeapi.OperationalError as e:
        #     self.logger_main.warning(f"[Count] 資料庫錯誤，資料庫負載過重: {e}")
        #     result["count"] = "E"
        #     return False, result
        # except Exception as e:
        #     self.logger_main.error(f"[Count] 發生錯誤，請檢查程式: {e}")
        #     result["count"] = "E"
        #     return False, result

    def compareTableSchema(self, table_name):
        result = {"schema": "NA"}
        #try:
        error_flag = False
        sql = f"DESCRIBE {table_name} "
        with self.src_conn.cursor() as src_cursor, self.tg_conn.cursor() as tg_cursor:
            src_cursor.execute(sql)
            source_schema = src_cursor.fetchall()
            self.logger_main.info(f"[Schema]來源表格的 schema: {source_schema}")
            tg_cursor.execute(sql)
            target_schema = tg_cursor.fetchall()
            self.logger_main.info(f"[Schema]目標表格的 schema: {target_schema}")

        if len(source_schema) != len(target_schema):
            self.logger_main.error(f"[Schema] 欄位數量不一致。來源表格有 {len(source_schema)} 個欄位，目標表格有 {len(target_schema)} 個。")
            error_flag = True

        for i in range(len(source_schema)):
            source_col = source_schema[i]
            target_col = target_schema[i]
            if source_col[0] != target_col[0]:
                self.logger_main.error(f"[Schema] 欄位名稱不一致。來源表格的第 {i + 1} 個欄位是 '{source_col[0]}', 但目標表格是 '{target_col[0]}'.")
                error_flag = True
            if source_col[1] != target_col[1]:
                self.logger_main.error(f"[Schema] 欄位型態不一致。來源表格的 '{source_col[0]}' 欄位型態為 '{source_col[1]}', 但目標表格為 '{target_col[1]}'.")
                error_flag = True
        if error_flag:
            result["schema"] = "N"
            return False, result
        else:
            self.logger_main.info(f"[Schema] 欄位名稱與型態皆一致。")
            result["schema"] = "Y"
            return True, result
        # except jaydebeapi.DatabaseError as e:
        #     raise Exception(f"[Schema] 資料庫錯誤: {e}")
        # except Exception as e:
        #     raise Exception(f"[Schema] 發生錯誤: {e}")

    def compareTableData(self, table_name):
        result = {"data": "NA"}
        self.logger_main.info(f"[Data]開始比對 DB:{self.src_db_name} 的 Table:{table_name} 的資料.....")

        # try:
        #     with self.src_conn.cursor() as cursor:
        #         cursor.execute(f'show partitions {table_name}')
        #         rows = cursor.fetchall()
        #         if rows:
        #             self.logger_main.warning(f"[Data] 該表有分區")
        # except Exception as e:
        #     self.logger_main.warning(f"[Data] 取得分區名稱時發生錯誤: {e}")
        #     self.logger_main.warning(f"[Data] 該Table沒有分區")
        

        """取得所有欄位名稱"""
        with self.src_conn.cursor() as cursor:
            cursor.execute(f'select * from {table_name} LIMIT 1')
            column_names = [desc[0] for desc in cursor.description]
        
        """組成concat_expr"""
        safe_cols = ", ".join([
            f"CAST(COALESCE({c}, '') AS string)"
            for c in column_names
        ])
        concat_expr = f"concat_ws('\\u0001', {safe_cols})"
        self.logger_main.info(f"[Data] concat_expr: {concat_expr}")

        """組成source_sql"""
        source_conditions = f"{self.partition_field} = '{self.partition_date}'" if self.partition_date and self.partition_field else ""
        if source_conditions:
            self.logger_main.info(f"[Data] 取得指定分區的資料開始執行SQL")
        else:
            self.logger_main.info(f"[Data] 無指定Partition或是該表無Partition，取得完表的資料開始執行SQL")

        source_sql = (f"SELECT {concat_expr} AS row_key FROM {table_name} where {source_conditions} LIMIT {self.sample_cnt}") if source_conditions \
                else (f"SELECT {concat_expr} AS row_key FROM {table_name} LIMIT {self.sample_cnt}")
        self.logger_main.info(f"[Data] source 預計執行的 SQL: {source_sql}")
        
        """執行source_sql"""
        with self.src_conn.cursor() as cursor:
            cursor.execute(source_sql)
            source_rows = cursor.fetchall()
            sample_keys = [r[0] for r in source_rows]
            if sample_keys:
                self.logger_main.info(f"[Data] 取得source的sample_keys，數量: {len(sample_keys)}")

        """組成target_sql"""
        target_conditions = f"{self.partition_field} = '{self.partition_date}'" if self.partition_date and self.partition_field else ""
        in_list = ",".join(["'" + k.replace("'", "''") + "'" for k in sample_keys])
        where_clause = (f"WHERE {target_conditions} AND {concat_expr} IN ({in_list})") if target_conditions \
               else (f"WHERE {concat_expr} IN ({in_list})")
        target_sql = f"SELECT {concat_expr} AS row_key, COUNT(*) AS cnt FROM {table_name} {where_clause} GROUP BY {concat_expr}"

        """執行target_sql"""
        with self.tg_conn.cursor() as cursor:
            cursor.execute(target_sql)
            rows = cursor.fetchall()
            target_rows = {row[0]: row[1] for row in rows}
            target_cnt = sum([row[1] for row in rows])
      
        """開始檢核Data"""
        if target_cnt != self.sample_cnt:
            self.logger_main.warning(f'[Data] 筆數不一致 目標資料筆數: {target_cnt}  預期資料筆數: {self.sample_cnt}')

        """組成data_detail_results"""
        data_detail_results: List[Tuple[str, str]] = []
        for row_key in sample_keys:
            data_detail_result = target_rows.get(row_key, 0)
            if data_detail_result == 0:
                data_detail_result = "N"
            else:
                data_detail_result = "Y"
            data_detail_results.append((row_key, data_detail_result))
        self.logger_main.info(f"[Data] 資料詳細比對結果組裝完成")
        
        """插入data_detail_results"""
        self.logger_main.info(f"[DB Compare] 開始寫入 Data Detail 檢核結果至 Log Table")
        
        try:
            self.insertDataDetailCompareResult(data_detail_results, table_name)
        except Exception as e:
            # self.logger_main.error(f"[DB Compare] 寫入 Data Detail 檢核結果至 Log Table 失敗")
            self.errorExit(f"[DB Compare] 寫入 Data Detail 檢核結果至 Log Table 失敗") #需要確認是否需要跳出
        
        self.logger_main.info(f"[DB Compare] 完成寫入 Data Detail 檢核結果至 Log Table")

        """檢核 Data Overview 結果"""
        missing = [key for key in sample_keys if target_rows.get(key, 0) == 0]
        if missing:
            self.logger_main.error(f'[Data] 新庫缺少樣本')
            result["data"] = "N"
            return False, result
        else:
            result["data"] = "Y"
            self.logger_main.info(f'[Data] 抽樣全部命中')
            return True, result

    def insertOverviewCompareResult(self, fun:str, result:Dict[str,str], table_name:str):
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        NOT_RUN = "NA"
        # 依本次 fun 決定三個欄位要寫的值
        if fun == "schema":
            schema_v, count_v, data_v = result["schema"], NOT_RUN, NOT_RUN
        elif fun == "count":
            schema_v, count_v, data_v = NOT_RUN, result["count"], NOT_RUN
        elif fun == "data":
            schema_v, count_v, data_v = NOT_RUN, NOT_RUN, result["data"]
        elif fun == "all":
            schema_v, count_v, data_v = result["schema"], result["count"], result["data"]
        compare_db_name = "hadp_out_stg"
        compare_table_name = "compare_overview_log"
        compare_table_full_name = f"{compare_db_name}.{compare_table_name}"
        
        sql = (
                f"INSERT INTO TABLE {compare_table_full_name} "
                f"PARTITION ("
                f"  compare_date='{self.compare_date}', "
                f"  db_name='{self.tg_db_name}', "
                f"  table_name='{table_name}'"
                f") "
                "SELECT "
                f" '{timestamp}' AS ts, "
                f" '{count_v}'  AS COUNT_RESULT, "
                f" '{schema_v}' AS SCHEMA_RESULT, "
                f" '{data_v}'   AS DATA_RESULT, "
                f" '{self.partition_date}' AS partition_date"
            )

        with self.tg_conn.cursor() as cursor:
            cursor.execute(sql)

    def insertDataDetailCompareResult(self, results:List[Tuple[str, str]], table_name:str):
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        compare_db_name = "hadp_out_stg"
        compare_table_name = "compare_data_detail_log"
        compare_table_full_name = f"{compare_db_name}.{compare_table_name}"
        
        for result in results:
            row_key = result[0]
            row_key_safe = row_key.replace("\u0001", "\\u0001")
            data_detail_result = result[1]
            sql = (
                    f"INSERT INTO TABLE {compare_table_full_name} "
                    f"PARTITION ("
                    f"  compare_date='{self.compare_date}', "
                    f"  db_name='{self.tg_db_name}', "
                    f"  table_name='{table_name}'"
                    f")"
                    "SELECT "
                    f" '{timestamp}' AS ts, "
                    f" '{self.partition_date}' AS partition_date, "
                    f" '{row_key_safe}' AS row_key, "
                    f" '{data_detail_result}' AS DATA_DETAIL_RESULT"
                )
            with self.tg_conn.cursor() as cursor:
                cursor.execute(sql)

    def getDatabaseTables(self, conn):
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                rs = cursor.fetchall()
                tables = [table[1] for table in rs]
                return tables
        except Exception as e:
            raise Exception(f"取得表格列表時發生錯誤: {e}")

    def isPartitionTable(self, table_name):
        try:
            with self.src_conn.cursor() as src_cursor, self.tg_conn.cursor() as tg_cursor:
                sql = (
                    f"show partitions {table_name} "
                    f"PARTITION ({self.partition_field} = '{self.partition_date}')"
                )
                src_cursor.execute(sql)
                src_rows = src_cursor.fetchall()
                tg_cursor.execute(sql)
                tg_rows = tg_cursor.fetchall()
                if src_rows and len(src_rows) > 0 and tg_rows and len(tg_rows) > 0:
                    self.logger_main.info(f"[DB Compare] 該表有分區")
                    return True
                elif len(src_rows) == 0 or len(tg_rows) == 0:
                    self.logger_main.warning(f"[DB Compare] 指定的 Partition Date 沒有符合條件的分區")
                    self.logger_main.warning(f"[DB Compare] 來源資料庫的 Partition 數量: {len(src_rows)}")
                    self.logger_main.warning(f"[DB Compare] 目標資料庫的 Partition 數量: {len(tg_rows)}")
                    return False, "X"

        except jaydebeapi.ProgrammingError as e:
            self.logger_main.error(f"[DB Compare] 請檢查三個項目：1. Table 是否存在 2. Table 是否有分區表 3. Partition 欄位是否正確 {e}")
            return False , "X"
        except Exception as e:
            self.logger_main.error(f"[DB Compare] 取得分區名稱時發生意外錯誤: {e}")
            return False, "E"
       
    def run(self, fun):
        self._initialize_logger(fun)
        self.logger_main.setLevel(getattr(logging, self.log_level, logging.INFO))

        """定義檢核程序"""
        comparer_map = {
            'schema': self.compareTableSchema,
            'count':  self.compareTableCount,
            'data':   self.compareTableData,
        }
        if fun.lower() == 'all':
            target_comparers = [('schema', comparer_map['schema']),
                                ('count',  comparer_map['count']),
                                ('data',   comparer_map['data'])]
        elif fun.lower() in comparer_map:
            target_comparers = [(fun.lower(), comparer_map[fun.lower()])]
        else:
            self.errorExit(f"[DB Compare][{fun}] 無效的檢核項目")

        """開始執行檢核程序"""
        self.logger_main.info("開始執行檢核程序.....")
        if self.compare_table:
                tables = [self.compare_table]
                self.logger_main.info(f"[DB Compare] 檢核標的: DB:{self.src_db_name} vs DB:{self.tg_db_name} 的 Table:{self.compare_table}")
        else:
            tables = self.getDatabaseTables(self.src_conn)
            self.logger_main.info(f"[DB Compare] 檢核標的: DB:{self.src_db_name} vs DB:{self.tg_db_name} 的所有 Table, 共 {len(tables)} 張")
        
        for table in tables:
            compare_result = {"schema": "NA", "count": "NA", "data": "NA"}
            table_failed = False
            

            """檢查該表是否有符合條件的分區"""
            if self.partition_date and self.partition_field:
                self.logger_main.info(f"[DB Compare] 檢核標的：指定的分區: ({self.partition_field} = {self.partition_date})，該表是否有符合條件的分區")
                ok, result_code = self.isPartitionTable(table)
                if not ok:
                    if fun == "all":
                        compare_result = {"schema": result_code, "count": result_code, "data": result_code}
                    else:
                        compare_result[fun.lower()] = result_code
                    self.insertOverviewCompareResult(fun=fun.lower(), result=compare_result, table_name=table)
                    self.logger_main.error(f"[DB Compare] {table} 分區檢查失敗，略過此表")
                    continue


            """執行指定Table的比對程式"""
            for key, comparer in target_comparers:
                #重試機制
                retry_flag = False
                retry_count = 0
                max_retry_count = 1
                retry_sleep_time = self.retry_sleep_time
                
                #重試機制
                while True:
                    self.logger_main.info(f"[DB Compare] {table} 開始檢核項目：{key}")
                    #重試機制
                    if retry_flag:
                        self.logger_main.info(f"[DB Compare] {table} 重試第 {retry_count} 次檢核項目：{key}")
                    
                    try:
                        ok, partial_result = comparer(table)
                        compare_result.update(partial_result)
                        if not ok:
                            if partial_result.get(key) == "N":
                                self.logger_main.warning(f"[DB Compare] {table} 檢核項目 {key} 檢核結果：不一致")
                        break #重試機制
                    except jaydebeapi.OperationalError as e:
                        self.logger_main.error(f"[DB Compare]{table} 資料庫負載過重: {e}")
                        compare_result[key] = "E"
                        
                        #重試機制
                        if retry_count < max_retry_count:
                            retry_count += 1
                            retry_flag = True
                            time.sleep(retry_sleep_time)
                            continue
                        else:
                            break

                    except Exception as e:
                        self.logger_main.error(f"[DB Compare] {table} 執行發生例外: {e}")
                        compare_result[key] = "E"
                        break #重試機制
            
            if compare_result.get(key) in ("N", "E", "X"):
                table_failed = True
            
            self.logger_main.info(f"[DB Compare] 開始寫入 {table} 的 {fun} 檢核結果至 Log Table")
            
            try:
                self.insertOverviewCompareResult(fun=fun.lower(), result=compare_result, table_name=table)
            except Exception as e:
                    # self.logger_main.error(f"[DB Compare] {table} 寫入 overview log table 失敗")
                    self.errorExit(f"[DB Compare] 寫入 {fun} 檢核結果至 Log Table 失敗") #需要確認是否需要跳出

            if table_failed:
                self.logger_main.error(f"[DB Compare] {table} 檢核完成，結果不一致或是檢核出現錯誤")
            else:
                self.logger_main.info(f"[DB Compare] {table} 檢核完成，檢核項目: {fun}，檢核結果: 通過")


        self.logger_main.info(f"[DB Compare] {self.src_db_name} 完成所有檢核，檢核項目: {fun}，檢核結果: 通過")
        return True
        # else:
        #     tables = self.getDatabaseTables(self.src_conn)
        #     for table in tables:
        #         for comparer in target_comparers:
        #             self.logger_main.info(f"[DB Compare] 開始檢核項目：{comparer.__name__}")
        #             try:
        #                 result, partial_compare_result = comparer(table)
        #                 compare_result.update(partial_compare_result)
        #                 if not result:
        #                     error_flag = True
        #             except jaydebeapi.DatabaseError as e:
        #                 self.errorExit(f"[{table}] {comparer.__name__} 資料庫錯誤: {e}")
        #             except Exception as e:
        #                 self.errorExit(f"[{table}] {comparer.__name__} 比對發生錯誤: {e}")
               
        #         self.insertOverviewCompareResult(fun=fun, result=compare_result, table_name=table)
        #         if error_flag:
        #             self.errorExit(f"[{table}] 比對失敗")
        #         else:
        #             self.logger_main.info(f"[{table}] 比對完成，檢核項目: {fun}，檢核結果: 通過")
            



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # 定義參數
    parser = argparse.ArgumentParser(description="DB Schema Compare")
    parser.add_argument("--fun", required=True, help="function type")
    parser.add_argument("--mc", required=True, help="Main config file")
    parser.add_argument("--dc", required=True, help="DB Config")
    parser.add_argument("--args", required=True, help="Arguments string")

    # 解析參數
    args = parser.parse_args()
    fun = args.fun
    main_config_file = args.mc
    db_config_file = args.dc
    fc_args = args.args or "{}"

    # 判斷檔案是否存在
    if not os.path.exists(main_config_file):
        print("主設定檔不存在")
        exit(1)
    if not os.path.exists(db_config_file):
        print("DB設定檔不存在")
        exit(1)

    # 建立 main ConfigParser
    main_config = configparser.ConfigParser()
    main_config.read(main_config_file)
    db_config = configparser.ConfigParser()
    db_config.read(db_config_file)

    # 創建 Logger 實例
    Logger.Logger(main_config['LOG']['LOG_PATH'], main_config['LOG']['LOG_NAME'])  # 主要日誌
    # 取得主要 logger 實例
    logger_main = logging.getLogger(main_config['LOG']['LOG_NAME'])
    
    logger_main.info("Run DB Compare")
    try:
        dc = DbCompare(db_config, main_config, fc_args)
        logger_main.info("[DB Compare] 完成 CDH 和 HADP db連線")
    except Exception as e:
        logger_main.error(f"建立dbCompare時發生錯誤: {e}")
        exit(1)
    

    try:
        result = dc.run(fun)
        if result:
            logger_main.info("Run DB Compare success")
            exit(0)
        else:
            logger_main.error("Run DB Compare failed")
            exit(1)
    except Exception as e:
        logger_main.error(f"執行dbCompare時發生錯誤: {e}")
        exit(1)





