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
import hashlib
import re

class DbCompare():
    def __init__(self, db_config):
        self.db_config = db_config
        self.driver_path = self.db_config.get('DB_DRIVER', 'DRIVER_PATH')

        #Read source db config
        self.src_host = self.db_config.get('SOURCE', 'HOST')
        self.src_port = self.db_config.get('SOURCE', 'PORT')
        self.src_db_sec_file = self.db_config.get('SOURCE', 'SEC_FILE')
        self.src_db_key_file = self.db_config.get('SOURCE', 'KEY_FILE')
        self.src_db_name = self.db_config.get('SOURCE', 'DB_NAME')
        self.src_driver = self.db_config.get('SOURCE', 'DRIVER')
        self.src_db_user, self.src_db_sec_str = readSecFile(self.src_db_sec_file)
        self.src_db_salt = readSaltFile(self.src_db_key_file)
        self.src_db_sec = get_gpg_decrypt(self.src_db_sec_str, self.src_db_salt)
        self.src_conn = self.connectDb(self.src_driver, self.src_host, self.src_port, self.src_db_name,
                                       self.src_db_user, self.src_db_sec)

        # Read target db config
        self.tg_host = self.db_config.get('TARGET', 'HOST')
        self.tg_port = self.db_config.get('TARGET', 'PORT')
        self.tg_db_sec_file = self.db_config.get('TARGET', 'SEC_FILE')
        self.tg_db_key_file = self.db_config.get('TARGET', 'KEY_FILE')
        self.tg_db_name = self.db_config.get('TARGET', 'DB_NAME')
        self.tg_driver = self.db_config.get('TARGET', 'DRIVER')
        self.tg_db_user, self.tg_db_sec_str = readSecFile(self.tg_db_sec_file)
        self.tg_db_salt = readSaltFile(self.tg_db_key_file)
        self.tg_db_sec = get_gpg_decrypt(self.tg_db_sec_str, self.tg_db_salt)
        self.tg_conn = self.connectDb(self.tg_driver, self.tg_host, self.tg_port, self.tg_db_name,
                                       self.tg_db_user, self.tg_db_sec)

        self.compare_cnt = int(self.db_config.get('DATA', 'COMPARE_CNT'))
        self.compare_table = self.db_config.get('DATA', 'COMPARE_TABLE', fallback = None)

    def connectDb(self, driver, host, port, db_name, user, sec):
        if (driver == 'hive2'):
            driver_class = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
            jdbc_url = f"jdbc:hive2://{host}:{port}/{db_name};auth=LDAP"
            driver_jar = f"{self.driver_path}/kyuubi-hive-jdbc-shaded-1.10.2.jar"
        elif (driver == 'mysql'):
            driver_class = "com.mysql.cj.jdbc.Driver"
            jdbc_url = f"jdbc:mysql://{host}:{port}/{db_name}"
            driver_jar = f"{self.driver_path}\\mysql-connector-j-9.4.0.jar"

        conn = jaydebeapi.connect(driver_class, jdbc_url, [user, sec], driver_jar)
        return conn

    def compareTableCount(self, table_name):
        try:
            src_cursor = self.src_conn.cursor()
            tg_cursor = self.tg_conn.cursor()

            # 1. 取得來源表格的 count
            sql = f"SELECT count(*) FROM {table_name}"
            src_cursor.execute(sql)
            rs = src_cursor.fetchone()
            src_cnt = rs[0] if rs else 0

            # 2. 取得目標表格的 count
            tg_cursor.execute(sql)
            tg_cursor.execute(sql)
            rs = tg_cursor.fetchone()
            tg_cnt = rs[0] if rs else 0

            if src_cnt != tg_cnt:
                print("[{}] 筆數不一致: {} - {}".format(table_name, src_cnt, tg_cnt))


        except jaydebeapi.DatabaseError as e:
            return False, f"資料庫錯誤: {e}"
        except Exception as e:
            return False, f"發生錯誤: {e}"
        finally:
            if src_cursor:
                src_cursor.close()
            if tg_cursor:
                tg_cursor.close()


    def compareTableSchema(self, table_name):
        try:
            src_cursor = self.src_conn.cursor()
            tg_cursor = self.tg_conn.cursor()

            # 1. 取得來源表格的 schema
            sql = f"DESCRIBE {table_name} "
            src_cursor.execute(sql)
            source_schema = src_cursor.fetchall()
            #print("Source schema: ", source_schema)

            # 2. 取得目標表格的 schema
            tg_cursor.execute(sql)
            target_schema = tg_cursor.fetchall()
            #print("Target schema: ", target_schema)



            # 3. 比對欄位數量
            if len(source_schema) != len(target_schema):
                print(f"[{table_name}] 欄位數量不一致。來源表格有 {len(source_schema)} 個欄位，目標表格有 {len(target_schema)} 個。")

            # 4. 比對每個欄位的名稱和型態
            for i in range(len(source_schema)):
                source_col = source_schema[i]
                target_col = target_schema[i]

                # 比較欄位名稱 (第0個元素)
                if source_col[0] != target_col[0]:
                    print(f"[{table_name}] 欄位名稱不一致。來源表格的第 {i + 1} 個欄位是 '{source_col[0]}', 但目標表格是 '{target_col[0]}'.")

                # 比較欄位型態 (第1個元素)
                if source_col[1] != target_col[1]:
                    print(f"[{table_name}] 欄位型態不一致。來源表格的 '{source_col[0]}' 欄位型態為 '{source_col[1]}', 但目標表格為 '{target_col[1]}'.")


        except jaydebeapi.DatabaseError as e:
            return False, f"資料庫錯誤: {e}"
        except Exception as e:
            return False, f"發生錯誤: {e}"
        finally:
            if src_cursor:
                src_cursor.close()
            if tg_cursor:
                tg_cursor.close()

    def getWhereClause(self, partition_list: list) -> str:
        """
        將 SHOW PARTITIONS 語法回傳的列表，轉換成 SQL 的 WHERE 條件。

        Args:
            partition_list (list): 來自 SHOW PARTITIONS 的分區字串列表，
                                   例如 ['year=2024/month=01', 'year=2024/month=02']。

        Returns:
            str: 轉換後的 WHERE 條件字串，例如 "(year='2024' AND month='01') OR (year='2024' AND month='02')".
                 如果輸入列表為空，則回傳空字串。
        """
        if not partition_list:
            return ""

        where_conditions = []

        # 遍歷每一個分區字串
        for partition_str in partition_list:
            # 使用正則表達式解析 'key=value' 鍵值對
            # r'(\w+)=(\S+)'
            #   \w+ 匹配一個或多個單字字元 (a-zA-Z0-9_)
            #   \S+ 匹配一個或多個非空白字元
            # 這可以處理 'key=value' 和 'key=value/key2=value2' 等格式
            matches = re.findall(r'(\w+)=([^/]+)', partition_str)

            # 組成單一分區的條件字串 (例如: "year='2024' AND month='01'")
            single_partition_conditions = []
            for key, value in matches:
                # 將值用單引號包起來，以符合 SQL 語法
                single_partition_conditions.append(f"{key}='{value}'")

            # 將單一分區的條件用 AND 連接，並用括號包起來
            if single_partition_conditions:
                where_conditions.append("(" + " AND ".join(single_partition_conditions) + ")")

        # 將所有分區條件用 OR 連接
        if where_conditions:
            return " OR ".join(where_conditions)
        else:
            return ""



    def compareTableData(self, table_name):
        """
        從兩個不同資料庫的表格中，分批讀取資料並進行哈希比對。

        Args:
            source_conn: 來源資料庫連線物件。
            target_conn: 目標資料庫連線物件。
            source_table (str): 來源表格名稱。
            target_table (str): 目標表格名稱。
        """

        source_hashes = set()
        target_hashes = set()

        # 得到partition name
        partition_names = []
        if self.src_driver.lower() == 'hive2':
            with self.src_conn.cursor() as cursor:
                cursor.execute(f'show partition {table_name}')
                rows = cursor.fetchall()
                partition_names = [row[0] for row in rows].sort(reverse=True)

        # 得到欄位名稱
        with self.src_conn.cursor() as cursor:
            cursor.execute(f'select * from {table_name} LIMIT 1')
            column_names = [desc[0] for desc in cursor.description]
            columns_str = ', '.join(column_names)

        if partition_names:
            conditions = self.getWhereClause(partition_names[:1])
            sql = f"SELECT * FROM {table_name} where {conditions} order by {columns_str}"
        else:
            sql = f"SELECT * FROM {table_name} order by {columns_str}"

        #print("[SQL] ", sql)

        chunk_size = 10000  # 每次讀取的筆數，可根據記憶體調整
        # 處理來源表格
        cnt = 0
        with self.src_conn.cursor() as cursor:
            cursor.execute(sql)
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    row_str = str(row).encode('utf-8')
                    source_hashes.add(hashlib.md5(row_str).hexdigest())
                    cnt += 1
                    if cnt == self.compare_cnt:
                        break
                if cnt == self.compare_cnt:
                    break


        # 處理目標表格
        cnt = 0
        with self.tg_conn.cursor() as cursor:
            cursor.execute(sql)
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    row_str = str(row).encode('utf-8')
                    target_hashes.add(hashlib.md5(row_str).hexdigest())
                    cnt += 1
                    if cnt == self.compare_cnt:
                        break
                if cnt == self.compare_cnt:
                    break


        # 比對哈希集合
        if source_hashes != target_hashes:
            print(f'[{table_name}] 資料不一致')

            # missing_in_target = source_hashes.difference(target_hashes)
            # if missing_in_target:
            #     print(f"\n- 在目標資料庫中缺失的筆數: {len(missing_in_target)}")
            #
            # extra_in_target = target_hashes.difference(source_hashes)
            # if extra_in_target:
            #     print(f"\n- 在來源資料庫中缺失的筆數 (目標多出的): {len(extra_in_target)}")




    def getDatabaseTables(self, conn):
        """
        使用 jaydebeapi 取得資料庫中所有表格的名稱。

        Args:
            conn: 已建立的 jaydebeapi 資料庫連線物件。
        """
        try:
            with conn.cursor() as cursor:
                # 執行 SHOW TABLES 語法
                cursor.execute("SHOW TABLES")

                # 取得所有結果
                rs = cursor.fetchall()

                # 取得table name
                tables = [table[0] for table in rs]

                return tables

        except Exception as e:
            print(f"取得表格列表時發生錯誤: {e}")


    def run(self, fun):
        if self.compare_table:
            if fun.lower() == 'schema':
                self.compareTableSchema(self.compare_table)
            elif fun.lower() == 'count':
                self.compareTableCount(self.compare_table)
            elif fun.lower() == 'data':
                self.compareTableData(self.compare_table)
        else:
            tables = self.getDatabaseTables(self.src_conn)
            for table in tables:
                if fun.lower() == 'schema':
                    self.compareTableSchema(table)
                elif fun.lower() == 'count':
                    self.compareTableCount(table)
                elif fun.lower() == 'data':
                    self.compareTableData(table)




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # 定義參數
    parser = argparse.ArgumentParser(description="DB Schema Compare")
    parser.add_argument("--fun", required=True, help="function type")
    parser.add_argument("--dc", required=True, help="DB Config")

    # 解析參數
    args = parser.parse_args()
    fun = args.fun
    db_config_file = args.dc

    # 判斷檔案是否存在
    if not os.path.exists(db_config_file):
        print("DB設定檔不存在")
        exit(1)

    # 建立 main ConfigParser
    db_config = configparser.ConfigParser()
    db_config.read(db_config_file)

    dc = DbCompare(db_config)
    dc.run(fun)




