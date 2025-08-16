#coding:utf-8
'''
SqlDaoImpl.py
Object          : connect database and execute SQL
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           :
Parameters      :
Output          :
********************************************************************************
Modify          :
'''
from dao.DatabaseDao import DatabaseDao
import jaydebeapi

class JdbcDaoImpl(DatabaseDao):

    def __init__(self, driver_class, jdbc_url, user, password, driver_jar):
        """
        初始化 DAO
        :param driver_class: JDBC Driver class 名稱，例如 'org.apache.hive.jdbc.HiveDriver'
        :param jdbc_url: JDBC URL，例如 'jdbc:hive2://localhost:10000/default'
        :param user: 使用者名稱
        :param password: 密碼
        :param driver_jar: JDBC Driver jar 檔路徑
        """
        self.driver_class = driver_class
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password
        self.driver_jar = driver_jar
        self.conn = None

    def connect(self):
        """建立資料庫連線"""
        try:
            self.conn = jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                [self.user, self.password],
                self.driver_jar
            )
        except Exception as e:
            # 把錯誤往上丟，讓 Service 層決定怎麼處理
            raise Exception(f"資料庫連線失敗: {e}")

    def executeSql(self, sql):
        """執行單純的 SQL 指令 (如 CREATE TABLE)，成功回傳 True"""
        if not self.conn:
            raise Exception("尚未連線資料庫")

        try:
            curs = self.conn.cursor()
            curs.execute(sql)
            curs.close()
            return True
        except Exception as e:
            raise Exception(f"執行 SQL 指令失敗: {e}")


    def executeQuery(self, sql, params=None):
        """
        執行查詢 SQL
        :param sql: SQL 語句
        :param params: 可選，查詢參數 tuple
        :return: 查詢結果 list
        """
        if not self.conn:
            raise Exception("尚未連線資料庫")

        try:
            curs = self.conn.cursor()
            curs.execute(sql, params or [])
            result = curs.fetchall()
            curs.close()
            return result
        except Exception as e:
            raise Exception(f"執行查詢失敗: {e}")

    def executeUpdate(self, sql, params=None):
        """
        執行更新/插入/刪除 SQL
        :param sql: SQL 語句
        :param params: 可選，參數 tuple
        :return: 影響筆數
        """
        if not self.conn:
            raise Exception("尚未連線資料庫")

        try:
            curs = self.conn.cursor()
            curs.execute(sql, params or [])
            affected = curs.rowcount
            self.conn.commit()
            curs.close()
            return affected
        except Exception as e:
            raise Exception(f"執行更新失敗: {e}")

    def close(self):
        """關閉資料庫連線"""
        try:
            if self.conn:
                self.conn.close()
        except Exception as e:
            raise Exception(f"關閉連線失敗: {e}")

