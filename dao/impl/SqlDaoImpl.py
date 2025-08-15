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

class SqlDaoImpl(DatabaseDao):

    def __init__(self, host, port, user, sec, database_name, driver):
        self.host = host
        self.port = port
        self.user = user
        self.sec = sec
        self.database = database_name
        self.driver = driver
        self.jdbc_driver = ""
        self.jdbc_url = ""
        self.driver_path = ""
        self.db = self.connectDatabase(self.host, self.port, user, sec, database_name, self.driver)

    def connectDatabase(self, host, port, user, sec, db_name, driver):
        if(driver == "mysql"):
            #For test use hoard code to connect JDBC
            self.jdbc_driver = 'com.mysql.cj.jdbc.Driver'
            self.jdbc_url = f"jdbc:mysql://{host}:{port}/{db_name}"
            self.driver_path = "C:\\Users\\Baldwin\\PycharmProjects\\mysql-connector-j-9.4.0.jar"

        try:
            conn = jaydebeapi.connect(self.jdbc_driver, self.jdbc_url, [user, sec], self.driver_path)
        except jaydebeapi.DatabaseError as e:
            print(f"Error connecting to the database: {e}")
        return conn

    '''
        return: result set
    '''
    def executeSql(self, sql_string):
        try:
            cursor = self.db.cursor()
            cursor.execute(sql_string)
        except Exception as e:
            print(f"Execute SQL error: {e}")

