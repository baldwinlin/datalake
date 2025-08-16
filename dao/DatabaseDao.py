#coding:utf-8
'''
DatabaseDao.py
Object          : connect DB and execute SQL
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

import abc

class DatabaseDao(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        #Initialize DB connection
        return NotImplemented

    @abc.abstractmethod
    def connect(self):
        return NotImplemented
    '''
        return: result set
    '''
    @abc.abstractmethod
    def executeSql(self, sql_string):
        return NotImplemented

    @abc.abstractmethod
    def executeQuery(self, sql, params=None):
        return NotImplemented

    @abc.abstractmethod
    def executeUpdate(self, sql, params=None):
        return NotImplemented

    @abc.abstractmethod
    def close(self):
        return NotImplemented