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

    def __init__(self):
        #Initialize DB connection
        return NotImplemented


    '''
        return: result set
    '''
    @abc.abstractmethod
    def executeSql(self, sql_string):
        return NotImplemented