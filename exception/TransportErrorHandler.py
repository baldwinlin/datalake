#coding:utf-8
'''
TransportErrorHandler.py
Object          : The error handler for the process. To write the error message to log/error.log.
Author          : CTBC_ISD_EA Steven Li(#2164)
Version         : 1.0
Date written    : 2019-06-11
Modify Date     :
Memo.           :
Parameters      : 
Output          :
********************************************************************************
Modify          :
'''
import os, sys
import logging

class TransportErrorHandler():

    def __init__(self, logger_name):
        self.LOGGER_ERROR = logging.getLogger(logger_name)

    def exceptionWriter(self, error_message):
        self.LOGGER_ERROR.error(error_message)

    