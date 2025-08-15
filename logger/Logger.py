#coding:utf-8
'''
Logger.py
Object          : The logger to record the information of process.
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

class Logger():

    def __init__(self, log_path, log_name):
        formatter = logging.Formatter(fmt = '%(asctime)-22s %(levelname)-7s %(filename)-6s : %(lineno)d - %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')
        handler = logging.FileHandler('{}/{}.log'.format(log_path, log_name), mode = 'a')
        handler.setFormatter(formatter)
        screen_handler = logging.StreamHandler(stream = sys.stdout)
        screen_handler.setFormatter(formatter)
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.addHandler(screen_handler)
