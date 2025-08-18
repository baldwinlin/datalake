#coding:utf-8
'''
XUtil.py
Object          : The main function of XUtil process.
                  Usage: DataLakeUtils -fun SQL --mc main_config_file --fc ftp_config_file --args {"date":"YYYYMMDD","xxx":xxx}
                         FL - Ftp loader
                         FW - Ftp writter
                         SQL - SQL execution
                  Ex: python dataLakeUtils.py -fun FL --mc .\conf\main.conf -fc .\conf\ftpload.conf --args {\"date\":\"20250811\"}
                  python dataLakeUtils.py -fun SQL --mc .\conf\main.conf -fc .\conf\sample_sql.conf --sqlfile xxx --args {\"R_NAME\":\"Tom\"}
                  mac : python dataLakeUtils.py --fun DBT --mc "./conf/main.conf" --fc "./conf/dbt.conf" --args "{\"batch_date\":\"20250811\",\"command\":\"build\",\"script\":\"exec/marts.fx._bond_report.bbgc_descriptive_info\",\"env\":\"dev\",\"debug\":\"--debug\"}"
                  windows : python dataLakeUtils.py --fun DBT --mc ".\conf\main.conf" --fc ".\conf\dbt.conf" --args "{\"batch_date\":\"20250811\",\"command\":\"build\",\"script\":\"exec/marts.fx._bond_report.bbgc_descriptive_info\",\"env\":\"uat\",\"debug\":\"--debug\"}"
                  windows : python dataLakeUtils.py --fun DBT --mc ".\conf\main.conf" --fc ".\conf\dbt.conf" --args "{\"batch_date\":\"20250811\",\"command\":\"run\",\"script\":\"exec/marts.fx._bond_report.bbgc_descriptive_info\",\"env\":\"prod\",\"debug\":\"--debug\"}"

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
import sys
import os
import configparser
import argparse
from service.impl.FtpLoaderImpl import *
from service.impl.SqlExecutionImpl  import *
import json
from service.impl.DbtExecutionImpl import *
import logging
from logger import Logger
from exception.dataLakeUtilsErrorHandler import dataLakeUtilsErrorHandler


from crypto.Aes256Crypto import *

def readConfig(config_file):
    pass


def run(fun, main_config, config, fc_args, sql_file):

    print("")
    if(fun == 'FL'):
        print("Run FTP Loader")
        ftp_loader = FtpLoaderImpl(config, fc_args)
        ftp_loader.run()

    elif(fun == 'FW'):
        print("Run FTP Writer")

    elif(fun == 'SQL'):
        logger_main.info("Run SQL Exception")
        print("Run SQL Execution")
        try:
            sql_execution = SqlExecutionImpl(main_config, config, fc_args, sql_file)
        except Exception as e:
            logger_main.error(f"建立SqlExecutionImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"建立SqlExecutionImpl錯誤 {e}")
            exit(1)

        sql_execution.setLog(logger_main, errorHandler)
        if(sql_execution.run()):
            logger_main.info("Run SQL Exception success")

    elif(fun == 'DBT'):
        logger_main.info("Run DBT Execution")
        try:
            dbt_execution = DbtExecutionImpl(config, fc_args)
        except Exception as e:
            logger_main.error(f"建立DbtExecutionImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"建立DbtExecutionImpl錯誤 {e}")
            exit(1)
        dbt_execution.setLog(logger_main, errorHandler)
        if(dbt_execution.run()):
            logger_main.info("Run DBT Execution success")

    else:
        print("No such function")
        exit(1)



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Data Lake Utilities")

    # 定義參數
    parser.add_argument("--fun", required=True, help="function type")
    parser.add_argument("--mc", required=True, help="Main config file")
    parser.add_argument("--fc", required=True, help="Function config file")
    parser.add_argument("--args", help="Arguments string")
    parser.add_argument("--sqlfile", help="SQL file path")

    # 解析參數
    args = parser.parse_args()

    fun = args.fun
    main_confg_file = args.mc
    function_config_file = args.fc
    fc_args = args.args
    sql_file = args.sqlfile

    #判斷檔案是否存在
    if not os.path.exists(main_confg_file):
        print("主設定檔不存在")
        exit(1)
    if not os.path.exists(function_config_file):
        print("功能設定檔不存在")
        exit(1)

    # 建立 main ConfigParser
    main_config = configparser.ConfigParser()
    main_config.read(main_confg_file)

    # 建立 fuction ConfigParser
    fc_config = configparser.ConfigParser()
    fc_config.read(function_config_file)

    # 創建兩個 Logger 實例
    Logger.Logger(main_config['LOG']['LOG_PATH'], main_config['LOG']['LOG_NAME'])  # 主要日誌
    Logger.Logger(main_config['LOG']['LOG_PATH'], main_config['LOG']['ERROR_HANDLER'])  # 錯誤日誌

    # 取得主要 logger 實例
    logger_main = logging.getLogger(main_config['LOG']['LOG_NAME'])

    # 創建錯誤處理器
    errorHandler = dataLakeUtilsErrorHandler(main_config['LOG']['ERROR_HANDLER'])

    run(fun, main_config, fc_config, fc_args, sql_file)

