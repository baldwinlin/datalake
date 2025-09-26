#coding:utf-8
'''
XUtil.py
Object          : The main function of XUtil process.
                  Usage: DataLakeUtils -fun SQL --mc main_config_file --fc ftp_config_file --args {"date":"YYYYMMDD","xxx":xxx}
                         FL - Ftp loader
                         FW - Ftp writter
                         SQL - SQL execution
                         DBT - DBT
                         AIB - Airbyte Execution
                         AIC - Airbyte Cancell

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
from service.impl.FtpLoaderImpl import *
from service.impl.FtpWritterImpl import *
from service.impl.SqlExecutionImpl  import *
from service.impl.DbtExecutionImpl import *
from service.impl.AirbyteExecutionImpl import *
from service.impl.AirbyteCancelForced import *
from service.impl.UploadCheckImpl import *
from service.impl.HouseKeepingImpl import *
from util.CleanTempFIle import *

from logger import Logger
from exception.dataLakeUtilsErrorHandler import dataLakeUtilsErrorHandler
from crypto.Aes256Crypto import *

import logging
import os
import json
import configparser
import argparse


def readConfig(config_file):
    pass


def run(fun, main_config, fc_config, pc_config, fc_args, sql_file):

    print("")
    if(fun == 'FL'):
        logger_main.info("Run FTP Loader")
        try:
            service = FtpLoaderImpl(main_config, fc_config, pc_config, fc_args)
        except Exception as e:
            logger_main.error(f"建立FtpLoaderImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"建立FtpLoaderImpl錯誤 {e}")
            exit(1)
        
        if (service.run()):
            logger_main.info("Run FTP Loader success")
            exit(0)


    elif(fun == 'FW'):
        logger_main.info("Run FTP Writter")
        try:
            ftp_writter = FtpWritterImpl(main_config, fc_config, pc_config, fc_args, sql_file)
        except Exception as e:
            logger_main.error(f"FtpWritterImpl {e}")
            errorHandler.exceptionWriter(f"FtpWritterImpl {e}")
            exit(1)

        if (ftp_writter.run()):
            logger_main.info("Run FTP writter success")
            exit(0)

    elif(fun == 'SQL'):
        logger_main.info("Run SQL Exception")
        try:
            sql_execution = SqlExecutionImpl(main_config, fc_config, fc_args, sql_file)
        except Exception as e:
            logger_main.error(f"建立SqlExecutionImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"建立SqlExecutionImpl錯誤 {e}")
            exit(1)

        sql_execution.setLog(logger_main, errorHandler)
        if(sql_execution.run()):
            logger_main.info("Run SQL Exception success")
            exit(0)

    elif(fun == 'DBT'):
        logger_main.info("Run DBT Execution")
        try:
            dbt_execution = DbtExecutionImpl(main_config, fc_config, fc_args)
        except Exception as e:
            logger_main.error(f"建立DbtExecutionImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"建立DbtExecutionImpl錯誤 {e}")
            exit(1)
        # dbt_execution.setLog(logger_main, errorHandler)
        result = dbt_execution.run()
        if result == True:
            logger_main.info("Run DBT Execution success")
            exit(0)
        elif result == False:
            logger_main.error("Run DBT Execution failed")
            exit(1)

    elif(fun == 'AIB'):
        logger_main.info("Run Airbyte Execution")
        try:
            airbyte_execution = AirbyteExecutionImpl(main_config, fc_config, fc_args, pc_config)
        except Exception as e:
            logger_main.error(f"建立AirbyteExecutionImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"建立AirbyteExecutionImpl錯誤 {e}")
            exit(1)
        # airbyte_execution.setLog(logger_main, errorHandler)
        result = airbyte_execution.run()
        if result == True:
            logger_main.info("Run Airbyte Execution success")
            exit(0)
        elif result == False:
            logger_main.error("Run Airbyte Execution failed")
            exit(1)

    elif(fun == 'AIC'):
        logger_main.info("Run Airbyte Cancel Forced")
        try:
            airbyte_cancel = AirbyteCancelForcedImpl(main_config, fc_config, fc_args)
        except Exception as e:
            logger_main.error(f"建立AirbyteCancelForcedImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"建立AirbyteCancelForcedImpl錯誤 {e}")
            exit(1)
        result = airbyte_cancel.run()
        if result == True:
            logger_main.info("Run Airbyte Cancel Forced success")
            exit(0)
        elif result == False:
            logger_main.error("Run Airbyte Cancel Forced failed")
            exit(1)

    elif (fun == 'UC'):
        logger_main.info("Run Upload Check")
        try:
            uc = UploadCheckImpl(main_config, fc_config, pc_config, fc_args)
        except Exception as e:
            logger_main.error(f"UploadCheckImpl {e}")
            errorHandler.exceptionWriter(f"UploadCheckImpl {e}")
            exit(1)

        if (uc.run()):
            logger_main.info("Run Upload Check success")
            exit(0)
    
    elif (fun == 'HK'):
        logger_main.info("Run Housekeeping")
        try:
            hk = HouseKeepingImpl(main_config, fc_config, pc_config, fc_args)
        except Exception as e:
            logger_main.error(f"HouseKeepingImpl {e}")
            errorHandler.exceptionWriter(f"HouseKeepingImpl {e}")
            exit(1)
        if (hk.run()):
            logger_main.info("Run Housekeeping success")
            exit(0)

    elif (fun == "AL"):
        logger_main.info("[AL] Run Airbyte and Load Date to Hive Table")

        try:
            logger_main.info("[AL] 讀取 SQL 檔案")
            ddl_sql = pc_config.get('SQL', 'DDL_SQL')
            sql_list_str = pc_config.get('SQL', 'SQLS')
            sql_list = json.loads(sql_list_str)

        except Exception as e:
            logger_main.error(f"[AL] 讀取 SQL 檔案錯誤 {e}")
            errorHandler.exceptionWriter(f"[AL] 讀取 SQL 檔案錯誤 {e}")
            exit(1)

        try:
            airbyte = AirbyteExecutionImpl(main_config, fc_config, fc_args, pc_config)
        except Exception as e:
            logger_main.error(f"[AL-Airbyte] 建立AirbyteExecutionImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"[AL-Airbyte] 建立AirbyteExecutionImpl錯誤 {e}")
            exit(1)

        result = airbyte.run()
        if result == True:
            SQLS_mode = False
            logger_main.info("[AL-Airbyte] Run Airbyte Execution success")
            s3check_result = airbyte.validate_sync_result_s3()
            if s3check_result == "S3_EMPTY":
                logger_main.info("[AL-Airbyte] S3 檔案列表為空")
                SQLS_mode = False
            elif s3check_result == "S3_NOT_EMPTY":
                logger_main.info("[AL-Airbyte] S3 檔案列表不為空")
                SQLS_mode = True
        elif result == False:
            logger_main.error("[AL-Airbyte] Run Airbyte Execution failed")
            exit(1)


        try:
            DDL_SQL = SqlExecutionImpl(main_config, fc_config, fc_args, ddl_sql)
        except Exception as e:
            logger_main.error(f"[AL-DDL SQL] 建立SqlExecutionImpl錯誤 {e}")
            errorHandler.exceptionWriter(f"[AL-DDL SQL] 建立SqlExecutionImpl錯誤 {e}")
            exit(1)
        result = DDL_SQL.run()
        if result == True:
            logger_main.info("[AL-DDL SQL] Run Sql Execution success")
        elif result == False:
            logger_main.error("[AL-DDL SQL] Run Sql Execution failed")
            exit(1)
        

        if SQLS_mode == True:
            logger_main.info("[AL-SQLS] 執行 SQLS 所有 SQL 檔案")
            for idx, sql in enumerate(sql_list):
                try:
                    sql_execution = SqlExecutionImpl(main_config, fc_config, fc_args, sql)
                except Exception as e:
                    logger_main.error(f"[AL-SQLS] 建立SqlExecutionImpl錯誤 {e}")
                    errorHandler.exceptionWriter(f"[AL-SQLS] 建立SqlExecutionImpl錯誤 {e}")
                    exit(1)

                result = sql_execution.run()
                if result == True:
                    logger_main.info("[AL-SQLS] [{}] Run Sql Execution success".format(idx+1))
                elif result == False:
                    logger_main.error("[AL-SQLS] [{}] Run Sql Execution failed".format(idx+1))
                    exit(1)
            logger_main.info("[AL-SQLS] 執行 SQLS 所有 SQL 檔案完成")
        elif SQLS_mode == False:
            logger_main.info("[AL-SQLS] Airbyte 的S3沒有新的檔案 ，不用執行 SQLS")

        logger_main.info("[AL] Run Airbyte and Load Date to Hive Table success")
        exit(0)
        
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
    parser.add_argument("--pc", help="Process config file")

    # 解析參數
    args = parser.parse_args()

    fun = args.fun
    main_confg_file = args.mc
    function_config_file = args.fc
    fc_args = args.args
    sql_file = args.sqlfile
    process_confif_file = args.pc

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

    # 建立 fuction ConfigParser
    if(process_confif_file):
        pc_config = configparser.ConfigParser()
        pc_config.read(process_confif_file)
    else:
        pc_config = None

    # 創建兩個 Logger 實例
    Logger.Logger(main_config['LOG']['LOG_PATH'], main_config['LOG']['LOG_NAME'])  # 主要日誌
    Logger.Logger(main_config['LOG']['LOG_PATH'], main_config['LOG']['ERROR_HANDLER'])  # 錯誤日誌

    # 取得主要 logger 實例
    logger_main = logging.getLogger(main_config['LOG']['LOG_NAME'])

    # 創建錯誤處理器
    errorHandler = dataLakeUtilsErrorHandler(main_config['LOG']['ERROR_HANDLER'])

    run(fun, main_config, fc_config, pc_config, fc_args, sql_file)

