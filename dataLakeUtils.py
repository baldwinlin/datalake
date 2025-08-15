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
import configparser
import argparse
from service.impl.FtpLoaderImpl import *
from service.impl.SqlExecutionImpl  import *
import json
from service.impl.DbtExecutionImpl import *


from crypto.Aes256Crypto import *

def readConfig(config_file):
    pass


def run(fun, config, fc_args, sql_file):

    print("")
    if(fun == 'FL'):
        print("Run FTP Loader")
        ftp_loader = FtpLoaderImpl(config, fc_args)
        ftp_loader.run()

    elif(fun == 'FW'):
        print("Run FTP Writer")

    elif(fun == 'SQL'):
        print("Run SQL Execution")
        sql_execution = SqlExecutionImpl(config, fc_args, sql_file)
        sql_execution.run()

    elif(fun == 'DBT'):
        print("Run DBT Execution")
        dbt_execution = DbtExecutionImpl(config, fc_args)
        dbt_execution.run()

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

    # 建立 main ConfigParser
    main_config = configparser.ConfigParser()
    main_config.read(main_confg_file)

    # 建立 fuction ConfigParser
    fc_config = configparser.ConfigParser()
    fc_config.read(function_config_file)


    run(fun, fc_config, fc_args, sql_file)

