#coding:utf-8
'''
DbtExecutionImpl.py
Object          :
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
from service.DbtExecution import *
from crypto.Aes256Crypto import *
import subprocess
from pathlib import Path

import logging
from logger import Logger
import json
import os
import datetime

class DbtExecutionImpl(DbtExecution):
    def __init__(self, main_config, config, args):
        # config 是從 dbt.conf 來的配置
        self.config = config
        args = json.loads(args)
       
        # 從 dbt.conf 讀取 shell 和 sec 配置
        try:
            self.shellBase = config['SHELL']['SHELL_BASE']
            self.shellBase = os.path.expandvars(os.path.expanduser(self.shellBase.strip()))
            self.dbt_project_name = self.config.get('DBT','DBT_PROJECT_NAME')
            #測試時需要解開註解
            self.user = self.config.get('SEC','DBT_USER')
            self.sec = self.config.get('SEC','DBT_SEC')
            #正式使用時需要解開註解
            # self.dbt_sec_file = self.config.get('SEC','DBT_SEC_FILE')
            # self.dbt_key_file = self.config.get('SEC','DBT_SEC_KEY')
            # self.user, self.sec_str = readSecFile(self.dbt_sec_file)
            # self.salt = readSaltFile(self.dbt_key_file)
            # self.sec = get_gpg_decrypt(self.sec_str, self.salt)
        except Exception as e:
            raise Exception(f"讀取dbt config錯誤: {e}")
        
        # 從 args 讀取 dbt 執行參數
        try:
            self.command = args['command']
            self.sql_file = args['sql_file']
            self.batch_date = args['batch_date']
            self.target = args['target']
            self.debugMode = args.get('debug')
        except Exception as e:
            raise Exception(f"讀取dbt執行參數錯誤:請檢查是否有提供必要的參數: command, sql_file, batch_date, env")

        # 初始化 logger 變數
        self.main_config = main_config
        self.logger_main = None
        self.log_level = self.main_config["LOG"].get("LOG_LEVEL", "INFO").upper()

    def _initialize_logger(self):
        log_config = self.main_config
        dbt_log_path = f"{log_config['LOG']['LOG_PATH']}/dbt"
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        logger_file_name = f"{self.sql_file}_{timestamp}"
        Logger.Logger(dbt_log_path, logger_file_name) # 主要日誌
        self.logger_main = logging.getLogger(logger_file_name) # 取得主要 logger 實例


    def run(self):
        self._initialize_logger()  # 初始化 logger
        try:
            # 檢查必要參數
            if not self._validate_parameters():
                return False
            else:
                self.logger_main.info("所有必要參數已驗證通過。")
            
            # 選擇 shell 檔案
            shell = self.chooseShellFile()
            
            # 創建 CLI 參數
            shell_args = self.createCLI(shell)
            # 創建安全的 log 版本（遮罩敏感參數）
            safe_args = self._create_safe_log_args(shell_args)
            self.logger_main.info(f'執行 shell 命令: {" ".join(safe_args)}')
                
            # 執行 shell 命令
            success = self.connectShell(shell_args)
            if success:
                self.logger_main.info("DBT 執行完成.")
                return True
            else:
                self.logger_main.error("DBT 執行失敗.")
                return False

        except Exception as e:
            self.logger_main.error(f"執行過程中發生例外: {str(e)}")
            return False

    def _validate_parameters(self):
        # 檢查必要參數是否存在
        if not self.command or not self.sql_file or not self.batch_date:
            self.logger_main.error("必要參數缺失: command, sql_file, batch_date 必須提供。")
            return False
        
        # 檢查 batch_date 格式
        if not isinstance(self.batch_date, str) or len(self.batch_date) != 10:
            self.logger_main.error("batch_date 必須符合格式 YYYY-MM-DD。")
            return False

        # 檢查 command 是否在允許的範圍內
        if self.command not in ["build", "build_upstream", "run", "run_upstream", "test", "snapshot", "snapshot_upstream", "docs"]:
            self.logger_main.error(f"無效的 command: {self.command}. 允許的值為 'build', 'build_upstream', 'run', 'run_upstream', 'test', 'snapshot', 'snapshot_upstream', 'docs'.")
            return False
        return True
        
        # 檢查 env 是否在允許的範圍內
        # if self.target and self.target not in ["dev", "sit", "uat", "prod"]:
        #     self.logger_main.error(f"無效的環境變數: {self.target}. 允許的值為 'dev', 'sit', 'uat', 'prod'.")
        #     return False
        
        # # 檢查 batch_date 格式為 YYYYMMDD、YYYY-MM-DD 或 YYYY/MM/DD
        # if not isinstance(self.batch_date, str) or len(self.batch_date) not in [8, 10]:
        #     for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        #         try:
        #             datetime.strptime(self.batch_date, fmt)
        #             return True
        #         except ValueError:
        #             continue    
        #     self.logger_main.error("batch_date 格式錯誤，需為 YYYYMMDD、YYYY-MM-DD 或 YYYY/MM/DD。")
        #     return False

    def chooseShellFile(self):
        """選擇對應的 shell 檔案"""
        SHELL_MAP = {
            "build": "bin/dbt_build_node.sh",
            "build_upstream": "bin/dbt_build_node_upstream.sh",
            "run": "bin/dbt_run_node.sh",
            "run_upstream": "bin/dbt_run_node_upstream.sh",
            "test": "bin/dbt_test_node.sh",
            "snapshot": "bin/dbt_snapshot_node.sh",
            "snapshot_upstream": "bin/dbt_snapshot_node_upstream.sh",
            "docs": "bin/dbt_docs_serve.sh",
        }
        # 根據 command 選擇對應的 shell 檔案
        shell = Path(self.shellBase) / SHELL_MAP.get(self.command)
        return shell
    
    def createCLI(self, shell):
        # 基本執行指令參數組合
        args = [
            "bash", str(shell),
            self.sql_file,
            self.batch_date,
            self.target,
            self.dbt_project_name,
            self.user,
            self.sec,
        ]

        if self.debugMode:
            if self.debugMode in ["--debug", "Y", "y", "yes", "YES", "True", "true", "TRUE", "1"]:
                args += ["--debug"]
            else:
                pass
        return args
    
    def _create_safe_log_args(self, args):
        """創建安全的 log 版本，遮罩敏感參數"""
        safe_args = args.copy()
        # 假設密碼在第5個位置（索引4），根據你的 createCLI 方法
        safe_args[6] = "***"  
        safe_args[7] = "***"  
        return safe_args

    def connectShell(self, args):
        result = subprocess.run(args)
        self.logger_main.info("Exit code: %s", result.returncode)

        return result.returncode == 0
        