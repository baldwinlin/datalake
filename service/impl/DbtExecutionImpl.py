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
        self.config = config
        args = json.loads(args)
       
        try:
            self.projectBase = config['DBT']['PROJECT_BASE']
            self.projectBase = os.path.expandvars(os.path.expanduser(self.projectBase.strip()))
            self.shellBase = self.projectBase + "/bin"
            self.shellBase = os.path.expandvars(os.path.expanduser(self.shellBase.strip()))
            self.dbt_project_name = config['DBT']['DBT_PROJECT_NAME']
            
            #測試時需要解開註解
            # self.user = self.config.get('SEC','DBT_USER')
            # self.sec = self.config.get('SEC','DBT_SEC')
            #正式使用時需要解開註解
            self.dbt_sec_file = self.config.get('SEC','DBT_SEC_FILE')
            self.dbt_key_file = self.config.get('SEC','DBT_SEC_KEY')
            self.user, self.sec_str = readSecFile(self.dbt_sec_file)
            self.salt = readSaltFile(self.dbt_key_file)
            self.sec = get_gpg_decrypt(self.sec_str, self.salt)
        except Exception as e:
            raise Exception(f"讀取dbt config錯誤: {e}")
        
        # 測試用
        print(f"user: {self.user}")
        print(f"sec: {self.sec}")
        
        try:
            self.command = args['command']
            self.target = args['target']
        except Exception as e:
            raise Exception(f"讀取dbt執行參數錯誤:請檢查是否有提供必要的參數: command, target")
        
        self.sql_file = args.get('sql_file', None)
        self.batch_date = args.get('batch_date', None)
        self.debugMode = args.get('debug', False)
        if self.command in ["build", "build_upstream", "run", "run_upstream", "test", "snapshot", "snapshot_upstream"]:
            if not self.sql_file or not self.batch_date:
                raise Exception("必要參數缺失: sql_file 和 batch_date 必須提供。")
        elif self.command in ["docs"]:
            if not self.batch_date:
                raise Exception("必要參數缺失: batch_date 必須提供。")

        self.main_config = main_config
        self.logger_main = None
        self.log_level = self.main_config["LOG"].get("LOG_LEVEL", "INFO").upper()

    def _initialize_logger(self):
        log_config = self.main_config
        dbt_log_path = f"{log_config['LOG']['LOG_PATH']}/dbt"
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        
        if self.sql_file:
            logger_file_name = f"{self.command}_{self.sql_file}_{timestamp}"
        else:
            logger_file_name = f"{self.command}_{timestamp}"
        
        Logger.Logger(dbt_log_path, logger_file_name) # 主要日誌
        self.logger_main = logging.getLogger(logger_file_name) # 取得主要 logger 實例

    def errorExit(self, error_message):
        self.logger_main.error(error_message)
        exit(1)  

    def run(self):
        self._initialize_logger()  # 初始化 logger
        try:
            # 檢查必要參數
            if not self._validate_parameters():
                return False
            else:
                self.logger_main.info("所有必要參數已驗證通過。")
            
            # 選擇 shell 檔案
            try:
                shell = self.chooseShellFile()
            except Exception as e:
                self.errorExit(f"選擇 shell 檔案時發生錯誤: {str(e)}")
            self.logger_main.info(f"選擇 shell 檔案: {shell}")
            
            # 創建 CLI 參數
            try:
                shell_args = self.createCLI(shell)
            except Exception as e:
                self.errorExit(f"創建 CLI 參數時發生錯誤: {str(e)}")
            self.logger_main.info(f"創建 CLI 參數完成")
            
            # 創建安全的 log 版本（遮罩敏感參數）
            try:
                safe_args = self._create_safe_log_args(shell_args)
            except Exception as e:
                self.errorExit(f"創建安全的 log 版本時發生錯誤: {str(e)}")
            self.logger_main.info(f"創建安全的 log 版本完成")
            self.logger_main.info(f'執行 shell 命令: {" ".join(safe_args)}')
                
            # 執行 shell 命令
            try:
                success = self.connectShell(shell_args)
            except Exception as e:
                self.errorExit(f"執行 shell 命令時發生錯誤: {str(e)}")
            self.logger_main.info(f"執行 shell 命令完成")
            
            if success:
                self.logger_main.info("DBT 執行完成.")
                return True
            else:
                self.errorExit("DBT 執行失敗.")

        except Exception as e:
            self.errorExit(f"執行過程中發生例外: {str(e)}")

    def _validate_parameters(self):
        # 檢查 batch_date 格式
        if self.batch_date:
            try:
                self.batch_date = datetime.datetime.strptime(self.batch_date, "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                self.errorExit("batch_date 必須符合格式 YYYYMMDD。")

        # 檢查 command 是否在允許的範圍內
        if self.command not in ["debug", "build", "build_upstream", "run", "run_upstream", "test", "snapshot", "snapshot_upstream", "docs"]:
            self.errorExit(f"無效的 command: {self.command}. 允許的值為 'debug', 'build', 'build_upstream', 'run', 'run_upstream', 'test', 'snapshot', 'snapshot_upstream', 'docs'.")

        # 檢查 target 是否在允許的範圍內
        if self.target not in ["uat", "sit", "prod"]:
            self.errorExit(f"無效的 target: {self.target}. 允許的值為 'uat', 'sit', 'prod'.")

        #檢查 shellBase 是否正確存在
        if not Path(self.shellBase).exists():
            self.errorExit(f"shellBase 不存在: {self.shellBase}")
        
        #檢查 dbt_project_name 是否正確存在
        dbt_project_path = Path(self.projectBase) / self.dbt_project_name
        if not dbt_project_path.exists():
            self.errorExit(f"dbt_project_name 不存在: {dbt_project_path}")
        
        return True

    def chooseShellFile(self):
        """選擇對應的 shell 檔案"""
        SHELL_MAP = {
            "debug": "dbt_debug.sh",
            "build": "dbt_build_node.sh",
            "build_upstream": "dbt_build_node_upstream.sh",
            "run": "dbt_run_node.sh",
            "run_upstream": "dbt_run_node_upstream.sh",
            "test": "dbt_test_node.sh",
            "snapshot": "dbt_snapshot_node.sh",
            "snapshot_upstream": "dbt_snapshot_node_upstream.sh",
            "docs": "dbt_docs_serve.sh",
        }
        # 根據 command 選擇對應的 shell 檔案
        shell = Path(self.shellBase) / SHELL_MAP.get(self.command)
        return shell
    
    def createCLI(self, shell):
        # 基本執行指令參數組合
        if self.command not in ["docs", "debug"]:
            args = [
                "bash", str(shell),
                self.sql_file,
                self.batch_date,
                self.target,
                self.dbt_project_name,
                self.user,
                self.sec,
            ]
        elif self.command in ["docs"]:
            args = [
                "bash", str(shell),
                self.batch_date,
                self.target,
                self.dbt_project_name,
                self.user,
                self.sec,
            ]
        elif self.command in ["debug"]:
            args = [
                "bash", str(shell),
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
        
        if self.command  not in ["docs", "debug"]:
            safe_args[6] = "***"  
            safe_args[7] = "***"  
        elif self.command in ["docs"]:
            safe_args[5] = "***"  
            safe_args[6] = "***"  
        elif self.command in ["debug"]:
            safe_args[4] = "***"  
            safe_args[5] = "***"  
        return safe_args

    def connectShell(self, args):
        result = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.stdout:
            for line in result.stdout.splitlines():
                self.logger_main.info(line)

        if result.stderr:
            for line in result.stderr.splitlines():
                self.logger_main.warning(line)

        self.logger_main.info("Exit code: %s", result.returncode)

        combined_output = "\n".join(filter(None, [result.stdout, result.stderr]))

        if self._contains_error_output(combined_output):
            self.errorExit("DBT 執行未選取任何SQL檔案，視為失敗。")

        return result.returncode == 0

    def _contains_error_output(self, output: str) -> bool:
        if not output:
            self.errorExit("Shell 執行結果為空，視為失敗。")
        
        error_types = {
            "does not match any enabled nodes": "執行未選取任何SQL檔案，視為失敗。",
            "does not match any nodes": "執行未選取任何SQL檔案，視為失敗。",
            "no nodes selected": "執行未選取任何SQL檔案，視為失敗。",
            "nothing to do. try checking your model configs": "執行未選取任何SQL檔案，視為失敗。",
            "does not have a target named": "不正確的連線名稱，請檢查target是否正確。",
        }
        

        output_lower = output.lower()
        
        for error_type, error_message in error_types.items():
            if error_type in output_lower:
              self.logger_main.error(error_message)
              return True
        return False