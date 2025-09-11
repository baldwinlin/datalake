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
import subprocess
from pathlib import Path

import logging
from logger import Logger
# from exception.dataLakeUtilsErrorHandler import dataLakeUtilsErrorHandler
import json
import os
import datetime

class DbtExecutionImpl(DbtExecution):
    def __init__(self, main_config, config, args):
        # config 是從 dbt.conf 來的配置
        self.config = config
        args = json.loads(args)
        # 從 dbt.conf 讀取 shell 配置
        self.shellBase = config['SHELL']['SHELL_BASE']
        self.shellBase = os.path.expandvars(os.path.expanduser(self.shellBase.strip()))
        
        # 從 args 讀取執行參數
        self.command = args['command']
        self.script = args['script']
        self.batch_date = args['batch_date']
        self.env = args.get('env')
        self.debugMode = args.get('debug')

        # 初始化 logger 變數
        self.main_config = main_config
        self.logger_main = None
        self.log_level = self.main_config["LOG"].get("LOG_LEVEL", "INFO").upper()

        # self.errorHandler = None

    def _initialize_logger(self):
        # 讀取日誌配置，優先使用 main_config，否則從 main.conf 讀取
        log_config = self.main_config
        dbt_log_path = f"{log_config['LOG']['LOG_PATH']}/dbt"

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        logger_file_name = f"batch_date_{self.batch_date}_{timestamp}"


        # 創建兩個 Logger 實例
        Logger.Logger(dbt_log_path, logger_file_name) # 主要日誌
        # Logger.Logger(dbt_log_path, "dbt_error_handler") # 錯誤日誌

        # 更新 DbtExecution 的 logger_main 和 errorHandler attribute
        self.logger_main = logging.getLogger(logger_file_name) # 取得主要 logger 實例
        # self.errorHandler = dataLakeUtilsErrorHandler('dbt_error_handler') # 取得錯誤處理器實例


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
            self.logger_main.info(f'執行 shell 命令: {" ".join(shell_args)}')
                
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
        if not self.command or not self.script or not self.batch_date:
            self.logger_main.error("必要參數缺失: command, script, batch_date 必須提供。")
            return False
        
        """檢查 script 路徑是否存在
        original_script = self.script
        script_path = self.script + '.sh'
        self.logger_main.info(f"自動添加副檔名: {original_script} -> {script_path}")
        
        # 檢查檔案是否存在
        script_path = Path(script_path)
        if not script_path.is_file():  # ← 使用 Path.is_file() 檢查檔案存在性
            self.logger_main.error(f"無效的 script 路徑: {script_path}。請確保檔案存在。")
            return False
        """

        # 檢查 batch_date 格式
        if not isinstance(self.batch_date, str) or len(self.batch_date) != 8 or not self.batch_date.isdigit():
            self.logger_main.error("batch_date 必須是一個 8 位數字字符串，格式為 YYYYMMDD。")
            return False
        
        # 檢查 command 是否在允許的範圍內
        if self.command not in ["build", "build_upstream", "run", "run_upstream", "test", "snapshot", "snapshot_upstream", "docs"]:
            self.logger_main.error(f"無效的 command: {self.command}. 允許的值為 'build', 'build_upstream', 'run', 'run_upstream', 'test', 'snapshot', 'snapshot_upstream', 'docs'.")
            return False
        
        # 檢查 env 是否在允許的範圍內
        if self.env and self.env not in ["dev", "sit", "uat", "prod"]:
            self.logger_main.error(f"無效的環境變數: {self.env}. 允許的值為 'dev', 'sit', 'uat', 'prod'.")
            return False
        return True

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

        """
        如果需要檢查 shell 檔案是否存在，可以在這裡添加檢查邏輯。
        例如：
        if not shell.exists():
            self.logger_main.error(f"Shell 檔案不存在: {shell}")
            self.errorHandler.exceptionWriter(f"Shell 檔案不存在: {shell}")
            return None
        這樣可以確保在執行前 shell 檔案是可用的，避免後續執行失敗。
        """
        return shell
    
    def createCLI(self, shell):
        # 基本執行指令參數組合
        args = [
            "bash", str(shell),
            self.script,
            self.batch_date,
        ]

        if self.env:
            args += [self.env]

        if self.debugMode:
            args += ["--debug"]

        return args
    
    def connectShell(self, args):
        result = subprocess.run(args)
        self.logger_main.info("Exit code: %s", result.returncode)
        
        """
        # 如果需要捕獲輸出，可以使用以下方式
        """
        # # 設定 capture_output=True 和 text=True 來捕獲輸出
        # result = subprocess.run(
        #     args, 
        #     capture_output=True,  # 捕獲 stdout 和 stderr
        #     text=True,           # 將 bytes 轉換為 string
        #     bufsize=1,           # 行緩衝，即時輸出
        #     universal_newlines=True
        # )
        
        # # 記錄標準輸出
        # if result.stdout:
        #     # 將多行輸出分行記錄，便於閱讀
        #     for line in result.stdout.strip().split('\n'):
        #         if line.strip():  # 忽略空行
        #             self.logger_main.info(f"DBT Output: {line}")
        
        # # 記錄標準錯誤
        # if result.stderr:
        #     for line in result.stderr.strip().split('\n'):
        #         if line.strip():  # 忽略空行
        #             self.logger_main.warning(f"DBT Error: {line}")
        
        # # 記錄執行結果
        # self.logger_main.info(f"DBT execution exit code: {result.returncode}")
        
        return result.returncode == 0
