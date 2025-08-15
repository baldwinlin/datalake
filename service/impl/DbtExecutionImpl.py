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
import sys



class DbtExecutionImpl(DbtExecution):
    def __init__(self, config, args):

        self.shellBase = config['SHELL']['SHELL_BASE']
        self.command = args['command']
        self.script = args['script']
        self.batch_date = args['batch_date']
        self.env = args.get('env')
        self.debugMode = args.get('debug')

        # 檢查 command 是否符合設定
        try:
            assert self.command in ["build", "build_upstream", "run", "run_upstream", "test", "snapshot", "snapshot_upstream", "docs"]
        except AssertionError:
            print(f"錯誤的 command: {self.command}. 一定要是以下指令當中之一：'build', 'build_upstream', 'run', 'run_upstream', 'test', 'snapshot', 'snapshot_upstream', 'docs'.")
            sys.exit(1)

        try:
            assert self.env in ["dev", "sit", "uat", "prod", None]
        except AssertionError:
            print(f"錯誤的 env: {self.env}. 一定要是以下環境當中之一：'dev', 'sit', 'uat', 'prod'.")
            sys.exit(1)
             
    def run(self):
        shell = self.chooseShellFile()
        args = self.createCLI(shell)

        print("Executing command:", " ".join(args))
        
        success = self.connectShell(args)

        if success:
            print("DBT execution completed successfully.")
        else:
            print("DBT execution failed.")



    def chooseShellFile(self):

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

        shell = Path(self.shellBase) / SHELL_MAP.get(self.command)
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

        print("Exit code:", result.returncode)
        return result.returncode == 0


