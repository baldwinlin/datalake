#coding:utf-8
'''
DbtExecution.py
Object          : trigger 指定的 dbt trigger shell 運行 dbt 透過 DDAE 從 MSSQL 和 Oracle 取得檔案並寫入 Hive table 或 S3 Object Store.
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1. 以 Python 根據 CMD 呼叫指定的 trigger_dbt.sh，將 script, batch_date, --env, --debug 參數傳給 shell。
                  2. shell 會根據 dbt.conf 中的 SHELL_BASE 路徑，切換不同的 dbt 專案。
                  3. dbtExecution 會 map 轉換 CMD 到指定的 shell 檔案名稱，和 Shell base 組合路徑，驅動指定的 shell 檔。
                  4. dbt trigger shell 檔案會根據傳入的參數運行相應的 dbt 命令。
                 
Parameters      : 1. dbt config file(Shell base)
                  1.1 Script, Date, env, --debug 參數透過 CLI 傳入，不透過 Config file

Output          :
********************************************************************************
Modify          :
'''
import abc

class DbtExecution(abc.ABC):


    @abc.abstractmethod
    def __init__(self, config, args):
        return NotImplemented

    '''
        DBT execution main flow
    '''
    @abc.abstractmethod
    def run(self):
        return NotImplemented

    '''
        @dbt_config: read from the dbt config file.

    '''
    @abc.abstractmethod
    def chooseShellFile(self):
        return NotImplemented

    '''
        @ftp_path/name_pattern: read from the ftp config file.
        @date_string: from command argument
        return: ftp_file_list
    '''
    @abc.abstractmethod
    def createCLI(self):
        return NotImplemented

    '''
        @work_path: read from config file
    '''
    @abc.abstractmethod
    def connectShell(self, name, size):
        return NotImplemented