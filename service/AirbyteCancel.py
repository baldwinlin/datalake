#coding:utf-8
'''
AirbyteExecution.py
Object          : 使用 Airbyte Cloud API 執行指定的連線，完成資料的同步與轉換
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1. 透過指定 args 攜帶 connection name，觸發執行 airbyte 取得指定的 connection id 回傳給 airbyte 進行資料同步
                  2. airbyte_config 讀取 client_id, client_secret, workspace_id 和 airbyte_root_api
                  3. 使用 CheckConnectionInfo 取得詳細連線資訊
                  4. 使用 triggerSync 觸發資料同步
                  5. 使用 waitSync 等待資料同步完成，如果 token 失效則重新取得 access token

Parameters      : 1. dbt config file(Shell base)
                  1.1 Script, Date, env, --debug 參數透過 CLI 傳入，不透過 Config file

Output          :
********************************************************************************
Modify          :
'''
import abc

class AirbyteCancel(abc.ABC):


    @abc.abstractmethod
    def __init__(self, main_config, config, args):
        return NotImplemented

    '''
        Airbyte execution main flow
    '''
    @abc.abstractmethod
    def run(self):
        return NotImplemented

    '''
        @airbyte_config: read from the airbyte config file.

    '''

    @abc.abstractmethod
    def getAccessToken(self):
        return NotImplemented


    @abc.abstractmethod
    def cancelJob(self, job_id):
        return NotImplemented

    @abc.abstractmethod
    def waitForCancellation(self, job_id):
        return NotImplemented