#coding:utf-8
'''
UploadCheckImpl.py
Object          : 上架檢核
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1. 依據指定的資料表與檢核表，依據指定欄位檢核資料筆數是否正確
                  2. 資料筆數不一致會失敗告警
                  3. 0筆告警(可設定忽略)
Parameters      :
Output          :
********************************************************************************
Modify          :
'''

from service.UploadCheck import *
import json
from pathlib import Path

class UploadCheckImpl(UploadCheck):

    def __init__(self, main_config, fc_config, pc_config, args):
        self.fc_config = fc_config
        self.main_config = main_config
        self.args_str = args
        self.logger_main = None
        self.errorHandler = None
        self.args_dict = json.loads(self.args_str)
        self.log_level = main_config["LOG"].get("LOG_LEVEL", "INFO").upper()

        try:
            temp_path = self.main_config.get('LOG', 'TEMP_PATH')
            self.temp_path = Path(temp_path) / "uc"
        except Exception as e:
            raise Exception(f"[讀取TEMP path錯誤] {e}")

        try:
            self.driver_path = self.main_config.get('DB_DRIVER', 'DRIVER_PATH')
        except Exception as e:
            raise Exception(f"[讀取DB dreiver path錯誤] {e}")

    def run(self):
        pass