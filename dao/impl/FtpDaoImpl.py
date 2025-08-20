#coding:utf-8
'''
FtpDaoImpl.py
Object          : FTP file access - oupload files to target path and download files from source path
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

import os
import fnmatch
from ftplib import FTP
from typing import List
from dao.FileDao import FileDao

class FtpDaoImpl(FileDao):
    def __init__(self, host: str, username: str, password: str, port: int = 21, passive: bool = True):
        try:
            self.ftp = FTP()
            self.ftp.connect(host, port)
            self.ftp.login(username, password)
            if passive:
                self.ftp.set_pasv(True)
        except Exception as e:
            raise Exception(f"FTP 連線失敗: {e}")

    def listFiles(self, pattern: str) -> List[str]:
        try:
            files = self.ftp.nlst()
            return [f for f in files if fnmatch.fnmatch(os.path.basename(f), pattern)]
        except Exception as e:
            raise Exception(f"FTP 列出檔案失敗: {e}")

    def downloadFile(self, remote_path: str, local_path: str) -> None:
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                self.ftp.retrbinary(f"RETR {remote_path}", f.write)
        except Exception as e:
            raise Exception(f"FTP 下載檔案失敗 ({remote_path}): {e}")

    def uploadFile(self, local_path: str, remote_path: str) -> None:
        try:
            with open(local_path, "rb") as f:
                self.ftp.storbinary(f"STOR {remote_path}", f)
        except Exception as e:
            raise Exception(f"FTP 上傳檔案失敗 ({local_path}): {e}")

    def close(self):
        if self.ftp:
            self.ftp.quit()

