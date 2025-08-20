#coding:utf-8
'''
FtpDaoImpl.py
Object          : SFTP file access - oupload files to target path and download files from source path
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

from dao.FileDao import FileDao
import os
import fnmatch
import paramiko
from typing import Optional, List

class SftpDaoImpl(FileDao):
    def __init__(self, host: str, username: str, password: Optional[str] = None,
                 port: int = 22, key_filename: Optional[str] = None):
        try:
            self.transport = paramiko.Transport((host, port))
            if key_filename:
                private_key = paramiko.RSAKey.from_private_key_file(key_filename)
                self.transport.connect(username=username, pkey=private_key)
            else:
                self.transport.connect(username=username, password=password)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        except Exception as e:
            raise Exception(f"SFTP 連線失敗: {e}")

    def listFiles(self, pattern: str) -> List[str]:
        try:
            files = self.sftp.listdir(".")
            return [f for f in files if fnmatch.fnmatch(f, pattern)]
        except Exception as e:
            raise Exception(f"SFTP 列出檔案失敗: {e}")

    def downloadFile(self, remote_path: str, local_path: str) -> None:
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.sftp.get(remote_path, local_path)
        except Exception as e:
            raise Exception(f"SFTP 下載檔案失敗 ({remote_path}): {e}")

    def uploadFile(self, local_path: str, remote_path: str) -> None:
        try:
            self.sftp.put(local_path, remote_path)
        except Exception as e:
            raise Exception(f"SFTP 上傳檔案失敗 ({local_path}): {e}")

    def close(self):
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()
