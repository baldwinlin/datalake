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
# import json
# import fnmatch
import paramiko
from ftplib import FTP, error_perm

from typing import List, Optional
from dao.FileDao import FileDao
from stat import S_ISREG
from util.FilenameProcessor import FilenameProcessor

class FtpDaoImpl(FileDao):
    def __init__(self, ftp_type, host, port, user, sec, logger=None):
        #default 初始化連線物件
        self.FTP: Optional[FTP] = None
        self.SSH: Optional[paramiko.SSHClient] = None
        self.SFTP: Optional[paramiko.SFTPClient] = None

        # 連線到 FTP/SFTP 伺服器
        self.ftp_type = ftp_type
        self.host = host
        self.port = port
        self.user = user
        self.sec = sec

        self.timeout = 3600
        self.passive = True
        self.connect(ftp_type=ftp_type, host=host, port=port, user=user, sec=sec, timeout=self.timeout, passive=self.passive)
        
        # logger 設定
        self.logger = logger

    def _log(self, level, message):
        if self.logger:
            if level == 'info':
                self.logger.info(message)
            elif level == 'warning':
                self.logger.warning(message)
            elif level == 'error':
                self.logger.error(message)
            elif level == 'debug':
                self.logger.debug(message)
        else:
            print(f"[{level.upper()}] {message}")
            
    def connect(self, ftp_type, host, port, user, sec, timeout, passive):
        if ftp_type == "FTP":
            ftp = FTP()
            ftp.connect(host, port, timeout)
            ftp.login(user, sec)
            ftp.set_pasv(passive)
            ftp.voidcmd("TYPE I")  # 確保使用二進位模式
            self.FTP = ftp  # 儲存 FTP 連線物件
        
        elif ftp_type == "SFTP":
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, port, user, sec, timeout=timeout, allow_agent=False, look_for_keys=False)
            self.SSH = ssh
            ssh.get_transport().set_keepalive(30)  # 設定 keepalive 以保持連線
            sftp = ssh.open_sftp()
            sftp.chdir(sftp.normalize('.'))  # 進入 SFTP 根目錄
            self.SFTP = sftp  # 儲存 SFTP 連線物件

    # # ListFiles 私有函式模組：_process_name_pattern 和 _match_name_pattern
    # def _process_name_pattern(self, name_pattern, date):
    #     """處理檔案名稱模式，替換單一參數日期變數"""
    #     if not name_pattern:
    #         raise Exception("尚未正確設定檔案名稱模式")
        
    #     if "${date}" in name_pattern:
    #         if not date:
    #             raise Exception("尚未正確設定 Batch Date")
    #         processed_name_pattern = name_pattern.replace("${date}", str(date))
    #         return processed_name_pattern
    #     else:
    #         return name_pattern

    # def _match_name_pattern(self, name: str, target_name_pattern: str) -> bool:
    #     """檢查檔案名稱是否符合指定的模式"""
    #     try:
    #         result = fnmatch.fnmatch(name, target_name_pattern)
    #     except Exception as e:
    #         raise Exception(f"檢查檔案名稱是否符合指定的模式失敗: {e}")
    #     return result

    def listFiles(self, source_path, name_pattern, date) -> List[str]:
        """初始化列出符合名稱模式的檔案"""
        target_name_pattern = FilenameProcessor._process_name_pattern(name_pattern, date)
        self._log('info', f"置換後的檔案名稱模式: {target_name_pattern}")
        files: List[str] = []
        
        """使用 FTP server 列出符合名稱模式的檔案，支援 MLSD 和 NLST 列舉檔案"""
        if self.ftp_type == "FTP":
            try: # 使用 MLSD 列出檔案
                for file_name, facts in self.FTP.mlsd(source_path):
                    if facts.get("type") == "file" and FilenameProcessor._match_name_pattern(file_name, target_name_pattern):
                        files.append((file_name))
            except (error_perm, AttributeError): # 如果 MLSD 不可用，使用 nlst
                self._log('warning', "MLSD 不可用，使用 nlst 列出檔案")
                self.FTP.cwd(source_path)
                try:
                    for file_name in self.FTP.nlst():
                        if FilenameProcessor._match_name_pattern(file_name, target_name_pattern):
                            files.append(file_name)
                except Exception as e:
                    raise Exception(f"NLST 列出檔案失敗: {e}")
            return files

        elif self.ftp_type == "SFTP":
            for file_info in self.SFTP.listdir_attr(source_path):
                file_name = file_info.filename
                if (not file_name.startswith('.') and 
                    S_ISREG(file_info.st_mode) and 
                    FilenameProcessor._match_name_pattern(file_name, target_name_pattern)):
                    files.append(file_name)
            return files

    def downloadFile(self, file_name, source_path, target_path) -> None:        
        os.makedirs(target_path, exist_ok=True)
        remote_path = f"{source_path.rstrip('/')}/{file_name}"
        local_path = os.path.join(target_path, file_name)

        writing_mode = "wb"
        blocksize = 64*1024

        if self.ftp_type == "FTP":
            self.FTP.retrbinary(f"RETR {remote_path}", open(local_path, writing_mode).write, blocksize=blocksize)   
        elif self.ftp_type == "SFTP":
            self.SFTP.get(remote_path, local_path)

        
    def uploadFile(self, local_file: str, remote_path: str) -> None:
        remote_file = os.path.join(remote_path, os.path.basename(local_file))
        if self.ftp_type == "FTP":
            try:
                with open(local_file, "rb") as f:
                    self.FTP.storbinary(f"STOR {remote_file}", f)
            except Exception as e:
                raise Exception(f"FTP 上傳檔案失敗 ({local_file} -> {remote_file}): {e}")
        elif self.ftp_type == "SFTP":
            try:
                self.SFTP.put(local_file, remote_file)
            except Exception as e:
                raise Exception(f"FTP 上傳檔案失敗 ({local_file} -> {remote_file}): {e}")

    def close(self):
        if self.FTP:
            self.FTP.quit()
            self.FTP = None
        elif self.SFTP:
           self.SFTP.close()
           self.SFTP = None
           if self.SSH:
               self.SSH.close()
               self.SSH = None

