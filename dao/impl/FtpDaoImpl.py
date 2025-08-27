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
import json
import fnmatch
import paramiko
from ftplib import FTP, error_perm

from typing import List, Optional, Tuple
from dao.FileDao import FileDao
from stat import S_ISREG


class FtpDaoImpl(FileDao):
    def __init__(self, ftp_type, host, port, user, password):
        #default 初始化連線物件
        self.FTP: Optional[FTP] = None
        self.SSH: Optional[paramiko.SSHClient] = None
        self.SFTP: Optional[paramiko.SFTPClient] = None

       
        # 連線到 FTP/SFTP 伺服器
        self.ftp_type = ftp_type
        self.host = host
        self.port = port
        self.user = user
        self.password = password



        self.timeout = 30
        self.passive = True
        self.connect(ftp_type=ftp_type, host=host, port=port, user=user, password=password, timeout=self.timeout, passive=self.passive)

    def connect(self, ftp_type, host, port, user, password, timeout, passive):
        if ftp_type == "FTP":
            ftp = FTP()
            ftp.connect(host, port, timeout)
            ftp.login(user, password)
            ftp.set_pasv(passive)
            ftp.voidcmd("TYPE I")  # 確保使用二進位模式
            self.FTP = ftp  # 儲存 FTP 連線物件
        
        elif ftp_type == "SFTP":
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, port=port, username=user,
                        password=password, timeout=timeout,
                        allow_agent=False, look_for_keys=False)
            self.SSH = ssh
            ssh.get_transport().set_keepalive(30)  # 設定 keepalive 以保持連線
            sftp = ssh.open_sftp()
            sftp.chdir(sftp.normalize('.'))  # 進入 SFTP 根目錄
            self.SFTP = sftp  # 儲存 SFTP 連線物件

    # ListFiles 私有函式模組：_process_name_pattern 和 _match_name_pattern
    def _process_name_pattern(self, name_pattern, date):
        """處理檔案名稱模式，替換單一參數日期變數"""
        if name_pattern and date:
            processed_name_pattern = name_pattern.replace("${date}", str(date))
            return processed_name_pattern

    def _match_name_pattern(self, name: str, target_name_pattern: str) -> bool:
        """檢查檔案名稱是否符合指定的模式"""
        result = fnmatch.fnmatch(name, target_name_pattern)
        if result:
            print(f"檔案 {name} 符合模式 {target_name_pattern}")
        return result

    def listFiles(self, source_path, name_pattern, date) -> List[str]:
        if self.ftp_type == "FTP":
            if not self.FTP:
                raise Exception("FTP connection is not established. Call connect() first.")
        elif self.ftp_type == "SFTP":
            if not self.SFTP:
                raise Exception("SFTP connection is not established. Call connect() first.")

        """初始化列出符合名稱模式的檔案"""
        target_name_pattern = self._process_name_pattern(name_pattern, date)
        files: List[Tuple[str, Optional[int]]] = []
        
        """使用 FTP server 列出符合名稱模式的檔案，支援 MLSD 和 NLST 列舉檔案"""
        if self.ftp_type == "FTP":
            try: # 使用 MLSD 列出檔案
                for file_name, facts in self.FTP.mlsd(source_path):
                    if facts.get("type") == "file":
                        if self._match_name_pattern(file_name, target_name_pattern):
                            size = int(facts.get("size", 0))
                            files.append((file_name, size))
                print(f"使用 MLSD 找到 {len(files)} 個檔案")
            except (error_perm, AttributeError): # 如果 MLSD 不可用，使用 nlst
                print("MLSD 不可用，使用 nlst 列出檔案")
                self.FTP.cwd(source_path) #切換路徑到來源資料夾，利於後續 size 可以直接使用 name 來 處理每個檔案的大小
                for file_name in self.FTP.nlst():
                    if self._match_name_pattern(file_name, target_name_pattern):
                        size = None
                        try:
                            size = self.FTP.size(file_name)
                        except Exception:
                            pass # 測試是否需要切換到不同的 size 紀錄，還可以用 LIST 指令紀錄檔案資訊
                        files.append((file_name, size))
                print(f"使用 nlst 找到 {len(files)} 個檔案")
            return files
        
        elif self.ftp_type == "SFTP":
            for file_info in self.SFTP.listdir_attr(source_path):
                file_name = file_info.filename

                if file_name.startswith('.'): # 忽略隱藏檔案
                    continue
                if not S_ISREG(file_info.st_mode): # 忽略非一般檔案
                    continue
                if self._match_name_pattern(file_name, target_name_pattern):
                    size = int(file_info.st_size) if file_info.st_size is not None else None
                    files.append((file_name, size))
            return files

    def downloadFile(self, file_name, size, source_path, target_path) -> None:
        if self.ftp_type == "FTP":
            if not self.FTP:
                raise Exception("FTP connection is not established. Call connect() first.")
        elif self.ftp_type == "SFTP":
            if not self.SFTP:
                raise Exception("SFTP connection is not established. Call connect() first.")
        if file_name is None:
            raise Exception("filename must be provided")
        
        os.makedirs(target_path, exist_ok=True)
        remote_path = f"{source_path.rstrip('/')}/{file_name}"
        local_path = os.path.join(target_path, file_name)

        """檢查本地端是否有檔案，並且記錄檔案大小"""
        exists_local_file: bool = os.path.exists(local_path)
        local_file_size: Optional[int] = None

        if exists_local_file:
            local_file_size = os.path.getsize(local_path)
            print(f"本地檔案大小: {local_file_size} bytes")
        else:
            print(f"本地檔案: {local_path}, 不存在，開始下載")

        """取得遠端檔案大小"""
        if size is None and self.ftp_type == "FTP":
            self.FTP.voidcmd("TYPE I")  # 確保使用二進位模式
            print(f"尚未取得遠端檔案大小，嘗試使用 SIZE 指令取得: {remote_path}")
            try:
                size = self.FTP.size(remote_path)
                print(f"遠端檔案大小: {size} bytes")
            except error_perm:
                self.FTP.cwd(source_path) #切換路徑到來源資料夾，利於後續 size 可以直接使用 name 來 處理每個檔案的大小
                size = self.FTP.size(file_name)
                if size is None:
                    raise Exception(f"無法取得遠端檔案大小: {remote_path}")
        elif size is None and self.ftp_type == "SFTP":
            print(f"尚未取得遠端檔案大小，嘗試使用 stat 指令取得: {remote_path}")
            size = self.SFTP.stat(remote_path).st_size
            if size is None:
                raise Exception(f"無法取得遠端檔案大小: {remote_path}")
        if size:
            remote_file_size = size

        """檢查遠端檔案是否已經完整下載"""
        if exists_local_file and local_file_size == remote_file_size:
            print(f"File '{file_name}'已經完整下載過")
            return file_name

        """如果沒有完整下載，則開始下載"""
        # 斷點續傳位置
        resume_pos = local_file_size if exists_local_file else 0
        writing_mode = "ab" if resume_pos > 0 else "wb"
        blocksize = 64*1024
        if self.ftp_type == "FTP":
            with open(local_path, writing_mode) as f:
                if resume_pos > 0:
                    print(f"從 {resume_pos} bytes 開始續傳")
                    self.FTP.retrbinary(f"RETR {remote_path}", f.write, blocksize=blocksize, rest=resume_pos)
                elif resume_pos == 0:
                    print("開始從頭下載檔案")
                    self.FTP.retrbinary(f"RETR {remote_path}", f.write, blocksize=blocksize)   
        elif self.ftp_type == "SFTP":
                with self.SFTP.file(remote_path, "rb") as rf: #開啟遠端檔案進行讀取
                    if resume_pos > 0:
                        print(f"從 {resume_pos} bytes 開始續傳")
                        rf.seek(resume_pos)
                    else:
                        print("開始從頭下載檔案")
                    with open(local_path, writing_mode) as lf: #開啟本地檔案進行寫入

                        while True:
                            chunk = rf.read(blocksize)
                            if not chunk: 
                                print(f"下載完成: {local_path}")
                                break
                            lf.write(chunk)
                            progress = lf.tell()
                            if remote_file_size > 0:
                                progress_percent = (progress / remote_file_size) * 100
                                print (f"下載進度: ({progress}/{remote_file_size} bytes ({progress_percent:.1f}%)")

        """檢查下載後的檔案大小是否符合預期"""
        final_size = os.path.getsize(local_path)
        if  final_size != remote_file_size:
            raise IOError("下載大小不符")
        print(f"完成下載: {file_name}")
        return file_name

    def uploadFile(self, local_path: str, remote_path: str) -> None:
        try:
            with open(local_path, "rb") as f:
                self.ftp.storbinary(f"STOR {remote_path}", f)
        except Exception as e:
            raise Exception(f"FTP 上傳檔案失敗 ({local_path}): {e}")

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

