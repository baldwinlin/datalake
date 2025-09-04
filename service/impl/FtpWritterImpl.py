#coding:utf-8
'''
FtpWritterImpl.py
Object          : 從Hive table或S3 Object Store取得檔案並上傳至FTP路徑.
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : 1.1 指定Object Store file或(1.2)
                  1.2 指定Hive table並產出檔案，格式如下︰
                      a. 檔案名稱Pattern(ex: aaa*_${date}.CSV)，可置換日期變數
                      b. 可選擇檔案編碼(UTF-8，BIG5...)
                      c. 換行符號(\n, \r\n)
                      d. 可提供表頭
                      e. 分隔符號設定(ex: '|', '!~', '\u0006') 或 固定長度設定
                      f. 檔案可壓縮(可帶密碼)
                  2. 將檔上傳至FTP路徑
Parameters      : 1. FTP config file(FTP Host/FTP path/檔案名稱Pattern/解壓型式/解壓密碼)
                  1.1 Date
                  2. Object Store config file(Object Store path/file name/Access ID/Access Key)
                  3. Hive config file(IP/Port/DB name/table name/分隔符號/固定長度)
Output          :
********************************************************************************
Modify          :
'''
import logging
from logger import Logger
from exception.dataLakeUtilsErrorHandler import dataLakeUtilsErrorHandler
from pathlib import Path
from datetime import datetime
from service.FtpWritter import FtpWritter
from dao.impl.JdbcDaoImpl import JdbcDaoImpl
from crypto.Aes256Crypto import *
from pathlib import Path
import json
from util.Compressor import *
from dao.impl.S3DaoImpl import *
from dao.impl.FtpDaoImpl import *
import os
import tempfile
import jaydebeapi

class FtpWritterImpl(FtpWritter):

    def __init__(self, main_config, fc_config, pc_config, args, sql_file):
        self.fc_config = fc_config
        self.main_config = main_config
        self.args_str = args
        self.sql_file = sql_file
        self.logger_main = None
        self.errorHandler = None
        self.out_file_name = ""
        self.args_dict = json.loads(self.args_str)
        self.log_level = main_config["LOG"].get("LOG_LEVEL", "INFO").upper()

        try:
            temp_path = self.main_config.get('LOG','TEMP_PATH')
            self.temp_path = Path(temp_path) / "fw"
        except Exception as e:
            raise Exception(f"[讀取TEMP path錯誤] {e}")

        #create logger
        try:
            self.log_path = self.main_config.get('LOG','LOG_PATH')
            self.log_name = ""
        except Exception as e:
            raise Exception(f"[讀取LOG path錯誤] {e}")
        self.logger = self.createLog()

        try:
            self.driver_path = self.main_config.get('DB_DRIVER','DRIVER_PATH')
        except Exception as e:
            raise Exception(f"[讀取DB dreiver path錯誤] {e}")


        #確認資料來源S3 or Hive
        self.source_type = fc_config.get('SOURCE', 'TYPE', fallback='')
        if not self.source_type:
            raise Exception(f"functional config[SOURCE][TYPE]不存在")
        elif(self.source_type.lower() == 'db'):
            try:
                self.host = self.fc_config.get('DB', 'HOST')
                self.port = self.fc_config.get('DB', 'PORT')
                self.db_sec_file = self.fc_config.get('DB', 'SEC_FILE')
                self.db_key_file = self.fc_config.get('DB', 'KEY_FILE')
                self.db_name = self.fc_config.get('DB', 'DB_NAME')
                #self.table_name = self.fc_config.get('DB', 'TABLE_NAME')
                self.driver = self.fc_config.get('DB', 'DRIVER')
                self.db_user, self.db_sec_str = readSecFile(self.db_sec_file)
                self.db_salt = readSaltFile(self.db_key_file)
                # self.db_sec = aes256Decrypt(self.sec_str, bytes.fromhex(self.salt))
                self.db_sec = get_gpg_decrypt(self.db_sec_str, self.db_salt)
            except Exception as e:
                raise Exception(f"[讀取DB config錯誤] {e}")
        elif (self.source_type.lower() == 's3'):
            try:
                self.s3_host = fc_config.get('S3', 'HOST')
                self.s3_port = fc_config.get('S3', 'PORT')
                self.s3_bucket = fc_config.get('S3', 'BUCKET')
                self.s3_sec_file = fc_config.get('S3', 'ASSESS_ID_FILE')
                self.s3_key_file = fc_config.get('S3', 'ASSESS_KEY_FILE')
                self.s3_user, self.s3_sec_str = readSecFile(self.s3_sec_file)
                self.s3_salt = readSaltFile(self.s3_key_file)
                self.s3_sec = get_gpg_decrypt(self.s3_sec_str, self.s3_salt)
            except Exception as e:
                raise Exception(f"[讀取S3 config錯誤] {e}")
            self.src_path = fc_config.get('SOURCE', 'PATH', fallback=None)
            self.src_encoding = fc_config.get('SOURCE', 'ENCODING', fallback=None)
            self.src_name_pattern = fc_config.get('SOURCE', 'NAME_PATTERN', fallback=None)
        else:
            raise Exception(f"[Source type {self.source_type} 未定義]")

        #確認資料目的 FTP or S3
        self.target_type = fc_config.get('TARGET', 'TYPE', fallback=None)
        if not self.source_type:
            raise Exception(f"[functional config[TARGET][TYPE]不存在]")
        elif(self.target_type.lower() == 'ftp'):
            try:
                self.ftp_host = fc_config.get('FTP', 'HOST')
                self.ftp_port = fc_config.getint('FTP', 'PORT')
                self.ftp_sec_file = fc_config.get('FTP', 'SEC_FILE')
                self.ftp_key_file = fc_config.get('FTP', 'KEY_FILE')
                self.ftp_type = fc_config.get('FTP', 'FTP_TYPE')
                self.ftp_user, self.ftp_sec_str = readSecFile(self.ftp_sec_file)
                self.ftp_salt = readSaltFile(self.ftp_key_file)
                self.ftp_sec = get_gpg_decrypt(self.ftp_sec_str, self.ftp_salt)
            except Exception as e:
                raise Exception(f"[讀取FTP config錯誤] {e}")
        elif (self.target_type.lower() == 's3'):
            try:
                self.s3_host = fc_config.get('S3', 'HOST')
                self.s3_port = fc_config.get('S3', 'PORT')
                self.s3_bucket = fc_config.get('S3', 'BUCKET')
                self.s3_sec_file = fc_config.get('S3', 'ASSESS_ID_FILE')
                self.s3_key_file = fc_config.get('S3', 'ASSESS_KEY_FILE')
                self.s3_user, self.s3_sec_str = readSecFile(self.s3_sec_file)
                self.s3_salt = readSaltFile(self.s3_key_file)
                self.s3_sec = get_gpg_decrypt(self.s3_sec_str, self.s3_salt)
            except Exception as e:
                raise Exception(f"[讀取S3 config錯誤] {e}")
        else:
            raise Exception(f"[Target type {self.target_type} 未定義]")

        #Get config [TARGET] section
        self.tg_type = fc_config.get('TARGET', 'TYPE', fallback=None)
        self.tg_path = fc_config.get('TARGET', 'PATH', fallback=None)
        self.tg_name_pattern = fc_config.get('TARGET', 'NAME_PATTERN', fallback=None)
        self.tg_delimiter = fc_config.get('TARGET', 'DELIMITER', fallback=None)
        self.tg_col_size_file = fc_config.get('TARGET', 'COL_SIZE_FILE', fallback=None)
        self.tg_new_line_character = "\n" if fc_config.get('TARGET', 'NEW_LINE_CHARACTER', fallback=None) == "\\n" else "\r\n"
        self.tg_encoding = fc_config.get('TARGET', 'ENCODING', fallback=None)
        self.tg_col_size_file = fc_config.get('TARGET', 'COL_SIZE_FILE', fallback=None)
        self.tg_header = fc_config.get('TARGET', 'HEADER', fallback=None)

        # Get config [ZIP] section
        self.zip_type = fc_config.get('ZIP', 'ZIP_TYPE', fallback=None)
        self.zip_sec_file = fc_config.get('ZIP', 'SEC_FILE', fallback=None)
        self.zip_key_file = fc_config.get('ZIP', 'KEY_FILE', fallback=None)
        self.zip_sec = None
        if (self.zip_sec_file):
            self.zip_user, self.zip_sec_str = readSecFile(self.zip_sec_file)
            self.zip_salt = readSaltFile(self.zip_key_file)
            self.zip_sec = get_gpg_decrypt(self.zip_sec_str, self.zip_salt)

    def setLog(self, logger_main, errorHandler):
        self.logger_main = logger_main
        self.errorHandler = errorHandler

    def createLog(self):
        log_path = Path(self.log_path)
        sql_file = Path(self.sql_file)

        # 取得 sql 檔名（不含副檔名）
        filename = sql_file.stem  # create_01
        self.sql_file_name = sql_file.name

        # 日期
        today = datetime.today().strftime("%Y%m%d")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        #組合log name
        log_name = f"{filename}_{timestamp}"

        # 取得 sql 檔所在的子目錄名稱 (ddl)
        subdir = sql_file.parent.name

        # 建立 log 完整路徑
        log_dir = log_path / "fw"
        Logger.Logger(log_dir, log_name)  # 模組日誌
        return logging.getLogger(log_name)

    def connectDb(self):
        if (self.driver.lower() == 'hive2'):
            driver_class = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
            jdbc_url = f"jdbc:hive2://{self.host}:{self.port}/{self.db_name};auth=LDAP"
            driver_jar = f"{self.driver_path}/kyuubi-hive-jdbc-shaded-1.10.2.jar"
        elif (self.driver.lower() == 'mysql'):
            driver_class = "com.mysql.cj.jdbc.Driver"
            jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.db_name}"
            driver_jar = f"{self.driver_path}\\mysql-connector-j-9.4.0.jar"

        dao = JdbcDaoImpl(driver_class, jdbc_url, self.db_user, self.db_sec, driver_jar)

        try:
            dao.connect()
            self.logger.info('[資料庫連線完成]')
            return dao
        except Exception as e:
            self.logger.error(f'[資料庫連線失敗] {e}')
            self.errorHandler.exceptionWriter(f"[連線資料庫錯誤] {e}")
            exit(1)

    def exportFile(self, conn, sql_str, output_file, delimiter=",", new_line_ch="\n", encoding="utf-8"):
        try:

            cursor = conn.cursor()

            # 查詢所有資料
            try:
                cursor.execute(sql_str)
                rows = cursor.fetchall()
            except Exception as e:
                self.logger.error(f'[執行SQL失敗] {e}')
                self.errorHandler.exceptionWriter(f"[執行SQL失敗] {e}")
                exit(1)
            # 取得欄位名稱
            col_names = [desc[0] for desc in cursor.description]

            cnt = 0
            with open(output_file, "w", encoding=encoding, errors="replace", newline="") as f:
                # 先寫欄位名稱
                if(self.tg_header.lower() == 'y'):
                    f.write(delimiter.join(col_names) + new_line_ch)
                # 再寫每一筆資料
                for row in rows:
                    f.write(delimiter.join(str(v) if v is not None else "" for v in row) + new_line_ch)
                    cnt += 1

            self.logger.info(f'[產出檔案成功] {output_file}')
            self.logger.info(f'[產出筆數: {cnt}] ')

        except Exception as e:
            self.logger.error(f'[產出檔案錯誤] {e}')
            self.errorHandler.exceptionWriter(f"[產出檔案錯誤] {e}")
            exit(1)

        cursor.close()

    def formatField(self, value, length, dtype="str", encoding="big5"):
        """格式化欄位為固定長度 (byte)。"""
        sign_str = ""
        if value is None:
            value_str = ""
        elif dtype in ('num', 'fload') and value < 0:
            value_str = str(-value)
            sign_str = "-"
        else:
            value_str = str(value)

        raw_bytes = value_str.encode(encoding, errors="replace")

        if len(raw_bytes) > length:
            raw_bytes = raw_bytes[:length]
        else:
            pad_len = length - len(raw_bytes)
            if dtype in ("num", "float"):
                pad_byte = "0".encode(encoding)
                if sign_str:
                    raw_bytes = b"-" + pad_byte * (pad_len - 1) + raw_bytes
                else:
                    raw_bytes = pad_byte * pad_len + raw_bytes
            else:  # str
                pad_byte = " ".encode(encoding)
                raw_bytes = raw_bytes + pad_byte * pad_len

        return raw_bytes

    def exportFixedLengthFile(self, conn, sql, output_file, field_lengths, line_ending="\n", encoding="big5"):
        """
        從資料庫讀取資料，輸出固定長度檔案 (含欄位名稱 Header)。
        :param sql: 查詢語法
        :param output_file: 輸出檔案路徑
        :param field_lengths: 欄位長度list
        :param line_ending: 換行符號
        :param encoding: 輸出編碼
        """
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Exception as e:
            self.logger.error(f'[執行SQL失敗] {e}')
            self.errorHandler.exceptionWriter(f"[執行SQL失敗] {e}")
            exit(1)

        # 欄位資訊
        col_info = cursor.description  # [(name, type, ...), ...]
        conn.close()

        # 自動判斷型別
        def detect_dtype(db_type):
            if db_type == jaydebeapi.STRING:
                return "str"  # sqlite 無明確型別，預設文字
            elif db_type == jaydebeapi.NUMBER:
                return "num"
            elif db_type == jaydebeapi.FLOAT:
                return "float"
            else:
                return "str"

        col_meta = []
        for i, col in enumerate(col_info):
            col_name = col[0]
            col_length = field_lengths[i]
            dtype = detect_dtype(col[1])
            col_meta.append((col_name, col_length, dtype))

        try:
            with open(output_file, "wb") as f:
                # 輸出 Header
                if(self.tg_header.lower() == 'y'):
                    header_bytes = b""
                    for col_name, col_length, _ in col_meta:
                        header_bytes += self.formatField(col_name, col_length, "str", encoding)
                    f.write(header_bytes + line_ending.encode(encoding))

                # 輸出資料列
                cnt = 0
                for row in rows:
                    line_bytes = b""
                    for idx, (col_name, col_length, dtype) in enumerate(col_meta):
                        line_bytes += self.formatField(row[idx], col_length, dtype, encoding)
                    #print("out: ", line_bytes, len(line_bytes))
                    f.write(line_bytes + line_ending.encode(encoding))
                    cnt += 1

            self.logger.info(f'[產出檔案成功] {output_file}')
            self.logger.info(f'[產出筆數: {cnt}] ')
        except Exception as e:
            self.logger.error(f'[產出檔案錯誤] {e}')
            self.errorHandler.exceptionWriter(f"[產出檔案錯誤] {e}")
            exit(1)


    def readSqlFile(self):
        # Read SQL file to SQL string
        self.logger.info(f"SQL file = {self.sql_file}")
        self.logger.info(f"SQL args = {self.args_str}")
        try:
            with open(self.sql_file, "r") as file:
                sql_str = file.read()
        except Exception as e:  # Catching a more general exception for demonstration
            self.logger.error(f"[讀取SQL file錯誤] {e}")
            self.errorHandler.exceptionWriter(f"[讀取SQL file錯誤] {e}")
            exit(1)

        return self.replaceArg(sql_str)


    def replaceArg(self, src_string):
        if (self.args_str is not None):
            for key, value in self.args_dict.items():
                src_string = src_string.replace(key, value)
        return src_string


    def exportDbFile(self):
        try:
            dao = self.connectDb()
        except Exception as e:
            self.logger.error(f'[資料庫連線失敗] {e}')
            self.errorHandler.exceptionWriter(f"[連線資料庫錯誤] {e}")
            exit(1)
        sql_str = self.readSqlFile()
        self.logger.debug(f'[執行SQL]\n{sql_str}')
        out_file = self.temp_path / self.replaceArg(self.tg_name_pattern)
        if (self.tg_delimiter):  # Export file with delimiter
            self.exportFile(dao.conn, sql_str, out_file, self.tg_delimiter, self.tg_new_line_character,
                            self.tg_encoding)

        elif (self.tg_col_size_file):  # Export file with fixed field length
            with open(self.tg_col_size_file, "r", encoding="utf-8") as f:
                fields_lens = [int(line.strip()) for line in f if line.strip()]
            self.exportFixedLengthFile(dao.conn, sql_str, out_file, fields_lens, self.tg_new_line_character,
                                       self.tg_encoding)
        else:
            self.logger.error(f'[分隔符號或固定欄寬未定義]')
            self.errorHandler.exceptionWriter(f"[分隔符號或固定欄寬未定義]")
            exit(1)
        return out_file

    def getS3Files(self):
        search_key = self.src_path + self.replaceArg(self.src_name_pattern)
        self.logger.debug(f'[Search S3 file with key {search_key}]')
        try:
            s3SrcDao = S3DaoImpl(self.s3_bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
        except Exception as e:
            self.logger.error(f'[S3連線失敗] {e}')
            self.errorHandler.exceptionWriter(f"[S3連線失敗]")
            exit(1)

        filelist = s3SrcDao.listFiles(search_key)
        self.logger.debug(f'[Source file list ] {filelist}')

        download_files = []
        for file in filelist:
            remote_path = self.temp_path / Path(file).name
            self.logger.debug(f"[Download file] {file} -> {remote_path}")
            download_files.append(remote_path)
            try:
                s3SrcDao.downloadFile(file, remote_path)
            except Exception as e:
                self.logger.error(f'[S3下載失敗] {e}')
                self.errorHandler.exceptionWriter(f"[S3下載失敗]")
                exit(1)
            line_cnt = self.convertEncoding(remote_path, self.tg_encoding, self.src_encoding)
            self.logger.info(f'[檔案筆數] {remote_path} : {line_cnt}')

        return download_files

    def convertEncoding(self, file_path: str, target_encoding: str, source_encoding: str = "utf-8"):
        """
        將檔案從 source_encoding 轉換成 target_encoding，逐行處理，最後覆蓋原檔案
        :param file_path: 原始檔案路徑
        :param target_encoding: 目標編碼 (例如 'big5', 'utf-8')
        :param source_encoding: 原始檔案編碼 (預設 utf-8)
        """
        # 建立暫存檔
        if (target_encoding != source_encoding):
            try:
                tmp_path = file_path.with_suffix("." + "tmp")
                tmp_file = open(tmp_path, mode="w", encoding=target_encoding, errors="replace")
            except Exception as e:
                self.logger.error(f'[開啟暫存檔失敗] {e}')
                self.errorHandler.exceptionWriter(f"[開啟暫存檔失敗] {e}")
                exit(1)

        line_cnt = 0
        try:
            with open(file_path, "r", encoding=source_encoding, errors="replace") as src:
                for line in src:
                    if (target_encoding != source_encoding):
                        tmp_file.write(line)
                    line_cnt += 1
        except Exception as e:
            self.logger.error(f'[檔案轉碼失敗] {e}')
            self.errorHandler.exceptionWriter(f"[檔案轉碼失敗] {e}")
            exit(1)

        # 替換原檔案
        if (target_encoding != source_encoding):
            tmp_file.close()
            os.replace(tmp_path, file_path)
        return line_cnt

    def uploadFile(self, upload_file):
        if (self.tg_type.lower() == 'ftp'):
            # print("FTP: ",self.ftp_type, self.ftp_host, self.ftp_port, self.ftp_user, self.ftp_sec)
            try:
                ftpDao = FtpDaoImpl(self.ftp_type, self.ftp_host, self.ftp_port, self.ftp_user, self.ftp_sec)
            except Exception as e:
                self.logger.error(f'[FTP連線失敗] {e}')
                self.errorHandler.exceptionWriter(f"[FTP連線錯誤] {e}")
                exit(1)
            try:
                ftpDao.uploadFile(upload_file, self.tg_path)
                self.logger.info(f"[上傳檔案至FTP成功] {upload_file}")
            except Exception as e:
                self.logger.error(f'[FTP上傳失敗] {e}')
                self.errorHandler.exceptionWriter(f"[FTP上傳失敗] {e}")
                exit(1)
        elif (self.tg_type.lower() == 's3'):
            try:
                target_path = self.tg_path + Path(upload_file).name
                try:
                    s3Dao = S3DaoImpl(self.s3_bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
                except Exception as e:
                    self.logger.error(f'[S3連線失敗] {e}')
                    self.errorHandler.exceptionWriter(f"[S3連線失敗]")
                    exit(1)
                self.logger.debug(f'[上傳檔案: {upload_file} -> {target_path}')
                s3Dao.uploadFile(upload_file, target_path)
                self.logger.info(f'[上傳檔案至S3成功] {upload_file}')
            except Exception as e:
                self.logger.error(f'[上傳檔案至S3失敗] {e}')
                self.errorHandler.exceptionWriter(f"[上傳檔案至S3失敗] {e}")
                exit(1)


    def run(self):
        self.logger.setLevel(getattr(logging, self.log_level, logging.INFO))
        self.logger.info("[Run FTP writter]")
        filelist = []
        upload_file = None
        if(self.source_type.lower() == "db"):
            out_file = self.exportDbFile()
            upload_file = out_file
            filelist.append(str(out_file))
        elif(self.source_type.lower() == "s3"):
            out_file = self.temp_path / self.replaceArg(self.tg_name_pattern)
            filelist = self.getS3Files()
        else:
            self.logger.error(f"[Source type未定義]")
            self.errorHandler.exceptionWriter(f"[Source type未定義]")
            exit(1)


        #Compress data and upload files
        if(self.zip_type):
            #out_zip_file = out_file + "." + self.zip_type.lower()
            out_zip_file = str(out_file) + "." + self.zip_type.lower()
            self.logger.debug(f"[Compress file] {out_zip_file}")
            try:
                Compressor.compress(str(out_zip_file), filelist, self.zip_sec)
            except Exception as e:
                self.logger.error(f'[壓縮檔案失敗] {e}')
                self.errorHandler.exceptionWriter(f"[壓縮檔案失敗] {e}")
                exit(1)
            self.uploadFile(out_zip_file)
        else:
            for upload_file in filelist:
                self.uploadFile(upload_file)




