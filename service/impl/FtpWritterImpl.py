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

class FtpWritterImpl(FtpWritter):

    def __init__(self, main_config, fc_config, args, sql_file):
        self.fc_config = fc_config
        self.main_config = main_config
        self.args_str = args
        self.sql_file = sql_file
        self.logger_main = None
        self.errorHandler = None
        self.out_file_name = ""

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
            pass
        elif (self.source_type.lower() == 'ftp'):
            pass
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

        try:
            self.temp_path = self.main_config.get('LOG','TEMP_PATH')
        except Exception as e:
            raise Exception(f"[讀取TEMP path錯誤] {e}")

        #Get config [TARGET] section
        self.tg_type = fc_config.get('TARGET', 'TYPE', fallback=None)
        self.tg_path = fc_config.get('TARGET', 'PATH', fallback=None)
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

        #組合log name及 out file name
        self.out_file_name = f"{filename}_{timestamp}"
        log_name = f"{filename}_{today}"

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

            with open(output_file, "w", encoding=encoding, newline="") as f:
                # 先寫欄位名稱
                if(self.tg_header.lower() == 'y'):
                    f.write(delimiter.join(col_names) + new_line_ch)
                # 再寫每一筆資料
                for row in rows:
                    f.write(delimiter.join(str(v) if v is not None else "" for v in row) + new_line_ch)

            self.logger.info(f'[產出檔案成功] {output_file}')

        except Exception as e:
            self.logger.error(f'[產出檔案錯誤] {e}')
            self.errorHandler.exceptionWriter(f"[產出檔案錯誤] {e}")
            exit(1)

        cursor.close()

    def exportFixedLengthFile(self, conn, sql_str, output_file, field_lengths, line_ending="\n", encoding="utf-8"):
        """
        將資料表輸出為定長檔案
        :param field_lengths: list，每個欄位的長度 (ex: [5, 6, 10])
        :param pad_char: 補齊用字元，預設空白，可改成 "0"
        """
        pad_char = " "
        try:
            cursor = conn.cursor()

            cursor.execute(sql_str)
            rows = cursor.fetchall()
        except Exception as e:
            self.logger.error(f'[執行SQL失敗] {e}')
            self.errorHandler.exceptionWriter(f"[執行SQL失敗] {e}")
            exit(1)

        try:
            col_names = [desc[0] for desc in cursor.description]

            col_count = len(field_lengths)
            if len(cursor.description) != col_count:
                raise ValueError(f"[欄位數({len(cursor.description)}) 與定長規格({col_count}) 不一致！]")

            with open(output_file, "w", encoding=encoding, newline="") as f:
                if(self.tg_header.lower() == 'y'):
                    line = ""
                    for i, value in enumerate(col_names):
                        text = str(value) if value is not None else ""
                        fixed_text = text[:field_lengths[i]].ljust(field_lengths[i], pad_char)
                        line += fixed_text
                    f.write(line + line_ending)
                for row in rows:
                    line = ""
                    for i, value in enumerate(row):
                        text = str(value) if value is not None else ""
                        # 若超過長度，截斷；若不足，補 pad_char
                        fixed_text = text[:field_lengths[i]].ljust(field_lengths[i], pad_char)
                        line += fixed_text
                    f.write(line + line_ending)

            self.logger.info(f'[產出檔案成功] {output_file}')

        except Exception as e:
            self.logger.error(f'[產出檔案錯誤] {e}')
            self.errorHandler.exceptionWriter(f"[產出檔案錯誤] {e}")
            exit(1)

        cursor.close()

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

        # Replace SQL arguments
        if (self.args_str is not None):
            args_dict = json.loads(self.args_str)

            for key, value in args_dict.items():
                sql_str = sql_str.replace(key, value)

        return sql_str

    def run(self):
        self.logger.info("[Run FTP writter]")
        try:
            dao = self.connectDb()
        except Exception as e:
            self.logger.error(f'[資料庫連線失敗] {e}')
            self.errorHandler.exceptionWriter(f"[連線資料庫錯誤] {e}")
            exit(1)

        sql_str = self.readSqlFile()
        self.logger.info(f'[執行SQL]\n{sql_str}')


        out_file = self.temp_path + self.out_file_name
        upload_file = out_file
        if(self.tg_delimiter):    # Export file with delimiter
            self.exportFile(dao.conn, sql_str, out_file, self.tg_delimiter, self.tg_new_line_character, self.tg_encoding)

        elif(self.tg_col_size_file):  #Export file with fixed field length
            with open(self.tg_col_size_file, "r", encoding="utf-8") as f:
                fields_lens = [int(line.strip()) for line in f if line.strip()]
            self.exportFixedLengthFile(dao.conn, sql_str, out_file, fields_lens, self.tg_new_line_character,
                                       self.tg_encoding)
        else:
            self.logger.error(f'[分隔符號或固定欄寬未定義]')
            self.errorHandler.exceptionWriter(f"[分隔符號或固定欄寬未定義]")
            exit(1)

        filelist = []
        filelist.append(out_file)
        #print(filelist)
        out_zip_file = None
        #Compress data
        if(self.zip_sec_file):
            self.zip_user, self.zip_sec_str = readSecFile(self.zip_sec_file)
            self.zip_salt = readSaltFile(self.zip_key_file)
            self.zip_sec = get_gpg_decrypt(self.zip_sec_str, self.zip_salt)

        if(self.zip_type):
            out_zip_file = out_file + "." + self.zip_type.lower()
            Compressor.compress(out_zip_file, filelist, self.zip_sec)
            upload_file = out_zip_file

        #Upload file
        if(self.tg_type.lower() == 'ftp'):
            #print("FTP: ",self.ftp_type, self.ftp_host, self.ftp_port, self.ftp_user, self.ftp_sec)
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
        elif(self.tg_type.lower() == 's3'):
            try:
                target_path = Path(upload_file).name
                s3Dao = S3DaoImpl(self.s3_bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
                s3Dao.uploadFile(upload_file, target_path)
                self.logger.info(f'[上傳檔案至S3成功] {upload_file}')
            except Exception as e:
                self.logger.error(f'[上傳檔案至S3失敗] {e}')
                self.errorHandler.exceptionWriter(f"[上傳檔案至S3失敗] {e}")
                exit(1)
