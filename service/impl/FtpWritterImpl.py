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
from re import S
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
        self.args_dict = None
        if self.args_str:
            self.args_dict = json.loads(self.args_str)
        self.log_level = main_config["LOG"].get("LOG_LEVEL", "INFO").upper()


        #Get temp path
        try:
            temp_path = self.main_config.get('LOG','TEMP_PATH')
            self.temp_path = Path(temp_path) / "fw"
            if not self.temp_path.exists():
                self.temp_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise Exception(f"[讀取TEMP path錯誤] {e}")

        try:
            self.driver_path = self.main_config.get('DB_DRIVER','DRIVER_PATH')
        except Exception as e:
            raise Exception(f"[讀取DB dreiver path錯誤] {e}")

        self.log_prefix = pc_config.get('LOG', 'LOG_PREFIX', fallback='ftpwritter_')

        #確認資料來源S3 or Hive
        self.source_type = pc_config.get('SOURCE', 'TYPE', fallback='')
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
            self.work_sub_dir = pc_config.get('SOURCE', 'WORK_SUB_DIR', fallback=None)
        elif (self.source_type.lower() == 'dbfile'):
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

            try:
                self.s3_host = fc_config.get('S3', 'HOST')
                self.s3_port = fc_config.get('S3', 'PORT')
                self.s3_sec_file = fc_config.get('S3', 'ASSESS_ID_FILE')
                self.s3_key_file = fc_config.get('S3', 'ASSESS_KEY_FILE')
                self.s3_user, self.s3_sec_str = readSecFile(self.s3_sec_file)
                self.s3_salt = readSaltFile(self.s3_key_file)
                self.s3_sec = get_gpg_decrypt(self.s3_sec_str, self.s3_salt)
            except Exception as e:
                raise Exception(f"[讀取S3 config錯誤] {e}")
            self.src_path = pc_config.get('SOURCE', 'PATH', fallback=None)
            self.src_bucket = pc_config.get('SOURCE', 'BUCKET', fallback=None)
            self.src_delimiter = pc_config.get('SOURCE', 'DELIMITER', fallback=None)
            if self.src_delimiter:
                self.src_delimiter = self.getCorrectDelimiter(self.src_delimiter)
            self.src_encoding = pc_config.get('SOURCE', 'ENCODING', fallback='utf-8')
            self.src_name_pattern = pc_config.get('SOURCE', 'NAME_PATTERN', fallback=None)
            self.src_db_name = pc_config.get('SOURCE', 'DB_NAME', fallback=None)
            self.src_table_name = pc_config.get('SOURCE', 'TABLE_NAME', fallback=None)
            self.work_sub_dir = pc_config.get('SOURCE', 'WORK_SUB_DIR', fallback=None)


        elif (self.source_type.lower() == 's3'):
            try:
                self.s3_host = fc_config.get('S3', 'HOST')
                self.s3_port = fc_config.get('S3', 'PORT')
                self.s3_sec_file = fc_config.get('S3', 'ASSESS_ID_FILE')
                self.s3_key_file = fc_config.get('S3', 'ASSESS_KEY_FILE')
                self.s3_user, self.s3_sec_str = readSecFile(self.s3_sec_file)
                self.s3_salt = readSaltFile(self.s3_key_file)
                self.s3_sec = get_gpg_decrypt(self.s3_sec_str, self.s3_salt)
            except Exception as e:
                raise Exception(f"[讀取S3 config錯誤] {e}")
            self.src_path = pc_config.get('SOURCE', 'PATH', fallback=None)
            self.src_bucket = pc_config.get('SOURCE', 'BUCKET', fallback=None)
            self.src_encoding = pc_config.get('SOURCE', 'ENCODING', fallback='utf-8')
            self.src_name_pattern = pc_config.get('SOURCE', 'NAME_PATTERN', fallback=None)
            self.work_sub_dir = pc_config.get('SOURCE', 'WORK_SUB_DIR', fallback='')
        else:
            raise Exception(f"[Source type {self.source_type} 未定義]")

        #Get work path
        if self.work_sub_dir:
            self.work_path = self.temp_path / self.work_sub_dir
        else:
            self.work_path = self.temp_path
        if not self.work_path.exists():
            self.work_path.mkdir(parents=True, exist_ok=True)

        #確認資料目的 FTP or S3
        self.target_type = pc_config.get('TARGET', 'TYPE', fallback=None)
        if not self.target_type:
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
        self.tg_type = pc_config.get('TARGET', 'TYPE', fallback=None)
        self.tg_path = pc_config.get('TARGET', 'PATH', fallback=None)
        self.tg_name_pattern = pc_config.get('TARGET', 'NAME_PATTERN', fallback=None)
        self.tg_delimiter = pc_config.get('TARGET', 'DELIMITER', fallback=None)
        if(self.tg_delimiter):
            self.tg_delimiter = self.getCorrectDelimiter(self.tg_delimiter)
        self.tg_col_size_file = pc_config.get('TARGET', 'COL_SIZE_FILE', fallback=None)
        self.tg_new_line_character = "\n" if pc_config.get('TARGET', 'NEW_LINE_CHARACTER', fallback=None) == "\\n" else "\r\n"
        self.tg_encoding = pc_config.get('TARGET', 'ENCODING', fallback='utf-8')
        self.tg_col_size_file = pc_config.get('TARGET', 'COL_SIZE_FILE', fallback=None)
        self.tg_header = pc_config.get('TARGET', 'HEADER', fallback='N')
        self.tg_bucket = pc_config.get('TARGET', 'BUCKET', fallback=None)
        self.tg_ctl_file = pc_config.get('TARGET', 'CTL_FILE', fallback='N')
        self.tg_ctl_file_name_pattern = pc_config.get('TARGET', 'CTL_FILE_NAME_PATTERN', fallback=None)
        self.tg_ctl_chinese= pc_config.get('TARGET', 'CHINESE-SW', fallback='Y')

        # Get config [ZIP] section
        self.zip_type = pc_config.get('ZIP', 'ZIP_TYPE', fallback=None)
        self.zip_sec_file = pc_config.get('ZIP', 'SEC_FILE', fallback=None)
        self.zip_key_file = pc_config.get('ZIP', 'KEY_FILE', fallback=None)
        self.zip_sec = None
        if (self.zip_sec_file):
            self.zip_user, self.zip_sec_str = readSecFile(self.zip_sec_file)
            self.zip_salt = readSaltFile(self.zip_key_file)
            self.zip_sec = get_gpg_decrypt(self.zip_sec_str, self.zip_salt)

        # create logger
        try:
            self.log_path = self.main_config.get('LOG', 'LOG_PATH')
            self.log_name = ""
        except Exception as e:
            raise Exception(f"[讀取LOG path錯誤] {e}")
        self.logger = self.createLog()

    def getKeyValue(self, data_dict: dict, target_key: str):
        """
        以不區分大小寫的方式，從字典中尋找包含目標鍵子字串的鍵。

        Args:
            data_dict (dict): 來源字典。
            target_key (str): 目標鍵（子字串）。

        Returns:
            The value associated with the key, or None if no matching key is found.
        """
        target_key_lower = target_key.lower()
        for key, value in data_dict.items():
            if target_key_lower in key.lower():
                return value
        return None

    def getCorrectDelimiter(selr, delimiter_str):
        """
        將表示 Unicode 逸出序列的字串轉換為實際的分隔符號。

        Args:
          delimiter_str: 原始的分隔符號字串，可能包含 ',\'、'~!' 或 '\u0006' 或 '\t'。

        Returns:
          正確的分隔符號字串。
        """
        if isinstance(delimiter_str, str) and ('\\u' in delimiter_str or '\\t' in delimiter_str):
            try:
                # 將 '\\u0006' or '\\t' 轉換為正確的code
                return delimiter_str.encode().decode('unicode_escape')
            except UnicodeDecodeError:
                # 如果轉換失敗，則返回原始字串
                return delimiter_str
        else:
            # 如果沒有 '\\u'，則直接返回原始字串
            return delimiter_str

    def setLog(self, logger_main, errorHandler):
        self.logger_main = logger_main
        self.errorHandler = errorHandler

    def createLog(self):
        log_path = Path(self.log_path)
        # 日期
        today = datetime.today().strftime("%Y%m%d")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        #組合log name
        log_name = f"{self.log_prefix}{timestamp}"

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
            self.errorExit(f'[資料庫連線失敗] {e}')


    def exportFile(self, conn, sql_str, output_file, delimiter=",", new_line_ch="\n", encoding="utf-8"):
        """
        從資料庫中查詢資料並匯出到檔案，採用分批讀取以避免記憶體溢出。

        Args:
            conn: 資料庫連線物件。
            sql_str: 要執行的 SQL 查詢字串。
            output_file: 輸出檔案的路徑。
            delimiter: 欄位分隔符號，預設為逗號。
            new_line_ch: 換行字元，預設為 \n。
            encoding: 檔案編碼，預設為 utf-8。
        Returns:
            int: 實際寫入檔案的資料筆數。
        """
        chunk_size = 10000 #每次從資料庫讀取的資料筆數

        cnt = 0
        cursor = None
        try:
            cursor = conn.cursor()

            try:
                cursor.execute(sql_str)
            except Exception as e:
                self.errorExit(f'[執行SQL失敗] {e}')

            # 取得欄位名稱
            col_names = [desc[0] for desc in cursor.description]

            with open(output_file, "w", encoding=encoding, errors="replace", newline="") as f:
                # 先寫欄位名稱
                if(self.tg_header.lower() == 'y'):
                    f.write(delimiter.join(col_names) + new_line_ch)

                # 採用分批讀取的方式
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break  # 如果沒有更多資料，就結束迴圈

                    # 寫入每一筆資料
                    for row in rows:
                        f.write(delimiter.join(str(v) if v is not None else "" for v in row) + new_line_ch)
                        cnt += 1

            self.logger.info(f'[產出檔案成功] {output_file}')
            self.logger.info(f'[產出筆數: {cnt}] ')
            return cnt

        except Exception as e:
            self.errorExit(f'[產出檔案錯誤] {e}')
        finally:
            if cursor:
                cursor.close()


    def formatField(self, value, length, dtype="str", encoding="utf-8"):
        """格式化欄位為固定長度 (byte)。"""
        sign_str = ""
        value_str = str(value)
        if value is None or value == '':
            value_str = ""
        elif dtype == 'num' and value_str.startswith('-'):
            value_str = value_str[1:]
            sign_str = "-"
        elif dtype == 'float' and value_str.startswith('-'):
            value_str = value_str[1:]
            sign_str = "-"

        raw_bytes = value_str.encode(encoding, errors="replace")

        if len(raw_bytes) > length:
            raw_bytes = raw_bytes[:length]
        else:
            pad_len = length - len(raw_bytes)
            if dtype in ("num"):
                pad_byte = "0".encode(encoding)
                if sign_str:
                    raw_bytes = b"-" + pad_byte * (pad_len - 1) + raw_bytes
                else:
                    raw_bytes = pad_byte * pad_len + raw_bytes
            elif dtype in ("float"):
                pad_byte = "0".encode(encoding)
                if sign_str:
                    raw_bytes = b"-" + pad_byte * (pad_len - 1) + raw_bytes
                else:
                    raw_bytes = pad_byte * pad_len + raw_bytes
            else:  # str
                pad_byte = " ".encode(encoding)
                raw_bytes = raw_bytes + pad_byte * pad_len

        return raw_bytes

    def exportFixedLengthFile(self, conn, sql, output_file, field_lengths, line_ending="\n", encoding="utf-8"):
        """
        從資料庫讀取資料，輸出固定長度檔案 (含欄位名稱 Header)，採用分批讀取。
        :param sql: 查詢語法
        :param output_file: 輸出檔案路徑
        :param field_lengths: 欄位長度list
        :param line_ending: 換行符號
        :param encoding: 輸出編碼
        """
        chunk_size = 10000 #每次從資料庫讀取的資料筆數
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
        except Exception as e:
            self.errorExit(f"[執行SQL失敗] {e}")

        # 欄位資訊
        col_info = cursor.description  # (name, type_code, display_size, internal_size, precision, scale, null_ok)

        # 自動判斷型別
        def detect_dtype(db_type):
            if db_type == jaydebeapi.STRING:
                return "str"
            elif db_type == jaydebeapi.NUMBER:
                return "num"
            elif db_type == jaydebeapi.FLOAT:
                return "float"
            else:
                return "str"

        def detectDtype(db_type):
            if 'CHAR' in db_type:
                return 'str'
            elif 'INTEGER' in db_type:
                return 'num'
            elif 'FLOAT' in db_type:
                return 'float'
            elif 'DECIMAL' in db_type:
                return 'float'
            else:
                return 'str'

        col_meta = []
        for i, col in enumerate(col_info):
            col_name = col[0]
            col_length = field_lengths[i]
            #dtype = detect_dtype(col[1])
            dtype = detectDtype(str(col[1]))
            self.logger.debug("[{}] {}".format(col_name, str(col[1])))
            col_meta.append((col_name, col_length, dtype))

        try:
            with open(output_file, "wb") as f:
                # 輸出 Header
                if self.tg_header.lower() == 'y':
                    header_bytes = b""
                    for col_name, col_length, _ in col_meta:
                        header_bytes += self.formatField(col_name, col_length, "str", encoding)
                    f.write(header_bytes + line_ending.encode(encoding))

                # 輸出資料列
                cnt = 0
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break  # 如果沒有更多資料，就結束迴圈

                    for row in rows:
                        line_bytes = b""
                        for idx, (col_name, col_length, dtype) in enumerate(col_meta):
                            line_bytes += self.formatField(row[idx], col_length, dtype, encoding)
                        f.write(line_bytes + line_ending.encode(encoding))
                        cnt += 1

            self.logger.info(f'[產出檔案成功] {output_file}')
            self.logger.info(f'[產出筆數: {cnt}] ')
            return cnt
        except Exception as e:
            self.errorExit(f'[產出檔案錯誤] {e}')
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    def readSqlFile(self):
        # Read SQL file to SQL string
        self.logger.info(f"SQL file = {self.sql_file}")
        self.logger.info(f"SQL args = {self.args_str}")
        try:
            with open(self.sql_file, "r") as file:
                sql_str = file.read()
        except Exception as e:  # Catching a more general exception for demonstration
            self.errorExit(f"[讀取SQL file錯誤] {e}")

        return self.replaceArg(sql_str)


    def replaceArg(self, src_string):
        if (self.args_str is not None and src_string is not None):
            for key, value in self.args_dict.items():
                src_string = src_string.replace(key, value)
        return src_string


    def exportDbFile(self):
        try:
            dao = self.connectDb()
        except Exception as e:
            self.errorExit(f'[資料庫連線失敗] {e}')
        sql_str = self.readSqlFile()
        self.logger.debug(f'[執行SQL]\n{sql_str}')
        out_file = self.temp_path / self.replaceArg(self.tg_name_pattern)

        process_cnt = 0
        if (self.tg_delimiter):  # Export file with delimiter
            process_cnt = self.exportFile(dao.conn, sql_str, out_file, self.tg_delimiter,
                            self.tg_new_line_character, self.tg_encoding)

        elif (self.tg_col_size_file):  # Export file with fixed field length
            with open(self.tg_col_size_file, "r", encoding="utf-8") as f:
                fields_lens = [int(line.strip()) for line in f if line.strip()]
            process_cnt = self.exportFixedLengthFile(dao.conn, sql_str, out_file, fields_lens,
                                       self.tg_new_line_character, self.tg_encoding)
        else:
            self.errorExit(f'[分隔符號或固定欄寬未定義]')
        ctl_file = None
        if(self.tg_ctl_file.lower() == 'y'):
            ctl_file = self.temp_path / self.replaceArg(self.tg_ctl_file_name_pattern)
            batch_date = self.getKeyValue(self.args_dict, "batch_date")
            if not batch_date:
                batch_date = datetime.today().strftime("%Y%m%d")
            with open(ctl_file, "w", encoding=self.tg_encoding, errors="replace") as f:
                ctl_text = '***{}{}{:09d}{:09d}{}{}'.format(self.replaceArg(self.tg_name_pattern),
                                                            batch_date, process_cnt, process_cnt,
                                                            self.tg_ctl_chinese, batch_date)
                f.write(ctl_text)
        return out_file, ctl_file

    def getColMata(self):
        with open(self.tg_col_size_file, "r", encoding="utf-8") as f:
            fields_lens = [int(line.strip()) for line in f if line.strip()]


        sql = f'select * from {self.src_db_name}.{self.src_table_name} LIMIT 1'
        try:
            dao = self.connectDb()
        except Exception as e:
            self.errorExit(f'[資料庫連線失敗] {e}')

        try:
            cursor = dao.conn.cursor()
            cursor.execute(sql)
            self.logger.debug(f'[SQL] {sql}')
        except Exception as e:
            self.errorExit(f"[執行SQL失敗] {e}")

        # 欄位資訊
        col_info = cursor.description  # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        def detectDtype(db_type):
            if 'CHAR' in db_type:
                return 'str'
            elif 'INTEGER' in db_type:
                return 'num'
            elif 'FLOAT' in db_type:
                return 'float'
            elif 'DECIMAL' in db_type:
                return 'float'
            else:
                return 'str'

        col_meta = []
        for i, col in enumerate(col_info):
            col_name = col[0]
            col_length = fields_lens[i]
            dtype = detectDtype(str(col[1]))
            self.logger.debug("[{}] {}".format(col_name, str(col[1])))
            col_meta.append((col_name, col_length, dtype))

        return col_meta


    def processDbFile(self, db_files):
        out_file_path = self.work_path / self.replaceArg(self.tg_name_pattern)
        out_file = None
        try:
            if (self.tg_delimiter):
                out_file = open(out_file_path, "w", encoding=self.tg_encoding, errors="replace", newline="")
            else:
                out_file = open(out_file_path, "wb")
                col_meta = self.getColMata()
        except Exception as e:
            self.errorExit(f"[開檔{out_file_path}失敗] {e}")


        process_cnt = 0
        for file in db_files:
            with open(file, 'r', encoding=self.src_encoding) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        row = line.strip().split(self.src_delimiter)
                        process_cnt += 1
                    else:
                        continue

                    if self.tg_delimiter:
                        output = self.tg_delimiter.join(str(v) if v is not None else "" for v in row) + self.tg_new_line_character
                        out_file.write(output)
                    else:
                        line_bytes = b""
                        for idx, (col_name, col_length, dtype) in enumerate(col_meta):
                            line_bytes += self.formatField(row[idx], col_length, dtype, self.tg_encoding)
                        out_file.write(line_bytes + self.tg_new_line_character.encode(self.tg_encoding))

        #Create control file
        ctl_file = None
        if(self.tg_ctl_file.lower() == 'y'):
            ctl_file = self.work_path / self.replaceArg(self.tg_ctl_file_name_pattern)
            batch_date = self.getKeyValue(self.args_dict, "batch_date")
            if not batch_date:
                batch_date = datetime.today().strftime("%Y%m%d")
            with open(ctl_file, "w", encoding=self.tg_encoding, errors="replace") as f:
                ctl_text = '***{}{}{:09d}{:09d}{}{}'.format(self.replaceArg(self.tg_name_pattern),
                                                            batch_date, process_cnt, process_cnt,
                                                            self.tg_ctl_chinese, batch_date)
                f.write(ctl_text)

        if out_file:
            out_file.close()

        return out_file_path, ctl_file

    def downloadS3Files(self, src_bucket, download_path, search_key, s3_prefix):

        self.logger.debug(f'[Search S3 file with key {search_key}, s3_prefix {s3_prefix}]')
        try:
            s3SrcDao = S3DaoImpl(src_bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
        except Exception as e:
            self.errorExit(f'[S3連線失敗] {e}')

        try:
            filelist = s3SrcDao.listFiles(search_key, s3_prefix)
        except Exception as e:
            self.errorExit(f'[S3烈出檔案失敗] {e}')
        #self.logger.debug(f'[Source file list ] {filelist}')

        cnt = 0
        download_files = []
        for file in filelist:
            remote_path = download_path / Path(file).name
            #self.logger.debug(f"[Download file] {file} -> {remote_path}")
            download_files.append(str(remote_path))
            try:
                s3SrcDao.downloadFile(file, remote_path)
                cnt += 1
            except Exception as e:
                self.errorExit(f"[S3下載失敗]")
        self.logger.debug((f'Download {cnt} files'))
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
                self.errorExit(f"[開啟暫存檔失敗] {e}")

        line_cnt = 0
        try:
            with open(file_path, "r", encoding=source_encoding, errors="replace") as src:
                for line in src:
                    if (target_encoding != source_encoding):
                        tmp_file.write(line)
                    line = line.strip()
                    if line:
                        line_cnt += 1
        except Exception as e:
            self.errorExit(f"[檔案轉碼失敗] {e}")

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
                self.errorExit(f'[FTP連線失敗] {e}')
            try:
                ftpDao.uploadFile(upload_file, self.tg_path)
                self.logger.info(f"[上傳檔案至FTP成功] {upload_file}")
            except Exception as e:
                self.errorExit(f"[FTP上傳失敗 {upload_file}] {e}")
        elif (self.tg_type.lower() == 's3'):
            try:
                target_path = self.tg_path + Path(upload_file).name
                try:
                    s3Dao = S3DaoImpl(self.tg_bucket, self.s3_host, self.s3_port, self.s3_user, self.s3_sec)
                except Exception as e:
                    self.errorExit(f"[S3連線失敗]")
                self.logger.debug(f'[上傳檔案: {upload_file} -> {target_path}')
                s3Dao.uploadFile(upload_file, target_path)
                self.logger.info(f'[上傳檔案至S3成功] {upload_file}')
            except Exception as e:
                self.errorExit(f'[上傳檔案至S3失敗] {e}')

    def deleteFiles(self, file_list: list):
        """
        刪除傳入列表中的所有檔案。

        Args:
            file_list (list): 包含要刪除的檔案路徑的列表。
        """
        deleted_count = 0

        for file_path in file_list:
            try:
                # 檢查檔案是否存在
                if os.path.exists(file_path):
                    # 刪除檔案
                    os.remove(file_path)
                    #self.logger.debug(f"成功刪除檔案: {file_path}")
                    deleted_count += 1
                else:
                    self.logger.debug(f"警告: 檔案不存在，無法刪除: {file_path}")
            except OSError as e:
                # 處理刪除檔案時可能發生的錯誤，例如權限問題
                self.logger.debug(f"錯誤: 無法刪除檔案 {file_path} - {e}")
        return deleted_count

    def errorExit(self, error_message, error_traceback = ''):
        self.logger.error(error_message)
        if error_traceback:
            self.logger.debug(error_traceback)
        #self.errorHandler.exceptionWriter(error_message)
        exit(1)

    def run(self):
        self.logger.setLevel(getattr(logging, self.log_level, logging.INFO))
        self.logger.info("[Run FTP writter]")
        download_files = []
        filelist = []
        upload_file = None
        if(self.source_type.lower() == "db"):
            out_file, ctl_file = self.exportDbFile()
            upload_file = out_file
            filelist.append(str(out_file))
            if(ctl_file):
                filelist.append(str(ctl_file))
        elif self.source_type.lower() == "dbfile":
            search_key = self.src_path + '*'
            download_files = self.downloadS3Files(self.src_bucket, self.work_path, search_key, s3_prefix=self.src_path)
            out_file, ctl_file = self.processDbFile(download_files)
            #print(out_file, ctl_file)
            filelist.append(str(out_file))
            if (ctl_file):
                filelist.append(str(ctl_file))

        elif(self.source_type.lower() == "s3"):
            search_key = self.src_path + self.replaceArg(self.src_name_pattern)
            out_file = self.temp_path / self.replaceArg(self.tg_name_pattern)
            filelist = self.downloadS3Files(self.src_bucket, self.work_path, search_key, s3_prefix=self.src_path)
            for file in filelist:
                line_cnt = self.convertEncoding(file, self.tg_encoding, self.src_encoding)
                self.logger.info(f'[檔案筆數] {file} : {line_cnt}')
        else:
            self.errorExit(f"[Source type未定義]")


        #Compress data and upload files
        if(self.zip_type):
            #out_zip_file = out_file + "." + self.zip_type.lower()
            out_zip_file = str(out_file) + "." + self.zip_type.lower()
            self.logger.debug(f"[Compress file] {out_zip_file}")
            try:
                Compressor.compress(str(out_zip_file), filelist, self.zip_sec)
            except Exception as e:
                self.errorExit(f'[壓縮檔案失敗] {e}')
            self.uploadFile(out_zip_file)
            filelist.append(out_zip_file)
        else:
            for upload_file in filelist:
                self.logger.debug(f'[Upload file] {upload_file}')
                self.uploadFile(upload_file)

        #Delete process files
        if download_files:
            n = self.deleteFiles(download_files)
            self.logger.debug(f'[Delete download files: {n}]')
        n = self.deleteFiles(filelist)
        self.logger.debug(f'[Delete working files: {n}]')




