# from tkinter import NONE
from typing import Optional
from util.FilenameProcessor import FilenameProcessor
from util.Reformatter import Reformatter
import os
# import re

class Validator:

    @staticmethod
    def get_file_line_count(file_name:str, file_path:str, controller_file_delimiter:Optional[str] = None, controller_file_name_pattern:Optional[str] = None):
        if FilenameProcessor.is_controller_file(file_name, controller_file_name_pattern): 
            controller_file_path = os.path.join(file_path, file_name)
            expected_records = Validator._read_CTF_rows_data(controller_file_path, controller_file_delimiter)
            return ("檢核檔", file_name, expected_records,f"不包含 Header 總計筆數")
        else:
            file_path = os.path.join(file_path, file_name)
            with open(file_path, "rb") as f:
                downloaded_lines_count = len(f.readlines())
            return ( "檔案", file_name , downloaded_lines_count, f"總計筆數{downloaded_lines_count}")
    
    @staticmethod
    def _read_CTF_rows_data(controller_file_path: str, controller_file_delimiter: str) -> int:
        """
        讀取設定檔內容，格式為：20251202000106 或 20251202,000106
        提取筆數部分（去除日期）
        """
        try:
            
            with open(controller_file_path, 'r', encoding='big5') as f:
                content = f.read().strip()
            
            if controller_file_delimiter is None:
                if len(content) > 8 :
                    record_count = content[8:]
                    try:
                        count_int = int(record_count)
                    except Exception:
                        raise Exception(f"讀取控制檔失敗 {controller_file_path} 有分隔符號但是沒有輸入分隔符號或是資料格式錯誤：{content}")
                    return count_int
            elif controller_file_delimiter in content:
                parts = content.split(controller_file_delimiter)
                if len(parts) == 2:
                    try:
                        return int(parts[1])
                    except Exception:
                        raise Exception(f"讀取控制檔失敗 {controller_file_path} 輸入錯誤或資料格式錯誤：{content}")
            elif len(content) > 8 :
                record_count = content[8:]  # 跳過前8位日期
                try:
                    count_int = int(record_count)
                except Exception:
                    raise Exception(f"讀取控制檔失敗 {controller_file_path} 輸入錯誤或資料格式錯誤：{content}")
                return count_int
        except Exception as e:
            raise Exception(f"讀取設定檔失敗 {controller_file_path}: {e}")
    
    @staticmethod
    def check_file_line_count(file_rows_counts_info: list[str]):
        total_rows_count = 0
        controller_rows_count = 0
        file_counts = 0
        for kind, _ , rows_count, _ in file_rows_counts_info:
            if kind == "檔案":
                total_rows_count += rows_count
                file_counts += 1
            elif kind == "檢核檔":
                controller_rows_count = rows_count
        
        if controller_rows_count == 0:
            raise Exception("設定檔無讀到資料")
        
        if total_rows_count != controller_rows_count:
            raise Exception(f" 檔案行數不符，預期筆數：{controller_rows_count}，實際筆數：{total_rows_count}")
        
        return True

    @staticmethod
    def check_header_batch_date(header_file_path: str, delimiter: str, batch_date: str):
        with open(header_file_path, 'r', encoding='big5') as f:
            content = f.read().strip()

        if delimiter == None:
            if str(content[:8]) == batch_date:
                return True
            else:
                raise Exception(f"設定檔日期不符，預期日期：{batch_date}，實際日期：{content[:8]}")
        
        elif delimiter in content:
            parts = content.split(delimiter)
            if len(parts) == 2:
                if str(parts[0]) == batch_date:
                    return True
                else:
                    raise Exception(f"設定檔日期不符，預期日期：{batch_date}，實際日期：{parts[0]}")
        elif len(content) > 8 :
            if str(content[:8]) == batch_date:
                return True
            else:
                raise Exception(f"設定檔日期不符，預期日期：{batch_date}，實際日期：{content[:8]}")

    @staticmethod
    def checking_decoding(file_path: str, decoding: str):
        with open(file_path, "rb") as f:
            original_content = f.read()
       
        original_lines = Reformatter._split_content_with_row_lists(original_content)
        problematic_lines = []
        for index, (line, _ ) in enumerate(original_lines):
            try:
                line.decode(decoding, errors = "strict")
            except UnicodeDecodeError:
                problematic_lines.append(index)
        return problematic_lines    

    @staticmethod
    def checking_row_length(file_path: str, col_size_file: str):
        col_sizes = Reformatter._read_sizes_file(col_size_file, encoding = "utf-8")
        total_row_size = sum(col_sizes)
        
        # 直接讀取原始 byte 內容來檢查長度，而不是解碼後的字串長度
        with open(file_path, "rb") as f:
            content_bytes = f.read()
        
        lines = Reformatter._split_content_with_row_lists(content_bytes)

        error_lines = []
        for index, (line, _ ) in enumerate(lines):
            if len(line) != total_row_size:
                error_lines.append(index +1 )
        if len(error_lines) >0 :
            return error_lines
        else:
            return True


if __name__ == "__main__":

    file_rows_counts_info_1 = [
        ("檔案", "aaa1_20250829.csv", 100),
        ("檔案", "aaa2_20250829.csv", 11),
        ("檔案", "aaa3_20250829.csv", 11),
        ("檔案", "aaa4_20250829.csv", 11),
        ("檔案", "aaa5_20250829.csv", 11),
        ("檢核檔", "aaa_20250829_C.txt", 139)
    ]
    file_rows_counts_info_2 = [
        ("檔案", "aaa1_20250829.csv", 100),
        ("檢核檔", "aaa_20250829_C.txt", 199)
    ]
    try:
        Validator.check_file_line_count(file_rows_counts_info_1, "Y")
    except Exception as e:
        print(e)
    try:
        Validator.check_file_line_count(file_rows_counts_info_2, "Y")
    except Exception as e:
        print(e)
