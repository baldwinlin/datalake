from typing import Optional
from util.FilenameProcessor import FilenameProcessor
import os
# import re

class Validator:

    @staticmethod
    def get_file_line_count(file_name:str, file_path:str, controller_file_delimiter:Optional[str] = None, controller_file_name_pattern:Optional[str] = None):
        if FilenameProcessor._match_name_pattern(file_name, controller_file_name_pattern): 
            controller_file_path = os.path.join(file_path, file_name)
            expected_records = Validator._read_controller_file(controller_file_path, controller_file_delimiter)
            return ("檢核檔", file_name, expected_records)
        else:
            file_path = os.path.join(file_path, file_name)
            with open(file_path, "rb") as f:
                downloaded_lines_count = len(f.readlines())
            return ("檔案", file_name, downloaded_lines_count)
    
    @staticmethod
    def _read_controller_file(controller_file_path: str, controller_file_delimiter: str) -> int:
        """
        讀取設定檔內容，格式為：20251202000106 或 20251202,000106
        提取筆數部分（去除日期）
        """
        try:
            with open(controller_file_path, 'r', encoding='big5') as f:
                content = f.read().strip()

            # 方法1: 如果有分隔符號分隔 (20251202,000106)
            if controller_file_delimiter in content:
                parts = content.split(controller_file_delimiter)
                if len(parts) == 2:
                    return int(parts[1])
            
            # 方法2: 如果沒有逗號 (20251202000106)
            # 假設日期是8位數 (YYYYMMDD)，剩下的就是筆數
            if len(content) > 8 and content.isdigit():
                record_count = content[8:]  # 跳過前8位日期
                return int(record_count)
        except Exception as e:
            raise Exception(f"讀取設定檔失敗 {controller_file_path}: {e}")
    
    @staticmethod
    def check_file_line_count(file_rows_counts_info: list[str], header: str):
        total_rows_count = 0
        controller_rows_count = 0
        file_counts = 0
        for kind, fname, rows_count in file_rows_counts_info:
            if kind == "檔案":
                total_rows_count += rows_count
                file_counts += 1
            elif kind == "檢核檔":
                controller_rows_count = rows_count
        
        if header == "Y":
            if file_counts != 0:
                total_rows_count -= file_counts
        
        if controller_rows_count == 0:
            raise Exception("設定檔無讀到資料")
        
        if total_rows_count != controller_rows_count:
            raise Exception(f" 檔案行數不符，預期筆數：{controller_rows_count}，實際筆數：{total_rows_count}")
        
        return True


    @staticmethod
    def check_header_batch_date(header_file_path: str, delimiter: str, batch_date: str):
        with open(header_file_path, 'r', encoding='big5') as f:
            content = f.read().strip()
        if delimiter in content:
            parts = content.split(delimiter)
            if len(parts) == 2:
                if str(parts[0]) == batch_date:
                    return True
                else:
                    raise Exception(f"設定檔日期不符，預期日期：{batch_date}，實際日期：{parts[0]}")
        else:
            if str(content[:8]) == batch_date:
                return True
            else:
                raise Exception(f"設定檔日期不符，預期日期：{batch_date}，實際日期：{content[:8]}")





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
