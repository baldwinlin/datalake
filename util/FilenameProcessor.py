import fnmatch
import re
from typing import Optional

class FilenameProcessor:
    @staticmethod
    def _process_name_pattern(name_pattern, date):
        """處理檔案名稱模式，替換單一參數日期變數"""
        if not name_pattern:
            raise Exception("尚未正確設定檔案名稱模式")

        if "${batch_date}" in name_pattern:
            if date is None:
                raise Exception("需要置換日期，但尚未正確提供參數 date")

        processed_name_pattern = name_pattern.replace("${batch_date}", str(date))
        return processed_name_pattern
    
    @staticmethod
    def _match_name_pattern(name: str, target_name_pattern: Optional[str] = None) -> bool:
        """檢查檔案名稱是否符合指定的模式"""
        if target_name_pattern is None:
            return False
        
        if fnmatch.fnmatch(name, target_name_pattern):
            return True

        try:
            if re.fullmatch(target_name_pattern, name):
                return True
        except Exception as e:
            raise Exception(f"檢查檔案名稱是否符合指定的模式失敗，檔案名稱模式需要統一使用正規表示式或是萬用字元: {e}")
        
        return False

    @staticmethod
    def is_controller_file(name:str,controller_name_pattern:str) -> bool:
        if controller_name_pattern is None:
            return False
        
        if fnmatch.fnmatch(name, controller_name_pattern):
            return True

        return False

    