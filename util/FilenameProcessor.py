import fnmatch
from typing import Optional

class FilenameProcessor:
    @staticmethod
    def _process_name_pattern(name_pattern, date):
        """處理檔案名稱模式，替換單一參數日期變數"""
        if name_pattern and date:
            processed_name_pattern = name_pattern.replace("${date}", str(date))
            return processed_name_pattern
        else:
            raise Exception("尚未正確設定檔案名稱模式 或 Batch Date")
    @staticmethod
    def _match_name_pattern(name: str, target_name_pattern: Optional[str] = None) -> bool:
        """檢查檔案名稱是否符合指定的模式"""
        if target_name_pattern is None:
            return False
        result = fnmatch.fnmatch(name, target_name_pattern)
        return result
        