import os
import shutil

class CleanTempFile:
    @staticmethod
    def remove_temp_operation_directory(operation_folder_path: str):
        if os.path.exists(operation_folder_path):
            try:
                shutil.rmtree(operation_folder_path)
            except Exception as e:
                raise Exception(f"刪除暫存目錄時發生錯誤: {e}")