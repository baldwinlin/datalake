import os
import shutil

class CleanTempFile:
    @staticmethod
    def remove_temp_operation_directory(operation_folder_path: str):
        if not os.path.exists(operation_folder_path):
            raise Exception(f"暫存目錄不存在: {operation_folder_path}")
        elif os.path.exists(operation_folder_path):
            try:
                shutil.rmtree(operation_folder_path)
            except Exception as e:
                raise(f"刪除暫存目錄時發生錯誤: {e}")
                exit(1)