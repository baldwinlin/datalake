import os
import shutil

class CleanTempFile:

    @staticmethod
    def remove_temp_operation_directory(operation_folder_path: str):
        if not os.path.exists(operation_folder_path):
            print(f"暫存目錄不存在: {operation_folder_path}")
            return
        
        print(f"刪除暫存目錄: {operation_folder_path}")
        try:
            shutil.rmtree(operation_folder_path)
            print(f"已刪除暫存目錄: {operation_folder_path}")
        except Exception as e:
            print(f"刪除暫存目錄時發生錯誤: {e}")
            raise