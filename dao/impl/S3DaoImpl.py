#coding:utf-8
'''
FtpDaoImpl.py
Object          : S3 file access - oupload files to target path and download files from source path
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           :
Parameters      :
Output          :
********************************************************************************
Modify          :
'''

import boto3
import fnmatch
from dao.FileDao import FileDao
import os
from typing import List
from util.FilenameProcessor import FilenameProcessor
import re
class S3DaoImpl(FileDao):
    def __init__(self, bucket: str, host: str, port: str, aws_access_key_id=None,
                 aws_secret_access_key=None, region_name=None):
        try:
            self.s3 = boto3.client(
                's3',
                endpoint_url=f"http://{host}:{port}",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            self.bucket = bucket
        except Exception as e:
            raise Exception(f"S3 連線失敗: {e}")

    def connect(self):
        pass

    def listFiles(self, pattern: str) -> List[str]:
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket)

            files = []
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if FilenameProcessor._match_name_pattern(key, pattern):
                        files.append(key)
                    # if fnmatch.fnmatch(key, pattern):
                    #     files.append(key)

                    # REGEX_HINTS = ("^", "$", r"\d", r"\w", r"\s", "(", ")", "|", "{", "}")
                    # if any(h in pattern for h in REGEX_HINTS):
                    #     try:
                    #         if re.fullmatch(pattern, key) is not None:
                    #             files.append(key)
                    #     except re.error as e:
                    #         raise Exception(
                    #             f"檢查檔案名稱樣式失敗：疑似正規表示式不合法（{e}）。請統一使用 glob 或合法 regex。"
                    #         )
            return files
        except Exception as e:
            raise Exception(f"S3 列出檔案失敗: {e}")

    def downloadFile(self, remote_path: str, local_path: str) -> None:
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.s3.download_file(self.bucket, remote_path, local_path)
        except Exception as e:
            raise Exception(f"S3 下載檔案失敗 ({remote_path}): {e}")

    def uploadFile(self, local_path: str, remote_path: str) -> None:
        try:
            self.s3.upload_file(local_path, self.bucket, remote_path)
        except Exception as e:
            raise Exception(f"S3 上傳檔案失敗 ({local_path}): {e}")

    def deleteFile(self, remote_path: str) -> None:
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=remote_path)
        except Exception as e:
            raise Exception(f"S3 刪除檔案失敗 ({remote_path}): {e}")

    def deleteFiles(self, remote_path_list: List[str]) -> None:
        result = []
        if not remote_path_list:
            raise Exception("S3 刪除檔案失敗無需要刪除的檔案")
        CHUNK = 10
        for i in range(0, len(remote_path_list), CHUNK):
            chunk = remote_path_list[i:i+CHUNK]
            result = self.s3.delete_objects(Bucket=self.bucket, Delete={'Objects': [{'Key': key} for key in chunk], "Quiet": False})
        return result['Deleted']

    def close(self):
        pass

if __name__ == "__main__":
    # s3 = boto3.client(
    #     "s3",
    #     endpoint_url="http://localhost:4566",  # LocalStack
    #     aws_access_key_id="test",
    #     aws_secret_access_key="test",
    #     region_name="us-east-1"
    # )
    # s3.create_bucket(Bucket="my-bucket-123456")
    # print(s3.list_buckets())
    # # exit()

    s3Dao = S3DaoImpl("my-bucket-123456", "localhost", 4566, "test", "test")
    # s3Dao.uploadFile("C:\\Users\\Baldwin\\PycharmProjects\\dataLake\\temp\\test.txt", "datalake/test.txt")
    files = s3Dao.listFiles("dataLake/uploads/aaaUTF\d*_20250901\.txt")
    print(f"files: {files}")

    # delete_files = files[:3]
    # print(f"預定刪除的檔案: {delete_files}")
    # result = s3Dao.deleteFiles(delete_files)
    # print(f"刪除的結果: {result}")


    # print(files)