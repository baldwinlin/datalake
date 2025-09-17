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


from dao.FileDao import FileDao
from util.FilenameProcessor import FilenameProcessor


import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
from typing import List
import os
import base64, hashlib
import re
import fnmatch 


def _inject_content_md5(request, **kwargs):
    if not request.body or 'Content-MD5' in request.headers:
        return

    data = request.body if isinstance(request.body, (bytes, bytearray)) else bytes(request.body, 'utf-8')
    digest_b64 = base64.b64encode(hashlib.md5(data).digest()).decode('utf-8')
    request.headers['Content-MD5'] = digest_b64
    print(f"[MD5 injected] Content-MD5={digest_b64}, body_len={len(data)}")

class S3DaoImpl(FileDao):
    def __init__(self, bucket: str, host: str, port: str, aws_access_key_id=None,
                 aws_secret_access_key=None, region_name=None):
        try:
            self.s3 = boto3.client(
                's3',
                endpoint_url=f"http://{host}:{port}",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
                #config=Config(signature_version="s3v4", s3={'addressing_style': 'path'})
            )
            es = self.s3.meta.events
            es.register('request-created.s3.DeleteObjects', _inject_content_md5)

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

    def deleteFiles(self, remote_path_list: List[str]):
        result = []
        if not remote_path_list:
            raise Exception("S3 刪除檔案失敗無需要刪除的檔案")
        CHUNK = 2
        try:
            for i in range(0, len(remote_path_list), CHUNK):
                chunk = remote_path_list[i:i+CHUNK]
                result = self.s3.delete_objects(Bucket=self.bucket, Delete={'Objects': [{'Key': key} for key in chunk], "Quiet": False})
            return result['Deleted']
        except ClientError as e:
            # 整個請求層級失敗（例如 BadDigest / 簽名錯）
            raise Exception(f"S3 多檔刪除請求失敗：{e}")    

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