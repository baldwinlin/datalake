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

class S3DaoImpl(FileDao):
    def __init__(self, bucket: str, aws_access_key_id=None,
                 aws_secret_access_key=None, region_name=None):
        try:
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            self.bucket = bucket
        except Exception as e:
            raise Exception(f"S3 連線失敗: {e}")

    def listFiles(self, pattern: str) -> List[str]:
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket)

            files = []
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if fnmatch.fnmatch(os.path.basename(key), pattern):
                        files.append(key)
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


if __name__ == "__main__":
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",  # LocalStack
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )

    print(s3.list_buckets())