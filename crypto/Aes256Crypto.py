#coding:utf-8
'''
FtpLoader.py
Object          : 使用AES256+BASE64方式加解密
Author          :
Version         :
Date written    :
Modify Date     :
Memo.           : use pycryptodome package
Parameters      :
Output          :
********************************************************************************
Modify          :
'''

import base64
import gnupg

'''
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES
def aes256Enrypt(plain_text, key):
    iv = get_random_bytes(16)  # 生成隨機初始化向量
    cipher = AES.new(key, AES.MODE_CBC, iv)  # 創建加密對象
    encrypted = cipher.encrypt(pad(plain_text.encode('utf-8'), AES.block_size))  # 加密並補位
    return base64.b64encode(iv + encrypted).decode('utf-8')  # 返回加密後的資料（含IV）


def aes256Decrypt(encrypted_text, key):
    print(f"sec = [{encrypted_text}] [{key}]")
    encrypted_bytes = base64.b64decode(encrypted_text)  # 解碼
    iv = encrypted_bytes[:16]  # 提取IV
    encrypted_data = encrypted_bytes[16:]  # 提取加密資料
    cipher = AES.new(key, AES.MODE_CBC, iv)  # 創建解密對象
    decrypted = unpad(cipher.decrypt(encrypted_data), AES.block_size).decode('utf-8')  # 解密並去補位
    return decrypted
'''

def readSecFile(sec_file):
    try:
        file = open(sec_file, "r")
        line = file.readline().strip('\n')
        username = line[9:]
        line = file.readline().strip('\n')
        sec = line[7:]
    except Exception as e:  # Catching a more general exception for demonstration
        print(f"An error occurred during file writing: {e}")

    #print(f"user={username}, sec={sec}")
    return username, sec

def readSaltFile(salt_file):
    try:
        file = open(salt_file, "r")
        line = file.readline().strip('\n')
        salt = line[5:]
    except Exception as e:  # Catching a more general exception for demonstration
        print(f"An error occurred during file writing: {e}")
    return salt

def get_gpg_decrypt(key, salt):
    if (key is None) or (salt is None):
        raise Exception(f"gpg_decrypt error (key: {key}, salt: {salt})")
    gpg = gnupg.GPG()
    enc_raw = gpg.decrypt(base64.b64decode(key.strip()), passphrase=salt).data.decode()

    return enc_raw


if __name__ == "__main__":

    key = 'jA0ECQMCfMmwgW3dgnr20lIBicyWECvqJ5mfL8oVvbVQGElGVIovQa41FXofyZrmRGqDVSgH5uF10qN8uqtSKf6wKJvS8VwP5bzE2tEFlrS8VdrHqRvhgZf1MmBnTZtkBtcT'
    salt = 'DuFHWY9AOYviv'
    sec = get_gpg_decrypt(key, salt)
    print("sec = ", sec)
    exit(0)

    #key = get_random_bytes(32)  # 256位元密鑰
    key = bytes.fromhex('a23903bea14926ca205a850a3a26cd968deae4ed35400f7c1826e978a829b507')

    print("Key: ", key.hex())
    plain_text = "Baldwin31"  # 要加密的明文

    print(f"明文: {plain_text}")

    encrypted = aes256Enrypt(plain_text, key)  # 加密
    print(f"加密後: {encrypted}")

    decrypted = aes256Decrypt(encrypted, key)  # 解密
    print(f"解密後: {decrypted}")
