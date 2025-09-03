import os
import tarfile
import gzip
import shutil
import py7zr
import pyzipper


class Compressor:
    @staticmethod
    def compress(archive_name, files, password=None):
        if archive_name.lower().endswith(".tar.gz"):
            ext = ".tar.gz"
        else:
            ext = os.path.splitext(archive_name)[1].lower()

        if ext == ".zip":
            Compressor.compress_zip(archive_name, files, password)
        elif ext == ".7z":
            Compressor.compress_7z(archive_name, files, password)
        elif ext == ".gz" and len(files) == 1:
            # 單一 .gz 檔案
            Compressor.compress_gz(files[0], archive_name)
        elif ext in [".tar", ".tgz", ".tar.gz"]:
            Compressor.compress_tar(archive_name, files)
        else:
            raise ValueError(f"Unsupported format: {ext}")

    @staticmethod
    def decompress(archive_name, extract_path, password=None):
        if archive_name.lower().endswith(".tar.gz"):
            ext = ".tar.gz"
        else:
            ext = os.path.splitext(archive_name)[1].lower()

        if ext == ".zip":
            return Compressor.decompress_zip(archive_name, extract_path, password)
        elif ext == ".7z":
            return Compressor.decompress_7z(archive_name, extract_path, password)
        elif ext == ".gz" and not archive_name.endswith(".tar.gz"):
            # 單一 .gz 檔案
            return Compressor.decompress_gz(archive_name, extract_path)
        elif ext in [".tar", ".tgz", ".tar.gz"]:
            return Compressor.decompress_tar(archive_name, extract_path)
        else:
            raise ValueError(f"Unsupported format: {ext}")

    @staticmethod
    def _filter_system_files(file_list, extract_path):
        """過濾掉系統產生的檔案"""
        filtered = []
        for file in file_list:
            if file.endswith('/'):
                continue
            base = os.path.basename(file)
            
            # macOS 系統檔案
            if (file.startswith('__MACOSX/') or 
                base.startswith('._') or 
                base == '.DS_Store'):
                continue
                
            # Windows 系統檔案
            if (base == 'Thumbs.db' or 
                base == 'desktop.ini' or
                file.startswith('$RECYCLE.BIN/') or
                file.startswith('System Volume Information/')):
                continue
                
            # Linux 系統檔案
            if (base.startswith('.Trash-') or 
                base == '.directory' or
                base.startswith('.fuse_hidden')):
                continue
                
            # 通用隱藏檔案
            if base.startswith('.'):
                continue
                
            full_path = os.path.join(extract_path, file)
            if os.path.isfile(full_path):
                filtered.append(file)
        return filtered

    # ========== ZIP (AES-256) ==========
    @staticmethod
    def compress_zip(zip_filename, files, password=None):
        with pyzipper.AESZipFile(zip_filename, 'w', compression=pyzipper.ZIP_LZMA) as zf:
            if password:
                zf.setpassword(password.encode('utf-8'))
                zf.setencryption(pyzipper.WZ_AES, nbits=256)
            for file in files:
                zf.write(file, arcname=os.path.basename(file))

    @staticmethod
    def decompress_zip(zip_filename, extract_path, password=None):
        with pyzipper.AESZipFile(zip_filename, 'r') as zf:
            if password:
                file_list = zf.namelist()
                zf.extractall(path=extract_path, pwd=password.encode('utf-8'))
            else:
                file_list = zf.namelist()
                zf.extractall(path=extract_path)

            return Compressor._filter_system_files(file_list, extract_path)

    # ========== 7Z ==========
    @staticmethod
    def compress_7z(archive_name, files, password=None):
        with py7zr.SevenZipFile(archive_name, 'w', password=password) as archive:
            for file in files:
                archive.write(file, arcname=os.path.basename(file))

    @staticmethod
    def decompress_7z(archive_name, extract_path, password=None):
        with py7zr.SevenZipFile(archive_name, 'r', password=password) as archive:
            archive.extractall(path=extract_path)
            file_list = archive.getnames()
            return Compressor._filter_system_files(file_list, extract_path)

    # ========== TAR / TAR.GZ ==========
    @staticmethod
    def compress_tar(tar_filename, files):
        mode = "w"
        if tar_filename.endswith(".tar.gz") or tar_filename.endswith(".tgz") or tar_filename.endswith(".gz"):
            mode = "w:gz"
        with tarfile.open(tar_filename, mode) as tar:
            for file in files:
                tar.add(file, arcname=os.path.basename(file))

    @staticmethod
    def decompress_tar(tar_filename, extract_path):
        mode = "r"
        if tar_filename.endswith(".tar.gz") or tar_filename.endswith(".tgz") or tar_filename.endswith(".gz"):
            mode = "r:gz"
        with tarfile.open(tar_filename, mode) as tar:
            tar.extractall(path=extract_path)
            file_list = tar.getnames()
            return Compressor._filter_system_files(file_list, extract_path)

    # ========== GZ 單檔 ==========
    @staticmethod
    def compress_gz(file_path, output_path):
        with open(file_path, "rb") as f_in:
            with gzip.open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    @staticmethod
    def decompress_gz(gz_path, extract_path):
        # 從 .gz 檔名推測原始檔名
        base_name = os.path.basename(gz_path)
        if base_name.endswith('.gz'):
            original_name = base_name[:-3]  # 移除 .gz
        else:
            original_name = base_name
            
        output_path = os.path.join(extract_path, original_name)
        
        with gzip.open(gz_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # 回傳解壓縮後的檔案列表
        return [original_name]



if __name__ == "__main__":
    files = ["C:\\Users\\Baldwin\\PycharmProjects\\dataLake\\temp\\test.txt",
             "C:\\Users\\Baldwin\\PycharmProjects\dataLake\\temp\\test2.txt"]

    # ZIP (ZipCrypto，不安全)
    Compressor.compress("test.zip", files, password="12345")
    Compressor.decompress("test.zip", "./out_zip", password="12345")

    # 7Z (AES-256)
    Compressor.compress("test.7z", files, password="12345")
    Compressor.decompress("test.7z", "./out_7z", password="12345")

    # TAR
    Compressor.compress("test.tar", files)
    Compressor.decompress("test.tar", "./out_tar")

    # TAR.GZ
    Compressor.compress("test.tar.gz", files)
    Compressor.decompress("test.tar.gz", "./out_targz")

    # GZ (單一檔案)
    # with open("a.txt", "rb") as f_in:
    #     with gzip.open("a.txt.gz", "wb") as f_out:
    #         shutil.copyfileobj(f_in, f_out)
    #
    # with gzip.open("a.txt.gz", "rb") as f_in:
    #     with open("a_out.txt", "wb") as f_out:
    #         shutil.copyfileobj(f_in, f_out)