from dao.FileDao import FileDao
from dao.impl.FtpDaoImpl import FtpDaoImpl
# from dao.impl.S3DaoImpl import S3DaoImpl

class FileDAOFactory:
    @staticmethod
    def create_dao(config, args: dict) -> FileDao:
        """
        根據配置創建對應的 DAO 實例
        """
        # 檢查配置中的檔案類型
        if 'FTP' in config:
            return FtpDaoImpl(config, args)
        # elif 'S3' in config:
        #     # 假設 S3 配置格式
        #     bucket = config['S3']['BUCKET']
        #     access_key = config['S3']['ACCESS_KEY'] 
        #     secret_key = config['S3']['SECRET_KEY']
        #     region = config['S3']['REGION']
        #     return S3DaoImpl(bucket, access_key, secret_key, region)
        # else:
        #     raise ValueError("不支援的檔案存取類型")