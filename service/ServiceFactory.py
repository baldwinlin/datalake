from service.FtpLoader import FtpLoader
from service.impl.FtpLoaderImpl import FtpLoaderImpl
from dao.FileDao import FileDao

class ServiceFactory:
    @staticmethod
    def create_ftp_loader(dao: FileDao) -> FtpLoader:
        """
        創建 FTP Loader 服務實例
        """
        return FtpLoaderImpl(dao)