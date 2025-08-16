import os, sys
import logging

class Logger():

    def __init__(self, log_path, log_name):
        # 定義日誌格式
        formatter = logging.Formatter(fmt = '%(asctime)-22s %(levelname)-7s %(filename)-6s : %(lineno)d - %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')
        
        # 檔案輸出處理器
        handler = logging.FileHandler('{}/{}.log'.format(log_path, log_name), mode = 'a')
        handler.setFormatter(formatter)

        # 終端機輸出處理器
        screen_handler = logging.StreamHandler(stream = sys.stdout)
        screen_handler.setFormatter(formatter)

        # 創建 logger 實例
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.addHandler(screen_handler)
