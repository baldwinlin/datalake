
import os, sys
import logging

class dataLakeUtilsErrorHandler():
    def __init__(self, logger_name):
        self.LOGGER_ERROR = logging.getLogger(logger_name)
    
    def exceptionWriter(self, error_message):
        self.LOGGER_ERROR.error(error_message)