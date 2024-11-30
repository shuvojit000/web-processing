import logging
from pathlib import Path

base_dir = str(Path(__file__).parent.resolve())

class SeparatedLogFormatter(logging.Formatter):
    def format(self, record):
        result= logging.Formatter.format(self,record)
        if(record):
            result=result+"\n"+"-"*50+"\n"
        return result
    
    
def create_logger(module_name):
    logger=logging.getLogger(module_name)
    file_handler=logging.FileHandler(base_dir+"/file.log")
    console_handler=logging.StreamHandler()
    log_formatter=SeparatedLogFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(log_formatter)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    return logger