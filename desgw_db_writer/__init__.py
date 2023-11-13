import logging
import os
from logging import handlers


def log_namer(filename):
    """
    Force the name of the *backup* logfile to end with "log"
    Please the note the active file has no file extension and the rest are archives (e.g. desgw_db_writer.3.log)
    """
    return f"{filename}.log"


APP_ROOT = os.path.dirname(os.path.abspath(__file__))
LOCAL_LOGFILE = os.path.join(APP_ROOT, "desgw_db_writer")
megabyte = 10**6
handler = handlers.RotatingFileHandler(
    LOCAL_LOGFILE, maxBytes=1 * megabyte, backupCount=3
)
handler.namer = log_namer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(name)s - %(funcName)s - %(levelname)s: %(message)s",
    handlers=[handler],
    datefmt="%Y-%m-%d %H:%M:%S",
)
