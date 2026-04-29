import logging
import socket

try:
    CLIENT_IP = socket.gethostbyname(socket.gethostname())
except Exception:
    CLIENT_IP = "unknown"


class IPLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.clientip = CLIENT_IP
        return True


def get_logger() -> logging.Logger:
    logger = logging.getLogger("ekk_logger")
    if not logger.hasHandlers():
        handler = logging.FileHandler("error.log")
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [Client IP: %(clientip)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.ERROR)
        logger.addFilter(IPLogFilter())
    return logger
