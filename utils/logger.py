import logging
import os
import socket
import sys

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
        log_file = os.getenv("LOG_FILE", "logs/error.log")
        try:
            log_dir = os.path.dirname(log_file)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            handler = logging.FileHandler(log_file)
        except OSError:
            handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [Client IP: %(clientip)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.ERROR)
        logger.addFilter(IPLogFilter())
    return logger
