import time
import logging
import sys
from loguru import logger

def get_current_timestamp():
    """Returns current unix timestamp in seconds (int)."""
    return int(time.time())

def setup_logger():
    """Configures loguru logger."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    return logger

