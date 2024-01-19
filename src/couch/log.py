import logging
from rich.logging import RichHandler

logger = logging.getLogger("couch")
logger.addHandler(RichHandler(rich_tracebacks=True))
