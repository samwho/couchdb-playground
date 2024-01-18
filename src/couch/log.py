import logging

logger = logging.getLogger("couch")
handler = logging.StreamHandler()

handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)"))

logger.addHandler(handler)