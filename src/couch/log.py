import logging

logger = logging.getLogger("couch")
handler = logging.StreamHandler()

handler.setFormatter(
    logging.Formatter(
        "\033[94m%(asctime)s\033[0m \033[93m%(levelname)s\033[0m %(message)s \033[95m(%(filename)s:%(lineno)d)\033[0m"
    )
)

logger.addHandler(handler)
