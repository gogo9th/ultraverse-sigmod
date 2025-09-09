import os

from .logger import get_logger

MYSQL_DISTRIBUTION_URL = "https://dev.mysql.com/get/Downloads/MySQL-8.1/mysql-8.1.0-linux-glibc2.28-x86_64.tar.xz"
MYSQL_DISTRIBUTION_NAME = "mysql-8.1.0-linux-glibc2.28-x86_64"
MYSQL_DISTRIBUTION_TARNAME = "mysql-8.1.0-linux-glibc2.28-x86_64.tar.xz"

def download_mysql() -> bool:
    logger = get_logger("download_mysql")

    if not os.path.exists("cache"):
        os.mkdir("cache")

    if not os.path.exists(f"cache/{MYSQL_DISTRIBUTION_TARNAME}"):
        logger.info(f'Downloading MySQL distribution: {MYSQL_DISTRIBUTION_URL}')

        # Download MySQL using curl; download to [pwd]/cache
        retval = os.system(f"curl -L -o cache/{MYSQL_DISTRIBUTION_TARNAME} {MYSQL_DISTRIBUTION_URL}")

        if retval != 0:
            logger.error(f"Failed to download MySQL distribution")
            return False

    if not os.path.exists(f"cache/mysql/{MYSQL_DISTRIBUTION_NAME}/bin/mysql"):
        logger.info(f'Extracting MySQL distribution: cache/{MYSQL_DISTRIBUTION_TARNAME}')

        os.system('rm -rf cache/mysql')
        os.mkdir("cache/mysql")

        retval = os.system(f"tar -xf cache/{MYSQL_DISTRIBUTION_TARNAME} -C cache/mysql")

        if retval != 0:
            logger.error("WARN: Failed to extract MySQL distribution")
            return False

    logger.info(f"MySQL distribution is ready: using cache/mysql/{MYSQL_DISTRIBUTION_NAME}")
    return True

