import os
import platform

from .logger import get_logger

MYSQL_VERSION = "8.4.8"

# Platform-specific MySQL distribution info
MYSQL_DISTRIBUTIONS = {
    "Linux": {
        "x86_64": {
            "url": f"https://dev.mysql.com/get/Downloads/MySQL-8.4/mysql-{MYSQL_VERSION}-linux-glibc2.28-x86_64.tar.xz",
            "name": f"mysql-{MYSQL_VERSION}-linux-glibc2.28-x86_64",
            "tarname": f"mysql-{MYSQL_VERSION}-linux-glibc2.28-x86_64.tar.xz",
        },
    },
    "Darwin": {
        "arm64": {
            "url": f"https://dev.mysql.com/get/Downloads/MySQL-8.4/mysql-{MYSQL_VERSION}-macos15-arm64.tar.gz",
            "name": f"mysql-{MYSQL_VERSION}-macos15-arm64",
            "tarname": f"mysql-{MYSQL_VERSION}-macos15-arm64.tar.gz",
        },
        "x86_64": {
            "url": f"https://dev.mysql.com/get/Downloads/MySQL-8.4/mysql-{MYSQL_VERSION}-macos15-x86_64.tar.gz",
            "name": f"mysql-{MYSQL_VERSION}-macos15-x86_64",
            "tarname": f"mysql-{MYSQL_VERSION}-macos15-x86_64.tar.gz",
        },
    },
}


def get_mysql_distribution() -> dict | None:
    """Get the MySQL distribution info for the current platform."""
    system = platform.system()
    machine = platform.machine()

    if system not in MYSQL_DISTRIBUTIONS:
        return None

    arch_map = MYSQL_DISTRIBUTIONS[system]
    if machine not in arch_map:
        return None

    return arch_map[machine]


def get_mysql_bin_path() -> str:
    dist = get_mysql_distribution()
    if dist is None:
        raise RuntimeError("Unsupported platform for MySQL distribution")
    return f"{os.getcwd()}/cache/mysql/{dist['name']}/bin"


def download_mysql() -> bool:
    logger = get_logger("download_mysql")

    dist = get_mysql_distribution()
    if dist is None:
        logger.error(f"Unsupported platform: {platform.system()} {platform.machine()}")
        return False

    url = dist["url"]
    name = dist["name"]
    tarname = dist["tarname"]

    if not os.path.exists("cache"):
        os.mkdir("cache")

    if not os.path.exists(f"cache/{tarname}"):
        logger.info(f'Downloading MySQL distribution: {url}')

        # Download MySQL using curl; download to [pwd]/cache
        retval = os.system(f"curl -L -o cache/{tarname} {url}")

        if retval != 0:
            logger.error(f"Failed to download MySQL distribution")
            return False

    if not os.path.exists(f"cache/mysql/{name}/bin/mysql"):
        logger.info(f'Extracting MySQL distribution: cache/{tarname}')

        os.system('rm -rf cache/mysql')
        os.mkdir("cache/mysql")

        retval = os.system(f"tar -xf cache/{tarname} -C cache/mysql")

        if retval != 0:
            logger.error("WARN: Failed to extract MySQL distribution")
            return False

    logger.info(f"MySQL distribution is ready: using cache/mysql/{name}")
    return True
