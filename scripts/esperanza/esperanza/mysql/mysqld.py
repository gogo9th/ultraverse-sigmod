import logging
import sys
import os
import subprocess
import time

from typing import Optional

from esperanza.utils.download_mysql import get_mysql_distribution
from esperanza.utils.osutils import get_current_user
from esperanza.utils.logger import get_logger


def get_mysql_base_path() -> str:
    dist = get_mysql_distribution()
    if dist is None:
        raise RuntimeError("Unsupported platform for MySQL distribution")
    return f"{os.getcwd()}/cache/mysql/{dist['name']}"


MYSQL_DEFAULT_CONF_PATH = f"{os.getcwd()}/mysql_conf/my.cnf"

class _MySQLDaemonSession:
    def __init__(self, daemon: "MySQLDaemon", started: bool):
        self._daemon = daemon
        self._started = started

    def __enter__(self) -> "MySQLDaemon":
        if not self._started:
            raise RuntimeError("MySQL daemon is already running")
        return self._daemon

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if self._started:
            self._daemon.stop()
        return False

    def __bool__(self) -> bool:
        return self._started

class MySQLDaemon:
    """
    a class that wraps the MySQL daemon
    """

    logger: logging.Logger

    port: int
    data_path: str
    base_path: str
    config_path: str

    mysqld_handle: Optional[subprocess.Popen]

    def __init__(self, port: int, data_path: str, base_path: str | None = None, config_path=MYSQL_DEFAULT_CONF_PATH):
        if base_path is None:
            base_path = get_mysql_base_path()
        self.port = port
        self.data_path = data_path
        self.base_path = base_path
        self.config_path = config_path
        self.logger = get_logger("MySQLDaemon")

        self.mysqld_handle = None

    def __del__(self):
        self.stop()

    def __exec__(self, binary: str, args: list[str]) -> int:
        """
        executes a binary with the given arguments. (like execvp)
        """
        return subprocess.call([binary] + args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def __exec_nonblock__(self, binary: str, args: list[str]) -> subprocess.Popen:
        """
        executes a binary with the given arguments. (like execvp)
        """
        return subprocess.Popen([binary] + args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def bin_for(self, name: str) -> str:
        return f"{self.base_path}/bin/{name}"

    def flush_data(self):
        """
        flushes the MySQL data directory.
        """

        if self.mysqld_handle is not None:
            self.logger.error("MySQL daemon is running")
            return

        self.logger.info("flushing MySQL data directory...")
        os.system(f"rm -rf {self.data_path}/*")

    def flush_binlogs(self):
        """
        flushes the MySQL binlogs.
        """

        if self.mysqld_handle is not None:
            self.logger.error("MySQL daemon is running")
            return

        self.logger.info("flushing MySQL binlogs...")
        os.system(f"rm -rf {self.data_path}/server-binlog.*")

    def mysqldump(self, database: str, output: str):
        """
        dumps the given database to the given output file.
        """

        if self.mysqld_handle is None:
            raise Exception("MySQL daemon is not running")
            return

        self.logger.info(f"dumping database '{database}' to '{output}'...")
        retval = self.__exec__(
            self.bin_for("mysqldump"),
            [
                "-h127.0.0.1",
                f"--port={self.port}",
                "-uroot",
                "-ppassword",
                "-R",
                "--skip-opt",
                "--create-options",
                "--extended-insert",
                "--flush-logs",
                # "--single-transaction",
                "--lock-all-tables",
                "--complete-insert",
                database,
                f"--result-file={output}"
            ]
        )

        if retval != 0:
            raise Exception(f"failed to dump database '{database}'")

    def prepare(self):
        """
        this method prepares 'data directory' for MySQL daemon.
        """

        self.logger.info("preparing MySQL data directory...")

        os.system(f"mkdir -p {self.data_path}")

        username = get_current_user()
        mysqld = self.bin_for("mysqld")

        self.__exec__(mysqld, [
            f"--defaults-file={self.config_path}",
            "--initialize-insecure",
            f"--user={username}",
            f"--basedir={self.base_path}",
            f"--datadir={self.data_path}",
        ])

        self.logger.info("setting root password to 'password'...")

        handle = self.__exec_nonblock__(mysqld, [
            f"--defaults-file={self.config_path}",
            f"--user={username}",
            f"--basedir={self.base_path}",
            f"--datadir={self.data_path}",
            f"--port={self.port}",
        ])

        time.sleep(5)


        retval = self.__exec__(self.bin_for("mysql"), [
            "-h127.0.0.1",
            f"--port={self.port}",
            "-uroot",
            "--skip-password",
            "-e", "ALTER USER 'root'@'localhost' IDENTIFIED BY 'password';"
                  "CREATE DATABASE benchbase;"
                  "CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';"
                  "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';"
                  "FLUSH PRIVILEGES;"
        ])

        if retval != 0:
            handle.send_signal(9)

            self.logger.error("failed to set root password")
            raise Exception("failed to set root password")

        # send SIGTERM to the daemon
        handle.send_signal(15)
        handle.wait()

        self.logger.info("MySQL data directory is ready")


    def start(self) -> _MySQLDaemonSession:
        """
        starts the MySQL daemon and returns a context manager that
        stops it on scope exit.
        """

        if self.mysqld_handle is not None:
            self.logger.error("MySQL daemon is already running")
            return _MySQLDaemonSession(self, False)

        username = get_current_user()
        mysqld = self.bin_for("mysqld")

        self.mysqld_handle = self.__exec_nonblock__(mysqld, [
            f"--defaults-file={self.config_path}",
            f"--user={username}",
            f"--basedir={self.base_path}",
            f"--datadir={self.data_path}",
            f"--port={self.port}",
        ])

        return _MySQLDaemonSession(self, True)

    def stop(self, timeout=None) -> bool:
        """
        stops the MySQL daemon.
        returns True if the daemon was stopped successfully, False otherwise.
        """

        if self.mysqld_handle is None:
            self.logger.error("MySQL daemon is not running")
            return False

        self.mysqld_handle.send_signal(15)
        self.mysqld_handle.wait(timeout=timeout)

        self.mysqld_handle = None

        return True
