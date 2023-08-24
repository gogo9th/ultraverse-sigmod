import logging
import os
import subprocess
import time
import signal

from datetime import datetime

from esperanza.mysql.mysqld import MySQLDaemon
from esperanza.utils.logger import get_logger

class BenchmarkSession:

    logger: logging.Logger

    bench_name: str
    bench_date: datetime
    amount: str

    session_path: str
    mysqld: MySQLDaemon

    @staticmethod
    def get_runs_dirname(bench_name: str, bench_date: datetime, amount: str) -> str:
        return f"{os.getcwd()}/runs/{bench_name}-{amount}-{bench_date.strftime('%Y%m%d%H%M%S')}"

    def __init__(self, bench_name: str, amount: str, session_path: str = None):
        self.logger = get_logger(f"BenchmarkSession:{bench_name}-{amount}")

        self.bench_name = bench_name
        self.bench_date = datetime.now()
        self.amount = amount

        if session_path is None:
            self.session_path = BenchmarkSession.get_runs_dirname(self.bench_name, self.bench_date, self.amount)
        else:
            self.session_path = session_path

        self.mysqld = MySQLDaemon(3306, f"{self.session_path}/mysql")

        # setup sigint handler

        def sigint_handler(sig, frame):
            self.logger.info("SIGINT received, stopping...")
            self.mysqld.stop()
            exit(0)

        signal.signal(signal.SIGINT, sigint_handler)

    def run_benchbase(self, args: list[str]):
        """
        runs benchbase with the given arguments.
        """
        benchbase_home = os.environ['BENCHBASE_HOME']

        cwd = os.getcwd()

        os.chdir(benchbase_home)

        retval = subprocess.call(
            [f"{benchbase_home}/run-mariadb"] + args,
        )

        os.chdir(cwd)

        if retval != 0:
            raise Exception("failed to run benchbase")

    def prepare(self):
        """
        prepares the benchmark session.
        """

        self.logger.info("preparing benchmark session...")

        self.mysqld.prepare()
        time.sleep(5)

        self.mysqld.start()
        time.sleep(5)

        self.run_benchbase([self.bench_name, 'mariadb', self.amount, 'prepare'])
        time.sleep(5)

        self.logger.info("dumping database...")

        # checkpoint
        self.mysqld.mysqldump("benchbase", f"{self.session_path}/dbdump.sql")

        self.mysqld.stop()


        # fill data
        self.mysqld.flush_binlogs()

        self.mysqld.start()
        time.sleep(5)

        self.run_benchbase([self.bench_name, 'mariadb', self.amount, 'execute'])
        time.sleep(5)

        self.mysqld.stop()

        os.system(f"mv -v {self.session_path}/mysql/server-binlog.* {self.session_path}/")
        os.system(f"cp -rv {os.getcwd()}/procdefs/${self.bench_name} {self.session_path}/procdef")

    def run_statelogd(self):
        pass

    def run_db_state_change(self, args: list[str]):
        pass

    def tablediff(self, table1: str, table2: str, columns: list[str]):
        """
        compares the given tables.
        """
        self.logger.info(f"comparing tables '{table1}' and '{table2}'...")

        columns_str = ", ".join(columns)
        columns_t1 = ", ".join(list(map(lambda c: f"{table1}.{c}", columns)))
        columns_t2 = ", ".join(list(map(lambda c: f"{table2}.{c}", columns)))

        base_sql = (f"SELECT '{table1}' as `set`, t1.*"
                    f"    FROM {table1} t1"
                    f"    WHERE ROW({columns_t1}) NOT IN"
                    f"    (SELECT {columns_str} FROM {table2})"
                    f"UNION ALL"
                    f"SELECT '{table2}' as `set`, t2.*"
                    f"    FROM {table2} t2"
                    f"    WHERE ROW({columns_t2}) NOT IN"
                    f"    (SELECT {columns_str} FROM {table1})")

        sql = (f"SELECT CONCAT(\"found \", COUNT(*), \" differences\")"
               f"FROM ({base_sql})")


        # run mysql and get stdout into variable
        retval = subprocess.call([
            'mysql',
            '-h127.0.0.1',
            '-uroot',
            '-ppassword',
            '--raw',
            '-e', sql
        ])

        if retval != 0:
            raise Exception("failed to compare tables")