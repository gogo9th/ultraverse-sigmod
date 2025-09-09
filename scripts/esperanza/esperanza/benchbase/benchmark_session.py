import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

import fcntl
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
            self.mysqld.stop(5)
            exit(0)

        signal.signal(signal.SIGINT, sigint_handler)

    def run_benchbase(self, args: list[str]):
        """
        runs benchbase with the given arguments.
        """
        benchbase_home = os.environ['BENCHBASE_HOME']

        cwd = os.getcwd()

        os.chdir(benchbase_home)

        handle = subprocess.Popen(
            [f"{benchbase_home}/run-mariadb"] + args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE
        )

        # Q: should i use handle.communicate() instead of handle.stdout.read()?
        # A: no, because handle.communicate() waits for the process to finish, which is not what we want.
        #    we want to read the output while the process is running.

        with open(f"{self.session_path}/benchbase.log", 'w') as f:
            while True:
                line = handle.stdout.readline()
                if not line:
                    break
                print("\33[2K\r", end='')
                print(line.decode('utf-8').strip(), end='')
                sys.stdout.flush()
                f.write(line.decode('utf-8'))

        print()

        retval = handle.wait()

        os.chdir(cwd)

        if retval != 0:
            raise Exception("failed to run benchbase")

    def run_statelogd(self, args: list[str] = []):
        """
        runs statelogd.
        """
        ultraverse_home = os.environ['ULTRAVERSE_HOME']

        self.logger.info("running statelogd...")

        cwd = os.getcwd()
        os.chdir(self.session_path)

        handle = subprocess.Popen([
            f"{ultraverse_home}/statelogd",

            '-b', 'server-binlog.index',
            '-o', 'benchbase',

            '-n'
        ] + args, stderr=subprocess.PIPE)

        with open(f"{self.session_path}/statelogd.log", 'w') as f:
            while True:
                line = handle.stderr.readline()
                if not line:
                    break
                print("\33[2K\r", end='')
                print(line.decode('utf-8').strip(), end='')
                sys.stdout.flush()
                f.write(line.decode('utf-8'))

        print()

        retval = handle.wait()

        os.chdir(cwd)

        if retval != 0:
            raise Exception("failed to run statelogd: process exited with non-zero code " + str(retval))


    def run_db_state_change(self, args: list[str], stdout_name: str="db_state_change.stdout", stderr_name: str="db_state_change.stderr", pipe_stdin_file=None):
        """
        runs db_state_change.
        """

        def set_nonblock_io(io):
            fd = io.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        ultraverse_home = os.environ['ULTRAVERSE_HOME']

        self.logger.info("running db_state_change with args: " + " ".join(args))

        cwd = os.getcwd()
        os.chdir(self.session_path)

        handle = subprocess.Popen(
            [f"{ultraverse_home}/db_state_change"] + args,
            stdin=subprocess.PIPE if pipe_stdin_file is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        set_nonblock_io(handle.stdout)
        set_nonblock_io(handle.stderr)

        if pipe_stdin_file is not None:
            with open(pipe_stdin_file, 'r') as stdin_f:
                while True:
                    line = stdin_f.readline()
                    if not line:
                        break
                    handle.stdin.write(line.encode('utf-8'))
            handle.stdin.close()

        ends_with_newline = False

        with open(f"{self.session_path}/{stdout_name}", 'w') as stdout_f, \
             open(f"{self.session_path}/{stderr_name}", 'w') as stderr_f:
            while handle.poll() is None:
                time.sleep(0.016)
                out_line = handle.stdout.read()
                err_line = handle.stderr.read()

                if out_line:
                    stdout_f.write(out_line.decode('utf-8'))
                    stdout_f.flush()

                if err_line:
                    # print("\33[2K\r", end='')
                    # print(err_line.decode('utf-8').strip(), end='\n')
                    # sys.stdout.flush()

                    line_str = err_line.decode('utf-8')
                    line = line_str.splitlines(keepends=False)[-1]

                    if ends_with_newline:
                        sys.stdout.write("\33[2K\r")
                        sys.stdout.flush()

                    ends_with_newline = line_str.endswith('\n')

                    sys.stdout.write(line)
                    sys.stdout.flush()
                    stderr_f.write(err_line.decode('utf-8'))
                    stderr_f.flush()

        handle.stdout.close()
        handle.stderr.close()

        print()

        retval = handle.wait()

        os.chdir(cwd)

        if retval != 0:
            raise Exception("failed to run db_state_change: process exited with non-zero code " + str(retval))

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

        self.mysqld.stop()

        self.mysqld.start()
        time.sleep(5)

        # checkpoint
        self.logger.info("dumping database...")
        self.mysqld.mysqldump("benchbase", f"{self.session_path}/dbdump.sql")

        self.mysqld.stop()

        # execute
        self.mysqld.flush_binlogs()

        self.mysqld.start()
        time.sleep(5)

        self.run_benchbase([self.bench_name, 'mariadb', self.amount, 'execute'])
        time.sleep(5)

        self.mysqld.stop()

        self.mysqld.start()
        time.sleep(5)

        self.logger.info("dumping database...")
        self.mysqld.mysqldump("benchbase", f"{self.session_path}/dbdump_latest.sql")

        self.mysqld.stop()


        os.system(f"mv -v {self.session_path}/mysql/server-binlog.* {self.session_path}/")
        os.system(f"mkdir -p {self.session_path}/procdef")
        os.system(f"cp -rv {os.getcwd()}/procdefs/{self.bench_name}/* {self.session_path}/procdef/")



    def tablediff(self, table1: str, table2: str, columns: list[str]):
        """
        compares the given tables.
        """
        self.logger.info(f"comparing tables '{table1}' and '{table2}'...")

        columns_str = ", ".join(list(map(lambda c: f"`{c}`", columns)))
        columns_t1 = ", ".join(list(map(lambda c: f"t1.`{c}`", columns)))
        columns_t2 = ", ".join(list(map(lambda c: f"t2.`{c}`", columns)))

        base_sql = (f"SELECT '{table1}' as `set`, t1.*"
                    f"    FROM {table1} t1"
                    f"    WHERE ROW({columns_t1}) NOT IN"
                    f"    (SELECT {columns_str} FROM {table2})"
                    f"UNION ALL "
                    f"SELECT '{table2}' as `set`, t2.*"
                    f"    FROM {table2} t2"
                    f"    WHERE ROW({columns_t2}) NOT IN"
                    f"    (SELECT {columns_str} FROM {table1})")

        sql = (f"SELECT CONCAT(\"found \", COUNT(*), \" differences\")"
               f"FROM ({base_sql}) d")

        # run mysql and get stdout into variable
        handle = subprocess.Popen([
            'mysql',
            '-h127.0.0.1',
            '-uroot',
            '-ppassword',
            '-B', '--silent', '--raw',
        ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        handle.stdin.write(sql.encode('utf-8'))
        handle.stdin.close()

        stdout = handle.stdout.read()

        if stdout:
            print(stdout.decode('utf-8').strip())

        retval = handle.wait()

        if retval != 0:
            raise Exception("failed to compare tables")

    def load_dump(self, db_name: str, sqlfile: str):
        """
        loads the given sql dump.
        """
        self.logger.info(f"loading dump '{sqlfile}' into {db_name}...")

        self.eval(f"DROP DATABASE IF EXISTS {db_name}")
        self.eval(f"CREATE DATABASE {db_name}")

        # run mysql and get stdout into variable
        retval = subprocess.call([
            'mysql',
            '-h127.0.0.1',
            '-uroot',
            '-ppassword',
            db_name,
            '-B', '--silent', '--raw',
            '-e', f"source {sqlfile}"
        ], stdout=subprocess.DEVNULL)

        if retval != 0:
            raise Exception("failed to load dump")

    def eval(self, sql: str) -> bool:
        handle = subprocess.Popen([
            'mysql',
            '-h127.0.0.1',
            '-uroot',
            '-ppassword',
            '-B', '--silent', '--raw',
        ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        handle.stdin.write(sql.encode('utf-8'))
        handle.stdin.close()

        stdout = handle.stdout.read()

        if stdout:
            print(stdout.decode('utf-8').strip())

        retval = handle.wait()

        return retval == 0
