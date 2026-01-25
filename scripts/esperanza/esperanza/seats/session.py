from __future__ import annotations

import fcntl
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import mysql.connector

from esperanza.mysql.mysqld import MySQLDaemon
from esperanza.seats.constants import DEFAULT_NUM_CUSTOMERS, DEFAULT_QUERY_COUNT, NUM_AIRPORTS
from esperanza.seats.data_generator import SeatsDataGenerator
from esperanza.seats.workload import SeatsWorkloadExecutor
from esperanza.utils.logger import get_logger


class SeatsStandaloneSession:
    def __init__(
        self,
        name: str,
        scale: str,
        num_airports: int = NUM_AIRPORTS,
        num_customers: int = DEFAULT_NUM_CUSTOMERS,
        query_count: int = DEFAULT_QUERY_COUNT,
    ) -> None:
        self.name = name
        self.scale = scale
        self.num_airports = num_airports
        self.num_customers = num_customers
        self.query_count = query_count

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.session_path = str(Path.cwd() / "runs" / f"{name}-{scale}-{timestamp}")
        Path(self.session_path).mkdir(parents=True, exist_ok=True)

        self.logger = get_logger(f"SeatsStandaloneSession:{name}-{scale}")

        port = int(os.getenv("DB_PORT", "3306"))
        self.mysqld = MySQLDaemon(port, os.path.join(self.session_path, "mysql"))

        def sigint_handler(sig, frame):
            self.logger.info("SIGINT received, stopping...")
            self.mysqld.stop(5)
            raise SystemExit(0)

        signal.signal(signal.SIGINT, sigint_handler)

    @staticmethod
    def report_name_from_stdout(stdout_name: str) -> str:
        if stdout_name.endswith(".stdout"):
            return f"{stdout_name[:-7]}.report.json"
        return f"{stdout_name}.report.json"

    def _config_path(self) -> str:
        return f"{self.session_path}/ultraverse.json"

    def _generate_config(
        self,
        key_columns: list[str],
        backup_file: str = "dbdump.sql",
        column_aliases: dict[str, list[str]] | None = None,
        development_flags: list[str] | None = None,
    ) -> dict:
        if column_aliases is None:
            column_aliases = {}
        if development_flags is None:
            development_flags = []

        return {
            "binlog": {"path": ".", "indexName": "server-binlog.index"},
            "stateLog": {"path": ".", "name": "benchbase"},
            "keyColumns": key_columns,
            "columnAliases": column_aliases,
            "database": {
                "host": os.environ.get("DB_HOST", "127.0.0.1"),
                "port": int(os.environ.get("DB_PORT", "3306")),
                "name": "benchbase",
                "username": os.environ.get("DB_USER", "admin"),
                "password": os.environ.get("DB_PASS", "password"),
            },
            "statelogd": {
                "threadCount": 0,
                "oneshotMode": True,
                "procedureLogPath": "",
                "developmentFlags": development_flags,
            },
            "stateChange": {
                "threadCount": 0,
                "backupFile": backup_file,
                "keepIntermediateDatabase": False,
                "rangeComparisonMethod": "eqonly",
            },
        }

    def _write_config(self, config: dict) -> None:
        with open(self._config_path(), "w") as f:
            json.dump(config, f, indent=2)

    def _read_config(self) -> dict:
        with open(self._config_path(), "r") as f:
            return json.load(f)

    def update_config(
        self,
        key_columns: list[str] | None = None,
        backup_file: str | None = None,
        column_aliases: dict[str, list[str]] | None = None,
        development_flags: list[str] | None = None,
        keep_intermediate_database: bool | None = None,
    ) -> None:
        config = self._read_config()

        if key_columns is not None:
            config["keyColumns"] = key_columns
        if column_aliases is not None:
            config["columnAliases"] = column_aliases
        if backup_file is not None:
            config.setdefault("stateChange", {})["backupFile"] = backup_file
        if keep_intermediate_database is not None:
            config.setdefault("stateChange", {})["keepIntermediateDatabase"] = keep_intermediate_database
        if development_flags is not None:
            config.setdefault("statelogd", {})["developmentFlags"] = development_flags

        self._write_config(config)

    def prepare(self) -> None:
        """Initialize MySQL, load schema/data, and take a checkpoint dump."""
        self.logger.info("preparing Seats standalone session...")

        self.mysqld.prepare()
        time.sleep(5)
        with self.mysqld.start():
            time.sleep(5)

            self._create_database()
            self._execute_ddl()
            self._load_data()

            self.mysqld.mysqldump("benchbase", f"{self.session_path}/dbdump.sql")

        self.mysqld.flush_binlogs()

    def execute_workload(self) -> dict:
        """Run the SEATS workload."""
        conn = self._get_connection()
        executor = SeatsWorkloadExecutor(
            conn,
            self.num_airports,
            self.num_customers,
            self.query_count,
        )
        stats = executor.run()
        conn.close()
        return stats

    def finalize(self) -> None:
        """Finalize session artifacts after workload (mysqld should be stopped)."""
        self._move_binlogs()

    def run_statelogd(self, key_columns: list[str], development_flags: list[str] | None = None) -> None:
        """Run statelogd with the generated config."""
        ultraverse_home = os.environ["ULTRAVERSE_HOME"]

        self.logger.info("running statelogd...")

        cwd = os.getcwd()
        os.chdir(self.session_path)

        self._ensure_procdefs()

        config = self._generate_config(
            key_columns=key_columns,
            development_flags=development_flags,
        )
        self._write_config(config)

        handle = subprocess.Popen([
            f"{ultraverse_home}/statelogd",
            "-c", self._config_path(),
        ], stderr=subprocess.PIPE)

        with open(f"{self.session_path}/statelogd.log", "w") as f:
            while True:
                line = handle.stderr.readline()
                if not line:
                    break
                print("\33[2K\r", end="")
                text = line.decode("utf-8", errors="replace")
                print(text.strip(), end="")
                sys.stdout.flush()
                f.write(text)

        print()

        retval = handle.wait()

        os.chdir(cwd)

        if retval != 0:
            raise Exception("failed to run statelogd: process exited with non-zero code " + str(retval))

    def run_db_state_change(
        self,
        action: str,
        gid_range: tuple[int, int] | None = None,
        skip_gids: list[int] | None = None,
        replay_from: int | None = None,
        dry_run: bool = False,
        stdout_name: str = "db_state_change.stdout",
        stderr_name: str = "db_state_change.stderr",
    ) -> None:
        """Run db_state_change with the given action."""

        def set_nonblock_io(io):
            fd = io.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        ultraverse_home = os.environ["ULTRAVERSE_HOME"]

        self.logger.info(f"running db_state_change with action: {action}")

        cwd = os.getcwd()
        os.chdir(self.session_path)

        config = self._read_config()
        config.setdefault("stateChange", {})
        if not config["stateChange"].get("backupFile"):
            config["stateChange"]["backupFile"] = "dbdump.sql"

        config["stateChange"]["keepIntermediateDatabase"] = "replay" in action
        self._write_config(config)

        args = []
        if gid_range is not None:
            args += ["--gid-range", f"{gid_range[0]}...{gid_range[1]}"]
        if skip_gids:
            args += ["--skip-gids", ",".join(map(str, skip_gids))]
        if replay_from is not None:
            args += ["--replay-from", str(replay_from)]
        if dry_run:
            args.append("--dry-run")

        args += [self._config_path(), action]

        report_name = SeatsStandaloneSession.report_name_from_stdout(stdout_name)
        env = os.environ.copy()
        env["ULTRAVERSE_REPORT_NAME"] = f"{self.session_path}/{report_name}"

        handle = subprocess.Popen(
            [f"{ultraverse_home}/db_state_change"] + args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

        set_nonblock_io(handle.stdout)
        set_nonblock_io(handle.stderr)

        ends_with_newline = False

        with open(f"{self.session_path}/{stdout_name}", "w") as stdout_f, \
             open(f"{self.session_path}/{stderr_name}", "w") as stderr_f:
            while handle.poll() is None:
                time.sleep(0.016)
                out_line = handle.stdout.read()
                err_line = handle.stderr.read()

                if out_line:
                    stdout_f.write(out_line.decode("utf-8"))
                    stdout_f.flush()

                if err_line:
                    line_str = err_line.decode("utf-8")
                    line = line_str.splitlines(keepends=False)[-1]

                    if ends_with_newline:
                        sys.stdout.write("\33[2K\r")
                        sys.stdout.flush()

                    ends_with_newline = line_str.endswith("\n")

                    sys.stdout.write(line)
                    sys.stdout.flush()
                    stderr_f.write(err_line.decode("utf-8"))
                    stderr_f.flush()

        handle.stdout.close()
        handle.stderr.close()

        print()

        retval = handle.wait()

        os.chdir(cwd)

        if retval != 0:
            raise Exception("failed to run db_state_change: process exited with non-zero code " + str(retval))

    def tablediff(
        self,
        table1: str,
        table2: str,
        columns: list[str],
        float_columns: list[str] | None = None,
        epsilon: float | None = None,
    ) -> None:
        self.logger.info(f"comparing tables '{table1}' and '{table2}'...")

        float_columns_set = set(float_columns or [])
        if epsilon is None:
            epsilon = self._env_float("ESPERANZA_TABLEDIFF_EPS", default=1e-6)
        if epsilon < 0:
            epsilon = 0.0
        eps_literal = f"{epsilon:.17g}"

        if float_columns_set and epsilon > 0:
            def _join_predicate(col: str) -> str:
                if col in float_columns_set:
                    return (
                        f"((t1.`{col}` IS NULL AND t2.`{col}` IS NULL) OR "
                        f"(t1.`{col}` IS NOT NULL AND t2.`{col}` IS NOT NULL "
                        f"AND ABS(t1.`{col}` - t2.`{col}`) <= {eps_literal}))"
                    )
                return f"t1.`{col}` <=> t2.`{col}`"

            join_pred = " AND ".join([_join_predicate(c) for c in columns])
        else:
            join_pred = " AND ".join([f"t1.`{c}` <=> t2.`{c}`" for c in columns])
        show_details = self._env_truthy("ESPERANZA_TABLEDIFF_DETAILS", default=False)
        detail_limit = max(0, self._env_int("ESPERANZA_TABLEDIFF_LIMIT", 20))

        base_sql = (
            f"SELECT '{table1}' as `set`, t1.*"
            f"    FROM {table1} t1"
            f"    WHERE NOT EXISTS"
            f"    (SELECT 1 FROM {table2} t2 WHERE {join_pred})"
            f"UNION ALL "
            f"SELECT '{table2}' as `set`, t2.*"
            f"    FROM {table2} t2"
            f"    WHERE NOT EXISTS"
            f"    (SELECT 1 FROM {table1} t1 WHERE {join_pred})"
        )

        sql = (
            f"SELECT CONCAT(\"found \", COUNT(*), \" differences\")"
            f"FROM ({base_sql}) d"
        )

        handle = subprocess.Popen(
            [self.mysqld.bin_for("mysql")] + self._mysql_admin_args() + [
                "-B", "--silent", "--raw",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        handle.stdin.write(sql.encode("utf-8"))
        handle.stdin.close()

        stdout = handle.stdout.read()

        if stdout:
            print(stdout.decode("utf-8").strip())

        retval = handle.wait()

        if retval != 0:
            raise Exception("failed to compare tables")

        if show_details and detail_limit > 0:
            self.logger.info(
                f"showing up to {detail_limit} diff rows for '{table1}' vs '{table2}'..."
            )
            detail_sql = f"SELECT * FROM ({base_sql}) d LIMIT {detail_limit}"
            handle = subprocess.Popen(
                [self.mysqld.bin_for("mysql")] + self._mysql_admin_args() + [
                    "--table",
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            handle.stdin.write(detail_sql.encode("utf-8"))
            handle.stdin.close()

            stdout = handle.stdout.read()

            if stdout:
                print(stdout.decode("utf-8").strip())

            retval = handle.wait()

            if retval != 0:
                raise Exception("failed to print table differences")

    def _create_database(self) -> None:
        conn = self._get_connection(database=None)
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS benchbase")
        cursor.execute("CREATE DATABASE benchbase")
        cursor.close()
        conn.close()

    def _execute_ddl(self) -> None:
        ddl_path = Path(__file__).parent.parent.parent / "seats_sql" / "ddl-mysql.sql"
        if not ddl_path.exists():
            raise FileNotFoundError(f"DDL file not found: {ddl_path}")

        cmd = [self.mysqld.bin_for("mysql")] + self._mysql_admin_args() + ["benchbase"]
        result = subprocess.run(
            cmd,
            input=ddl_path.read_bytes(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if result.returncode != 0:
            output = result.stdout.decode("utf-8", errors="replace")
            raise Exception(f"failed to execute DDL: {output}")

    def _load_data(self) -> None:
        conn = self._get_connection()
        generator = SeatsDataGenerator(conn, self.num_customers)
        generator.generate_all()
        conn.close()

    def _get_connection(self, database: str | None = "benchbase"):
        kwargs = {
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "user": os.getenv("DB_USER", "admin"),
            "password": os.getenv("DB_PASS", "password"),
            "autocommit": False,
            "consume_results": True,
        }
        if database is not None:
            kwargs["database"] = database
        return mysql.connector.connect(**kwargs)

    def _mysql_admin_args(self) -> list[str]:
        host = os.environ.get("DB_HOST", "127.0.0.1")
        port = os.environ.get("DB_PORT", "3306")
        user = os.environ.get("DB_USER", "admin")
        password = os.environ.get("DB_PASS", "password")
        return [f"-h{host}", f"--port={port}", f"-u{user}", f"-p{password}", "--protocol=tcp"]

    @staticmethod
    def _env_truthy(name: str, default: bool = False) -> bool:
        value = os.environ.get(name)
        if value is None:
            return default
        return value.strip().lower() in ("1", "true", "yes", "y", "on")

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        value = os.environ.get(name)
        if value is None or value.strip() == "":
            return default
        try:
            return int(value)
        except ValueError:
            return default

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        value = os.environ.get(name)
        if value is None or value.strip() == "":
            return default
        try:
            return float(value)
        except ValueError:
            return default

    def _move_binlogs(self) -> None:
        binlog_dir = Path(self.mysqld.data_path)
        if not binlog_dir.exists():
            return

        for binlog_path in binlog_dir.glob("server-binlog.*"):
            dest = Path(self.session_path) / binlog_path.name
            shutil.move(str(binlog_path), str(dest))

    def _ensure_procdefs(self) -> None:
        src_dir = Path(__file__).parent.parent.parent / "procdefs" / "seats"
        if not src_dir.exists():
            self.logger.warning(f"procdefs not found: {src_dir}")
            return

        dst_dir = Path(self.session_path) / "procdef"
        dst_dir.mkdir(parents=True, exist_ok=True)

        copied = 0
        for proc_path in src_dir.glob("*.sql"):
            shutil.copy2(proc_path, dst_dir / proc_path.name)
            copied += 1

        if copied == 0:
            self.logger.warning(f"no procdef files found in {src_dir}")
        else:
            self.logger.info(f"copied {copied} procdef files to {dst_dir}")
