import os
import re
import shutil
import subprocess
import time

from esperanza.benchbase.benchmark_session import BenchmarkSession
from esperanza.utils.download_mysql import download_mysql, get_mysql_bin_path
from esperanza.utils.state_change_report import read_state_change_report

KEY_COLUMNS = [
    'items.id',
    'orders.order_id',
]
BACKUP_FILE = "dbdump.sql"
COLUMN_ALIASES: dict[str, list[str]] = {}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
MINISHOP_DIR = os.path.join(SCRIPT_DIR, "minishop")
PROCDEF_DIR = os.path.join(SCRIPT_DIR, "procdefs", "minishop")

SCHEMA_SQL = os.path.join(MINISHOP_DIR, "schema.sql")
PROCEDURES_SQL = os.path.join(MINISHOP_DIR, "procedures.sql")
SCENARIO_SQL = os.path.join(MINISHOP_DIR, "scenario.sql")

PREPEND_SQL_FILENAME = "prepend_partial_refund.sql"

EXPECTED_STOCK_ITEM1 = 11
EXPECTED_STOCK_ITEM2 = 6
EXPECTED_ORDER_STATUS = "REFUNDED"
EXPECTED_REFUND_COUNT = 1
EXPECTED_REFUND_AMOUNT = 100
EXPECTED_REPLAY_GID_COUNT = 1


def mysql_args(session: BenchmarkSession, database: str | None = None, no_header: bool = False) -> list[str]:
    args = [
        session.mysqld.bin_for("mysql"),
        "-h127.0.0.1",
        f"--port={session.mysqld.port}",
        "-uroot",
        "-ppassword",
        "-B",
        "--silent",
        "--raw",
    ]
    if no_header:
        args.append("-N")
    if database:
        args.append(database)
    return args


def mysql_source(session: BenchmarkSession, sql_path: str, database: str | None = None) -> None:
    args = mysql_args(session, database)
    args += ["-e", f"source {sql_path}"]
    subprocess.check_call(args, stdout=subprocess.DEVNULL)


def mysql_scalar(session: BenchmarkSession, sql: str, database: str = "benchbase") -> str:
    args = mysql_args(session, database, no_header=True)
    args += ["-e", sql]
    output = subprocess.check_output(args)
    return output.decode("utf-8").strip()


def copy_procdefs(session: BenchmarkSession) -> None:
    dst_dir = os.path.join(session.session_path, "procdef")
    os.makedirs(dst_dir, exist_ok=True)
    for name in os.listdir(PROCDEF_DIR):
        if not name.endswith(".sql"):
            continue
        shutil.copy2(os.path.join(PROCDEF_DIR, name), dst_dir)

def patch_procedures(session: BenchmarkSession) -> tuple[str, str]:
    patch_dir = os.path.join(session.session_path, "procpatcher")
    os.makedirs(patch_dir, exist_ok=True)

    patched_sql = os.path.join(patch_dir, "procedures.patched.sql")
    helper_sql = os.path.join(patch_dir, "__ultraverse__helper.sql")
    procpatcher_dir = os.path.join(REPO_ROOT, "procpatcher")

    subprocess.check_call(
        ["go", "run", ".", PROCEDURES_SQL, patched_sql],
        cwd=procpatcher_dir,
    )

    if not os.path.exists(helper_sql):
        raise RuntimeError("procpatcher did not generate __ultraverse__helper.sql")

    return patched_sql, helper_sql


def write_prepend_sql(session: BenchmarkSession, order_id: int) -> str:
    sql_path = os.path.join(session.session_path, PREPEND_SQL_FILENAME)
    with open(sql_path, "w") as sql_file:
        sql_file.write(
            "UPDATE orders\n"
            "SET status = 'REFUNDED'\n"
            f"WHERE order_id = {order_id};\n"
            "\n"
            "UPDATE items\n"
            "SET stock = stock + 1\n"
            "WHERE id = 1;\n"
            "\n"
            "INSERT INTO refunds(order_id, amount)\n"
            f"VALUES ({order_id}, 100);\n"
        )
    return sql_path


def find_gid_by_query(session: BenchmarkSession, needles: list[str]) -> int:
    ultraverse_home = os.environ.get("ULTRAVERSE_HOME")
    if not ultraverse_home:
        raise RuntimeError("ULTRAVERSE_HOME is not set")

    viewer_path = os.path.join(ultraverse_home, "state_log_viewer")
    result = subprocess.run(
        [viewer_path, "-i", "benchbase"],
        cwd=session.session_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError("state_log_viewer failed")

    current_gid = None
    for line in result.stdout.splitlines():
        match = re.search(r"Transaction #(\d+)", line)
        if match:
            current_gid = int(match.group(1))
            continue
        if current_gid is not None and any(needle in line for needle in needles):
            return current_gid

    raise RuntimeError(f"failed to find gid for queries containing {', '.join(needles)}")


def perform_state_change(session: BenchmarkSession, rollback_gid: int, prepend_sql_path: str):
    logger = session.logger

    rollback_log_name = f"rollback_prepend_{rollback_gid}"
    rollback_stdout_name = rollback_log_name + ".stdout"
    rollback_stderr_name = rollback_log_name + ".stderr"
    rollback_report_name = session.report_name_from_stdout(rollback_stdout_name)

    replay_log_name = rollback_log_name + ".replay"
    replay_stdout_name = replay_log_name + ".stdout"
    replay_stderr_name = replay_log_name + ".stderr"
    replay_report_name = session.report_name_from_stdout(replay_stdout_name)

    rollback_action = f"rollback={rollback_gid}:prepend={rollback_gid},{os.path.basename(prepend_sql_path)}"
    session.run_db_state_change(
        rollback_action,
        replay_from=0,
        stdout_name=rollback_stdout_name,
        stderr_name=rollback_stderr_name,
    )

    rollback_report = read_state_change_report(f"{session.session_path}/{rollback_report_name}")

    session.run_db_state_change(
        "replay",
        replay_from=0,
        stdout_name=replay_stdout_name,
        stderr_name=replay_stderr_name,
    )

    replay_report = read_state_change_report(f"{session.session_path}/{replay_report_name}")
    logger.info(f"State Change Report: {rollback_log_name}")
    logger.info(f"prepare: {rollback_report['executionTime']}, replay: {replay_report['executionTime']}")

    return rollback_report, replay_report


def verify_rowwise_skip(rollback_report: dict) -> None:
    replay_count = rollback_report.get("replayGidCount")
    if replay_count is None:
        raise RuntimeError("rollback report missing replayGidCount")

    if replay_count != EXPECTED_REPLAY_GID_COUNT:
        raise RuntimeError(
            f"expected replayGidCount={EXPECTED_REPLAY_GID_COUNT} for row-wise skip, got {replay_count}"
        )


def verify_results(session: BenchmarkSession, order_id: int) -> None:
    stock_item1 = int(mysql_scalar(session, "SELECT stock FROM items WHERE id = 1;"))
    stock_item2 = int(mysql_scalar(session, "SELECT stock FROM items WHERE id = 2;"))
    order_status = mysql_scalar(
        session,
        f"SELECT status FROM orders WHERE order_id = {order_id};",
    )
    refund_count = int(mysql_scalar(
        session,
        f"SELECT COUNT(*) FROM refunds WHERE order_id = {order_id};",
    ))

    if stock_item1 != EXPECTED_STOCK_ITEM1:
        raise RuntimeError(f"items.id=1 stock expected {EXPECTED_STOCK_ITEM1}, got {stock_item1}")
    if stock_item2 != EXPECTED_STOCK_ITEM2:
        raise RuntimeError(f"items.id=2 stock expected {EXPECTED_STOCK_ITEM2}, got {stock_item2}")
    if order_status != EXPECTED_ORDER_STATUS:
        raise RuntimeError(f"order status expected {EXPECTED_ORDER_STATUS}, got {order_status}")
    if refund_count != EXPECTED_REFUND_COUNT:
        raise RuntimeError(f"refund count expected {EXPECTED_REFUND_COUNT}, got {refund_count}")
    if refund_count == EXPECTED_REFUND_COUNT:
        refund_amount = int(mysql_scalar(
            session,
            f"SELECT amount FROM refunds WHERE order_id = {order_id};",
        ))
        if refund_amount != EXPECTED_REFUND_AMOUNT:
            raise RuntimeError(f"refund amount expected {EXPECTED_REFUND_AMOUNT}, got {refund_amount}")


if __name__ == "__main__":
    if not download_mysql():
        print("MySQL distribution is not available")
        exit(1)

    os.environ["MYSQL_BIN_PATH"] = get_mysql_bin_path()

    os.putenv("DB_HOST", "127.0.0.1")
    os.putenv("DB_PORT", "3306")
    os.putenv("DB_USER", "admin")
    os.putenv("DB_PASS", "password")

    session = BenchmarkSession("minishop", "mini")
    logger = session.logger

    session.mysqld.prepare()
    time.sleep(5)

    logger.info("starting mysqld...")
    with session.mysqld.start():
        time.sleep(5)

        logger.info("creating schema/procedures...")
        mysql_source(session, SCHEMA_SQL)

        patched_sql, helper_sql = patch_procedures(session)
        mysql_source(session, helper_sql, "benchbase")
        mysql_source(session, patched_sql, "benchbase")

        logger.info("dumping checkpoint...")
        session.mysqld.mysqldump("benchbase", f"{session.session_path}/dbdump.sql")

        logger.info("stopping mysqld...")

    session.mysqld.flush_binlogs()

    logger.info("starting mysqld for scenario...")
    with session.mysqld.start():
        time.sleep(5)

        logger.info("running scenario...")
        mysql_source(session, SCENARIO_SQL)

        logger.info("stopping mysqld...")

    logger.info("starting mysqld for dump...")
    with session.mysqld.start():
        time.sleep(5)

        logger.info("dumping latest database...")
        session.mysqld.mysqldump("benchbase", f"{session.session_path}/dbdump_latest.sql")

        order_id = int(mysql_scalar(
            session,
            "SELECT MIN(order_id) FROM orders WHERE item_id = 1;",
        ))
        logger.info(f"order id for prepend: {order_id}")

        logger.info("stopping mysqld...")

    os.system(f"mv -v {session.session_path}/mysql/server-binlog.* {session.session_path}/")

    copy_procdefs(session)

    session.run_statelogd(key_columns=KEY_COLUMNS)
    session.update_config(backup_file=BACKUP_FILE, column_aliases=COLUMN_ALIASES)

    logger.info("starting mysqld for state change...")
    with session.mysqld.start():
        time.sleep(10)

        logger.info("creating cluster...")
        session.run_db_state_change("make_cluster")

        refund_gid = find_gid_by_query(session, ["refund_item", "refunds", "REFUNDED"])
        logger.info(f"refund gid: {refund_gid}")

        prepend_sql_path = write_prepend_sql(session, order_id)
        rollback_report, _replay_report = perform_state_change(session, refund_gid, prepend_sql_path)
        verify_rowwise_skip(rollback_report)

        verify_results(session, order_id)

        logger.info("stopping mysqld...")
