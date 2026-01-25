import os
import time

from esperanza.tpcc.session import TPCCStandaloneSession
from esperanza.utils.download_mysql import download_mysql, get_mysql_bin_path
from esperanza.utils.state_change_report import read_state_change_report

KEY_COLUMNS = [
    'warehouse.w_id',
    'customer.c_w_id',
    'stocks.s_w_id',
    'order_line.ol_w_id',
    'district.d_w_id',
    'order.o_w_id',
    'history.h_c_w_id',
    'item.i_id'
]
BACKUP_FILE = "dbdump.sql"
COLUMN_ALIASES: dict[str, list[str]] = {}

DB_TABLE_DIFF_OPTIONS = {
    "warehouse": ["w_id", "w_ytd", "w_tax", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip"],
    "item": ["i_id", "i_name", "i_price", "i_data", "i_im_id"],
    "stock": ["s_w_id", "s_i_id", "s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10"],
    "district": ["d_w_id", "d_id", "d_ytd", "d_tax", "d_next_o_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip"],
    "customer": ["c_w_id", "c_d_id", "c_id", "c_discount", "c_credit", "c_last", "c_first", "c_credit_lim", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_middle", "c_data"],
    "history": ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"],
    "oorder": ["o_w_id", "o_d_id", "o_id", "o_c_id", "o_carrier_id", "o_ol_cnt", "o_all_local", "o_entry_d"],
    "new_order": ["no_w_id", "no_d_id", "no_o_id"],
    "order_line": ["ol_w_id", "ol_d_id", "ol_o_id", "ol_number", "ol_i_id", "ol_delivery_d", "ol_amount", "ol_supply_w_id", "ol_quantity", "ol_dist_info"],
}


def decide_rollback_gids(session: TPCCStandaloneSession, ratio: float) -> list[int]:
    ratio = max(min(ratio, 1.0), 0.0)

    if ratio == 0.0:
        return [0]

    rollback_log_name = f"rollback_auto_decision_{ratio}"
    decision_stdout_name = rollback_log_name + ".stdout"
    decision_stderr_name = rollback_log_name + ".stderr"
    decision_report_name = session.report_name_from_stdout(decision_stdout_name)

    session.run_db_state_change(
        f"auto-rollback={ratio}",
        stdout_name=decision_stdout_name,
        stderr_name=decision_stderr_name,
    )

    decision_report = read_state_change_report(f"{session.session_path}/{decision_report_name}")
    return decision_report["rollbackGids"]


def perform_state_change(
    session: TPCCStandaloneSession,
    rollback_gids: list[int],
    do_extra_replay_st: bool = False,
    do_table_diff: bool = False,
) -> None:
    logger = session.logger

    rollback_log_name = f"rollback_{rollback_gids[0]}_{rollback_gids[-1]}"
    rollback_stdout_name = rollback_log_name + ".stdout"
    rollback_stderr_name = rollback_log_name + ".stderr"
    rollback_report_name = session.report_name_from_stdout(rollback_stdout_name)

    replay_log_name = rollback_log_name + ".replay"
    replay_stdout_name = replay_log_name + ".stdout"
    replay_stderr_name = replay_log_name + ".stderr"
    replay_report_name = session.report_name_from_stdout(replay_stdout_name)

    rollback_action = f"rollback={','.join(map(str, rollback_gids))}"
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

    if do_extra_replay_st:
        replay_st_log_name = rollback_log_name + ".replay-st"
        replay_st_stdout_name = replay_st_log_name + ".stdout"
        replay_st_stderr_name = replay_st_log_name + ".stderr"
        replay_st_report_name = session.report_name_from_stdout(replay_st_stdout_name)
        session.run_db_state_change(
            f"{rollback_action}:full-replay",
            stdout_name=replay_st_stdout_name,
            stderr_name=replay_st_stderr_name,
        )

        replay_st_report = read_state_change_report(f"{session.session_path}/{replay_st_report_name}")

        session.mysqld.mysqldump(
            replay_st_report["intermediateDBName"],
            f"{session.session_path}/dbdump_st_latest.sql",
        )
    else:
        replay_st_report = None

    logger.info(f"State Change Report: {rollback_log_name}")
    if do_extra_replay_st and replay_st_report is not None:
        logger.info(
            f"prepare: {rollback_report['executionTime']}, "
            f"replay: {replay_report['executionTime']}, "
            f"replay-st: {replay_st_report['executionTime']}"
        )
    else:
        logger.info(
            f"prepare: {rollback_report['executionTime']}, "
            f"replay: {replay_report['executionTime']}"
        )

    if do_extra_replay_st and do_table_diff and replay_st_report is not None:
        for table, cols in DB_TABLE_DIFF_OPTIONS.items():
            session.tablediff(
                f"benchbase.{table}",
                f"{replay_st_report['intermediateDBName']}.{table}",
                cols,
            )


def perform_full_replay(session: TPCCStandaloneSession) -> None:
    logger = session.logger

    full_replay_log_name = "full_replay"
    full_replay_stdout_name = full_replay_log_name + ".stdout"
    full_replay_stderr_name = full_replay_log_name + ".stderr"
    full_replay_report_name = session.report_name_from_stdout(full_replay_stdout_name)

    session.run_db_state_change(
        "full-replay",
        stdout_name=full_replay_stdout_name,
        stderr_name=full_replay_stderr_name,
    )

    replay_report = read_state_change_report(f"{session.session_path}/{full_replay_report_name}")

    logger.info(f"State Change Report: {full_replay_log_name}")
    logger.info(f"full-replay: {replay_report['executionTime']}")


if __name__ == "__main__":
    if not download_mysql():
        print("MySQL distribution is not available")
        raise SystemExit(1)

    os.environ["MYSQL_BIN_PATH"] = get_mysql_bin_path()

    os.putenv("DB_HOST", "127.0.0.1")
    os.putenv("DB_PORT", "3306")
    os.putenv("DB_USER", "admin")
    os.putenv("DB_PASS", "password")

    session = TPCCStandaloneSession("tpcc_standalone", "1m", scale_factor=1, query_count=92000)
    logger = session.logger

    session.prepare()

    logger.info("Starting mysqld for workload...")
    with session.mysqld.start():
        time.sleep(5)

        logger.info("Executing workload...")
        stats = session.execute_workload()
        logger.info(f"Workload stats: {stats}")

        logger.info("dumping latest database...")
        session.mysqld.mysqldump("benchbase", f"{session.session_path}/dbdump_latest.sql")

        logger.info("stopping mysqld...")

    session.finalize()

    session.run_statelogd(key_columns=KEY_COLUMNS)
    session.update_config(backup_file=BACKUP_FILE, column_aliases=COLUMN_ALIASES)

    logger.info("Starting mysqld for state change...")
    with session.mysqld.start():
        time.sleep(10)

        session.run_db_state_change("make_cluster")
        perform_state_change(session, [0], do_extra_replay_st=True, do_table_diff=True)

        logger.info("stopping mysqld...")
