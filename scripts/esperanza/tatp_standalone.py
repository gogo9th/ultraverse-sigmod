import os
import time

from esperanza.tatp.session import TATPStandaloneSession
from esperanza.utils.download_mysql import download_mysql, get_mysql_bin_path
from esperanza.utils.state_change_report import read_state_change_report

KEY_COLUMNS = [
    "subscriber.s_id",
    "access_info.s_id",
    "call_forwarding.s_id",
    "special_facility.s_id",
]
BACKUP_FILE = "dbdump.sql"
COLUMN_ALIASES: dict[str, list[str]] = {
    "subscriber.sub_nbr": ["subscriber.s_id"],
}

DB_TABLE_DIFF_OPTIONS = {
    "subscriber": [
        "s_id", "sub_nbr", "bit_1", "bit_2", "bit_3", "bit_4", "bit_5", "bit_6", "bit_7", "bit_8",
        "bit_9", "bit_10", "hex_1", "hex_2", "hex_3", "hex_4", "hex_5", "hex_6", "hex_7", "hex_8",
        "hex_9", "hex_10", "byte2_1", "byte2_2", "byte2_3", "byte2_4", "byte2_5", "byte2_6",
        "byte2_7", "byte2_8", "byte2_9", "byte2_10", "msc_location", "vlr_location",
    ],
    "access_info": ["s_id", "ai_type", "data1", "data2", "data3", "data4"],
    "special_facility": ["s_id", "sf_type", "is_active", "error_cntrl", "data_a", "data_b"],
    "call_forwarding": ["s_id", "sf_type", "start_time", "end_time", "numberx"],
}


def decide_rollback_gids(session: TATPStandaloneSession, ratio: float) -> list[int]:
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
    session: TATPStandaloneSession,
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


def perform_full_replay(session: TATPStandaloneSession) -> None:
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

    session = TATPStandaloneSession(
        "tatp_standalone",
        "1m",
        num_subscribers=100000,
        query_count=100000,
    )
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

        logger.info("creating cluster...")
        session.run_db_state_change("make_cluster")

        perform_state_change(session, [0], do_extra_replay_st=True, do_table_diff=True)

        # rollback_gids = decide_rollback_gids(session, 0.01)
        # perform_state_change(session, rollback_gids, do_extra_replay_st=True, do_table_diff=True)

        # rollback_gids = decide_rollback_gids(session, 0.1)
        # perform_state_change(session, rollback_gids, do_extra_replay_st=True, do_table_diff=True)

        # perform_full_replay(session)

        logger.info("stopping mysqld...")
