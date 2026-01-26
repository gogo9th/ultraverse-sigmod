import os
import time

from esperanza.seats.session import SeatsStandaloneSession
from esperanza.utils.download_mysql import download_mysql, get_mysql_bin_path
from esperanza.utils.state_change_report import read_state_change_report

KEY_COLUMNS = [
    "country.co_id",
    "airport.ap_id",
    "airline.al_id",
    "customer2.c_id",
    "flight.f_id",
    "frequent_flyer.ff_c_id+frequent_flyer.ff_al_id",
    "reservation.r_c_id+reservation.r_f_id",
    "airport_distance.d_ap_id0+airport_distance.d_ap_id1",
]
BACKUP_FILE = "dbdump.sql"
COLUMN_ALIASES: dict[str, list[str]] = {
    "customer2.c_id_str": ["customer2.c_id"],
    "frequent_flyer.ff_c_id_str": ["frequent_flyer.ff_c_id"],
}

DB_TABLE_DIFF_OPTIONS = {
    "country": ["co_id", "co_name", "co_code_2", "co_code_3"],
    "airport": [
        "ap_id",
        "ap_code",
        "ap_name",
        "ap_city",
        "ap_postal_code",
        "ap_co_id",
        "ap_longitude",
        "ap_latitude",
        "ap_gmt_offset",
        "ap_wac",
    ],
    "airline": [
        "al_id",
        "al_iata_code",
        "al_icao_code",
        "al_call_sign",
        "al_name",
        "al_co_id",
    ],
    "customer2": ["c_id", "c_id_str", "c_base_ap_id", "c_balance"],
    "flight": [
        "f_id",
        "f_al_id",
        "f_depart_ap_id",
        "f_depart_time",
        "f_arrive_ap_id",
        "f_arrive_time",
        "f_status",
        "f_base_price",
        "f_seats_total",
        "f_seats_left",
    ],
    "frequent_flyer": ["ff_c_id", "ff_al_id", "ff_c_id_str"],
    "reservation": ["r_id", "r_c_id", "r_f_id", "r_seat", "r_price"],
    "airport_distance": ["d_ap_id0", "d_ap_id1", "d_distance"],
}


def decide_rollback_gids(session: SeatsStandaloneSession, ratio: float) -> list[int]:
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
    session: SeatsStandaloneSession,
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


def perform_full_replay(session: SeatsStandaloneSession) -> None:
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

    session = SeatsStandaloneSession(
        "seats_standalone",
        "1m",
        num_airports=100,
        num_customers=2000,
        query_count=10000,
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

        logger.info("stopping mysqld...")
