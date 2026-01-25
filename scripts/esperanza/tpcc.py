import os
import time

from esperanza.benchbase.benchmark_session import BenchmarkSession
from esperanza.utils.download_mysql import download_mysql, get_mysql_bin_path
from esperanza.utils.state_change_report import read_state_change_report

KEY_COLUMNS = [
    'warehouse.w_id',
    'item.i_id',
    'district.d_w_id+district.d_id',
    'customer.c_w_id+customer.c_d_id+customer.c_id',
    'stock.s_w_id+stock.s_i_id',
    'oorder.o_w_id+oorder.o_d_id+oorder.o_id',
    'oorder.o_w_id+oorder.o_d_id+oorder.o_c_id',
    'new_order.no_w_id+new_order.no_d_id+new_order.no_o_id',
    'order_line.ol_w_id+order_line.ol_d_id+order_line.ol_o_id+order_line.ol_number',
    'order_line.ol_w_id+order_line.ol_d_id+order_line.ol_o_id',
    'order_line.ol_supply_w_id+order_line.ol_i_id',
    'history.h_c_w_id+history.h_c_d_id+history.h_c_id',
    'history.h_w_id+history.h_d_id',
]
BACKUP_FILE = "dbdump.sql"
COLUMN_ALIASES: dict[str, list[str]] = {}

DB_TABLE_DIFF_OPTIONS = {
    # 테이블 비교를 할 테이블들을 지정한다.
    # 'table': ['col1', 'col2', ...] 같이 기입한다.
    'warehouse': ['w_id', 'w_ytd', 'w_tax', 'w_name', 'w_street_1', 'w_street_2', 'w_city', 'w_state', 'w_zip'],
    'item': ['i_id', 'i_name', 'i_price', 'i_data', 'i_im_id'],
    'stock': ['s_w_id', 's_i_id', 's_quantity', 's_ytd', 's_order_cnt', 's_remote_cnt', 's_data', 's_dist_01', 's_dist_02', 's_dist_03', 's_dist_04', 's_dist_05', 's_dist_06', 's_dist_07', 's_dist_08', 's_dist_09', 's_dist_10'],
    'district': ['d_w_id', 'd_id', 'd_ytd', 'd_tax', 'd_next_o_id', 'd_name', 'd_street_1', 'd_street_2', 'd_city', 'd_state', 'd_zip'],
    'customer': ['c_w_id', 'c_d_id', 'c_id', 'c_discount', 'c_credit', 'c_last', 'c_first', 'c_credit_lim', 'c_balance', 'c_ytd_payment', 'c_payment_cnt', 'c_delivery_cnt', 'c_street_1', 'c_street_2', 'c_city', 'c_state', 'c_zip', 'c_phone', 'c_since', 'c_middle', 'c_data'],
    'history': ['h_c_id', 'h_c_d_id', 'h_c_w_id', 'h_d_id', 'h_w_id', 'h_date', 'h_amount', 'h_data'],
    'oorder': ['o_w_id', 'o_d_id', 'o_id', 'o_c_id', 'o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d'],
    'new_order': ['no_w_id', 'no_d_id', 'no_o_id'],
    'order_line': ['ol_w_id', 'ol_d_id', 'ol_o_id', 'ol_number', 'ol_i_id', 'ol_delivery_d', 'ol_amount', 'ol_supply_w_id', 'ol_quantity', 'ol_dist_info']
}

def decide_rollback_gids(session: BenchmarkSession, ratio: float) -> list[int]:
    """
    롤백할 gid들을 결정한다
    :param session: session 오브젝트
    :return: 롤백할 gid들
    """

    ratio = max(min(ratio, 1.0), 0.0)

    if ratio == 0.0:
        return [0]

    logger = session.logger

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

    return decision_report['rollbackGids']

def perform_state_change(session: BenchmarkSession, rollback_gids: list[int], do_extra_replay_st: bool=False, do_table_diff: bool=False):
    """
    db_state_change를 실행한다.
    :param session: session 오브젝트
    :param rollback_gids: 롤백할 gid들
    :param do_extra_replay_st: single-thread로 full replay를 할지 여부
    :param do_table_diff: single-thread로 실행한 replay와 테이블 비교를 할지 여부
    """

    logger = session.logger

    rollback_log_name = f"rollback_{rollback_gids[0]}_{rollback_gids[-1]}"
    rollback_stdout_name = rollback_log_name + ".stdout"
    rollback_stderr_name = rollback_log_name + ".stderr"
    rollback_report_name = session.report_name_from_stdout(rollback_stdout_name)

    replay_log_name = rollback_log_name + ".replay"
    replay_stdout_name = replay_log_name + ".stdout"
    replay_stderr_name = replay_log_name + ".stderr"
    replay_report_name = session.report_name_from_stdout(replay_stdout_name)

    # # PREPARE 실행한다.
    rollback_action = f"rollback={','.join(map(str, rollback_gids))}"
    session.run_db_state_change(
        rollback_action,
        replay_from=0,
        stdout_name=rollback_stdout_name,
        stderr_name=rollback_stderr_name,
    )

    rollback_report = read_state_change_report(f"{session.session_path}/{rollback_report_name}")

    # REPLAY 실행한다.
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
        # 위와 같은 조건으로 REPLAY를 한번 더 실행한다. (single-thread)
        session.run_db_state_change(
            f"{rollback_action}:full-replay",
            stdout_name=replay_st_stdout_name,
            stderr_name=replay_st_stderr_name,
        )

        replay_st_report = read_state_change_report(f"{session.session_path}/{replay_st_report_name}")

        session.mysqld.mysqldump(replay_st_report['intermediateDBName'], f"{session.session_path}/dbdump_st_latest.sql")
    else:
        replay_st_report = None

    logger.info(f"State Change Report: {rollback_log_name}")
    if do_extra_replay_st:
        logger.info(f"prepare: {rollback_report['executionTime']}, replay: {replay_report['executionTime']}, replay-st: {replay_st_report['executionTime']}")
    else:
        logger.info(f"prepare: {rollback_report['executionTime']}, replay: {replay_report['executionTime']}")

    if do_extra_replay_st and do_table_diff:
        # 테이블 비교를 한다.
        for (table, cols) in DB_TABLE_DIFF_OPTIONS.items():
            session.tablediff(
                f"benchbase.{table}",
                f"{replay_st_report['intermediateDBName']}.{table}",
                cols
            )


def perform_full_replay(session: BenchmarkSession):
    logger = session.logger

    full_replay_log_name = f"full_replay"
    full_replay_stdout_name = full_replay_log_name + ".stdout"
    full_replay_stderr_name = full_replay_log_name + ".stderr"
    full_replay_report_name = session.report_name_from_stdout(full_replay_stdout_name)
    # full replay 실행한다. (single-thread)
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
        exit(1)

    os.environ["MYSQL_BIN_PATH"] = get_mysql_bin_path()

    os.putenv("DB_HOST", "127.0.0.1")
    os.putenv("DB_PORT", "3306")
    os.putenv("DB_USER", "admin")
    os.putenv("DB_PASS", "password")

    session = BenchmarkSession("tpcc", "1m")
    logger = session.logger
    session.prepare()

    # statelogd를 실행해서 binary log에서 statelog를 생성한다.
    session.run_statelogd(key_columns=KEY_COLUMNS)
    session.update_config(backup_file=BACKUP_FILE, column_aliases=COLUMN_ALIASES)

    # db_state_change를 실행하기 위해 mysqld를 실행한다.
    logger.info("starting mysqld...")
    with session.mysqld.start():
        # mysqld 기동이 끝날 때까지 기다린다.
        time.sleep(10)

        # 클러스터 생성한다
        logger.info("creating cluster...")
        session.run_db_state_change("make_cluster")

        # state change를 행한다
        # 1. 최소 (상태전환 쿼리를 1개만 선택)
        perform_state_change(session, [0], do_extra_replay_st=True, do_table_diff=True)

        # 2. 1% 정도
        rollback_gids = decide_rollback_gids(session, 0.01)
        perform_state_change(session, rollback_gids, do_extra_replay_st=True, do_table_diff=True)

        # 3. 10% 정도
        rollback_gids = decide_rollback_gids(session, 0.1)
        perform_state_change(session, rollback_gids, do_extra_replay_st=True, do_table_diff=True)

        # 4. 100%
        perform_full_replay(session)

        logger.info("stopping mysqld...")
