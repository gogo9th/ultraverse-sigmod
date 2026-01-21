import os
import time

from esperanza.benchbase.benchmark_session import BenchmarkSession
from esperanza.utils.download_mysql import download_mysql
from esperanza.utils.state_change_report import read_state_change_report

DB_STATE_CHANGE_BASE_OPTIONS = [
    '-b', 'dbdump.sql',
    '-i', 'benchbase',
    '-d', 'benchbase',
    '-k', 'item2.i_id,useracct.u_id,review.u_id+review.i_id,trust.source_u_id+trust.target_u_id'
]

DB_TABLE_DIFF_OPTIONS = {
    # 테이블 비교를 할 테이블들을 지정한다.
    # 'table': ['col1', 'col2', ...] 같이 기입한다.

    'useracct': ['u_id', 'name', 'email', 'creation_date'],
    'item2': ['i_id', 'title', 'description', 'creation_date'],
    'review': ['a_id', 'u_id', 'i_id', 'rating', 'rank', 'comment', 'creation_date'],
    'trust': ['source_u_id', 'target_u_id', 'trust', 'creation_date'],
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
    decision_report_name = rollback_log_name + ".report.json"

    session.run_db_state_change(
        DB_STATE_CHANGE_BASE_OPTIONS + [
            '-r', decision_report_name,
            f"auto-rollback={ratio}"
        ],
        stdout_name=decision_stdout_name,
        stderr_name=decision_stderr_name
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
    rollback_report_name = rollback_log_name + ".report.json"

    replay_log_name = rollback_log_name + ".replay"
    replay_stdout_name = replay_log_name + ".stdout"
    replay_stderr_name = replay_log_name + ".stderr"
    replay_report_name = replay_log_name + ".report.json"

    # # PREPARE 실행한다.
    rollback_action = f"rollback={','.join(map(str, rollback_gids))}"
    session.run_db_state_change(
        DB_STATE_CHANGE_BASE_OPTIONS + [
            '-r', rollback_report_name,
            rollback_action
        ],
        stdout_name=rollback_stdout_name,
        stderr_name=rollback_stderr_name
    )

    rollback_report = read_state_change_report(f"{session.session_path}/{rollback_report_name}")

    # REPLAY 실행한다.
    session.run_db_state_change(
        DB_STATE_CHANGE_BASE_OPTIONS + [
            '-N',
            '-r', replay_report_name,
            'replay'
        ],
        stdout_name=replay_stdout_name,
        stderr_name=replay_stderr_name
    )

    replay_report = read_state_change_report(f"{session.session_path}/{replay_report_name}")

    if do_extra_replay_st:
        replay_st_log_name = rollback_log_name + ".replay-st"
        replay_st_stdout_name = replay_st_log_name + ".stdout"
        replay_st_stderr_name = replay_st_log_name + ".stderr"
        replay_st_report_name = replay_st_log_name + ".report.json"
        # 위와 같은 조건으로 REPLAY를 한번 더 실행한다. (single-thread)
        session.run_db_state_change(
            DB_STATE_CHANGE_BASE_OPTIONS + [
                '-N',
                '-r', replay_st_report_name,
                f"{rollback_action}:full-replay"
            ],
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
        tmp_db_name = f"{replay_report['intermediateDBName']}_tmp"

        session.load_dump(tmp_db_name, f"{session.session_path}/dbdump_st_latest.sql")

        replace_query = rollback_report['replaceQuery'] \
            .replace('__INTERMEDIATE_DB__', replay_report['intermediateDBName']) \
            .replace('benchbase', tmp_db_name)

        session.eval(replace_query)

        for (table, cols) in DB_TABLE_DIFF_OPTIONS.items():
            session.tablediff(
                f"{tmp_db_name}.{table}",
                f"{replay_st_report['intermediateDBName']}.{table}",
                cols
            )

def perform_full_replay(session: BenchmarkSession):
    logger = session.logger

    full_replay_log_name = f"full_replay"

    full_replay_stdout_name = full_replay_log_name + ".stdout"
    full_replay_stderr_name = full_replay_log_name + ".stderr"
    full_replay_report_name = full_replay_log_name + ".report.json"
    # full replay 실행한다. (single-thread)
    session.run_db_state_change(
        DB_STATE_CHANGE_BASE_OPTIONS + [
            '-N',
            '-r', full_replay_report_name,
            "full-replay"
        ],
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

    os.putenv("DB_HOST", "127.0.0.1")
    os.putenv("DB_PORT", "3306")
    os.putenv("DB_USER", "admin")
    os.putenv("DB_PASS", "password")

    session = BenchmarkSession("epinions", "1m")
    logger = session.logger
    session.prepare()

    # statelogd를 실행해서 binary log에서 statelog를 생성한다.
    session.run_statelogd(['-k', 'item2.i_id,useracct.u_id,review.u_id+review.i_id,trust.source_u_id+trust.target_u_id'])

    # db_state_change를 실행하기 위해 mysqld를 실행한다.
    logger.info("starting mysqld...")
    session.mysqld.start()

    # mysqld 기동이 끝날 때까지 기다린다.
    time.sleep(10)

    # 클러스터 생성한다
    logger.info("creating cluster...")
    session.run_db_state_change(DB_STATE_CHANGE_BASE_OPTIONS + ['make_cluster'])

    # state change를 행한다
    # 1. 최소 (상태전환 쿼리를 1개만 선택)
    perform_state_change(session, [0], do_extra_replay_st=True, do_table_diff=False)

    # 2. 1% 정도
    rollback_gids = decide_rollback_gids(session, 0.01)
    perform_state_change(session, rollback_gids, do_extra_replay_st=True, do_table_diff=False)

    # 3. 10% 정도
    rollback_gids = decide_rollback_gids(session, 0.1)
    perform_state_change(session, rollback_gids, do_extra_replay_st=True, do_table_diff=False)

    # 4. 100%
    perform_full_replay(session)


    logger.info("stopping mysqld...")
    session.mysqld.stop()
