import os

from esperanza.benchbase.benchmark_session import BenchmarkSession
from esperanza.utils.download_mysql import download_mysql

if not download_mysql():
    print("MySQL distribution is not available")
    exit(1)

session = BenchmarkSession("epinions", "1m")
session.prepare()

session.run_statelogd()

session.mysqld.start()
session.tablediff("benchbase_dist1.epinions", "benchbase_dist2.epinions",
                  ["col1", "col2", "col3"])
session.mysqld.stop()

# TODO: statelogd에 run_once 플래그를 추가해서, 한번만 실행되도록 하기
# TODO: db_state_change --gen-report 옵션 추가하기
