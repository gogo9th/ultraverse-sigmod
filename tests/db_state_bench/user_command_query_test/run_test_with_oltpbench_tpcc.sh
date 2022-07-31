#!/bin/bash

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

init_opt '-m'

if [ "$1" == "trx" ]; then
  echo "test with transaction"
elif [ "$1" == "proc" ]; then
  echo "test with procedure"
else
  echo "$0 [trx or proc]"
  echo "    trx     with transaction"
  echo "    proc    with procedure"
  exit 1
fi

DUMP_DATA=$EXPORT_PATH/tpcc_dump_base_modified
MY_DUMP_DATA=$EXPORT_PATH/tpcc_dump_my_modified

QUERY2_FILE='user_query2.sql'
echo 'USE tpcc;' > $QUERY2_FILE

function do_modified_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "run db_state..."
  echo $DB_STATE_CHANGE_PATH  --query2 $QUERY2_FILE -u$MYSQL_ID -p$MYSQL_PW
  $DB_STATE_CHANGE_PATH  --query2 $QUERY2_FILE -u$MYSQL_ID -p$MYSQL_PW
  print_log "run db_state...done"
}

function select_c_id {
  C_ID=`echo "SELECT C_ID FROM tpcc.CUSTOMER WHERE C_W_ID = 1 AND C_D_ID = 6 AND C_LAST = 'PRESCALLYPRES'  ORDER BY C_FIRST LIMIT 1" | mysql -u$MYSQL_ID -p$MYSQL_PW | grep -v C_ID`
  echo $C_ID
}

function create_procedure {
  mysql -u$MYSQL_ID -p$MYSQL_PW tpcc << EOM
  DROP PROCEDURE IF EXISTS TEST_PROC; 

  DELIMITER $$
  CREATE PROCEDURE TEST_PROC (IN _CID INT)
  BEGIN
      UPDATE tpcc.DISTRICT SET D_YTD = D_YTD + 1000 WHERE D_W_ID = 1 AND D_ID = 6;
      UPDATE tpcc.WAREHOUSE SET W_YTD = W_YTD + 1500 WHERE W_ID = 1;
      UPDATE tpcc.CUSTOMER SET C_BALANCE = -1200, C_YTD_PAYMENT = 1200, C_PAYMENT_CNT = 2 WHERE C_W_ID = 1 AND C_D_ID = 6 AND C_ID = _CID;
      INSERT INTO tpcc.HISTORY (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)  VALUES (6,1,_CID,6,1,'2019-12-08 03:58:31',1000,'KIMJH');
  END$$
EOM
}

function run_proc_query {
  C_ID=$(select_c_id)
  query="CALL TEST_PROC($C_ID);"

  if [ $1 == "DB" ]; then
    echo $query
  elif [ $1 == "ADD" ]; then
    TMP_DATETIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
    echo "ADD, $TMP_DATETIME, $query" >> $QUERY2_FILE
  else
    TMP_EPOCH_TIME=`date +"%s.%6N"`
    TMP_DATETIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`
    echo "DEL, $TMP_DATETIME;" >> $QUERY2_FILE
    echo "SET TIMESTAMP=$TMP_EPOCH_TIME; $query"
  fi
}

function run_trx_query {
  C_ID=$(select_c_id)
  query="BEGIN;\
  UPDATE tpcc.DISTRICT SET D_YTD = D_YTD + 1000 WHERE D_W_ID = 1 AND D_ID = 6;\
  UPDATE tpcc.WAREHOUSE SET W_YTD = W_YTD + 1500 WHERE W_ID = 1;\
  UPDATE tpcc.CUSTOMER SET C_BALANCE = -1200, C_YTD_PAYMENT = 1200, C_PAYMENT_CNT = 2 WHERE C_W_ID = 1 AND C_D_ID = 6 AND C_ID = $C_ID;\
  INSERT INTO tpcc.HISTORY (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)  VALUES (6,1,$C_ID,6,1,'2019-12-08 03:58:31',1000,'KIMJH');\
  COMMIT;"

  if [ $1 == "DB" ]; then
    echo $query
  elif [ $1 == "ADD" ]; then
    TMP_DATETIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
    echo "ADD, $TMP_DATETIME, $query" >> $QUERY2_FILE
  else
    TMP_EPOCH_TIME=`date +"%s.%6N"`
    TMP_DATETIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`
    echo "DEL, $TMP_DATETIME;" >> $QUERY2_FILE
    echo "SET TIMESTAMP=$TMP_EPOCH_TIME; $query"
  fi
}

function run_test {
  if [ $1 == "trx" ]; then
    # trx 추가 (commit this regular transaction for no reason)
    run_trx_query "DB" | mysql -u$MYSQL_ID -p$MYSQL_PW

    # 쿼리 파일에 trx 추가 (will retroactively add this transaction)
    run_trx_query "ADD"
    run_trx_query "ADD"

    # trx 추가 및 쿼리 파일에 삭제 (will retroactively delete this transaction)
    run_trx_query "DEL" | mysql -u$MYSQL_ID -p$MYSQL_PW

    # trx 추가 (commit this regular transaction for no reason)
    run_trx_query "DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
  else
    # procedure 추가 
    run_proc_query "DB" | mysql -u$MYSQL_ID -p$MYSQL_PW tpcc

    # 쿼리 파일에 procedure 추가
    run_proc_query "ADD"
    run_proc_query "ADD"

    # procedure 추가 및 쿼리 파일에 삭제
    run_proc_query "DEL" | mysql -u$MYSQL_ID -p$MYSQL_PW tpcc

    # procedure 추가
    run_proc_query "DB" | mysql -u$MYSQL_ID -p$MYSQL_PW tpcc
  fi
}

kill_mysqld

delete_log

copy_config

run_mysqld
sleep 5

print_log "cleanup tpcc database..."
echo "DROP DATABASE IF EXISTS tpcc" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS tpcc" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup tpcc database...done"

print_log "cleanup STATE_LOG_CHANGE_DB database..."
echo "DROP DATABASE IF EXISTS STATE_LOG_CHANGE_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS STATE_LOG_CHANGE_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup STATE_LOG_CHANGE_DB database...done"

print_log "cleanup STATE_LOG_BACKUP_DB database..."
echo "DROP DATABASE IF EXISTS STATE_LOG_BACKUP_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS STATE_LOG_BACKUP_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup STATE_LOG_BACKUP_DB database...done"

print_log "cleanup STATE_GROUP_DB database..."
echo "DROP DATABASE IF EXISTS STATE_GROUP_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS STATE_GROUP_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup STATE_GROUP_DB database...done"

echo "CREATE TABLE IF NOT EXISTS STATE_GROUP_DB.STATE_GROUP_TABLE \
     (group_id INT NOT NULL AUTO_INCREMENT,\
      time TIMESTAMP(6) NULL DEFAULT NULL,\
      INDEX group_id (group_id, time))" | mysql -u$MYSQL_ID -p$MYSQL_PW


print_log "prepare data..."
cd $OLTPBENCH_PATH
./oltpbenchmark -b tpcc -c $BASEDIR/tpcc_config.xml --create=true --load=true -v
cd $BASEDIR
# cat tpcc_config.xml | docker run -i --rm --network host oltpbench -b tpcc --create=true --load=true -v
print_log "prepare data...done"


if [ $1 == "proc" ]; then
  create_procedure
fi


# 단일쿼리 단위의 상태전환 추가/삭제만이 아닌 프로시저 단위의 상태전환 추가/삭제의 정상 작동함을 테스트
# 프로시저 추가/삭제의 타겟 시점이 쿼리 로그상에서 최초부분, 중간부분, 마지막부분일 때의 모두 정상 작동함을 테스트
# CASE1 : 최초 부분 (retroactively change the beginning part of the commit history)
# CASE2 : 중간 부분 (retroactively change the middle part of the commit history)
# CASE3 : 마지막 부분 (retroactively change the end part of the commit history)
TEST_CASE="CASE1"

if [ $TEST_CASE == "CASE1" ]; then
  # 최초 부분 (beginning part of the commit history)
  sleep 1
  run_test $1
fi

sleep 1

print_log "update data..."
cd $OLTPBENCH_PATH
$OLTPBENCH_PATH/oltpbenchmark -b tpcc -c $BASEDIR/tpcc_config.xml --execute=true -s 5 -v
cd $BASEDIR
# cat tpcc_config.xml | docker run -i --rm --network host oltpbench -b tpcc --execute=true -s 5 -v

if [ $TEST_CASE == "CASE2" ]; then
  # 중간 부분  (middle part of the commit history)
  sleep 1
  run_test $1
fi

cd $OLTPBENCH_PATH
$OLTPBENCH_PATH/oltpbenchmark -b tpcc -c $BASEDIR/tpcc_config.xml --execute=true -s 5 -v
cd $BASEDIR
# cat tpcc_config.xml | docker run -i --rm --network host oltpbench -b tpcc --execute=true -s 5 -v

if [ $TEST_CASE == "CASE3" ]; then
  # 마지막 부분 (end part of the commit history)
  sleep 1
  run_test $1
fi
print_log "update data...done"

sleep 3

print_log "export data..."
rm -f $DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B tpcc --skip-extended-insert > $DUMP_DATA
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$DUMP_DATA.CUSTOMER' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM tpcc.CUSTOMER" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$DUMP_DATA.DISTRICT' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM tpcc.DISTRICT" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$DUMP_DATA.HISTORY' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM tpcc.HISTORY" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$DUMP_DATA.WAREHOUSE' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM tpcc.WAREHOUSE" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "export data...done"

sleep 1

START_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
do_modified_state_change
END_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

sleep 1

print_log "export data..."
rm -f $MY_DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B tpcc --skip-extended-insert > $MY_DUMP_DATA
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$MY_DUMP_DATA.CUSTOMER' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM tpcc.CUSTOMER" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$MY_DUMP_DATA.DISTRICT' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM tpcc.DISTRICT" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$MY_DUMP_DATA.HISTORY' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM tpcc.HISTORY" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$MY_DUMP_DATA.WAREHOUSE' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM tpcc.WAREHOUSE" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "export data...done"

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0
