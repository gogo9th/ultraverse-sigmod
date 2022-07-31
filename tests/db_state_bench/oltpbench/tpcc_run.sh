#!/bin/bash

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

init_opt $@

if [ $DB_MODE == "O" ]; then
  EXPORT_DATA=$EXPORT_PATH/tpcc_export_base_origin
  MY_EXPORT_DATA=$EXPORT_PATH/tpcc_export_my_origin
  DUMP_DATA=$EXPORT_PATH/tpcc_dump_base_origin
  MY_DUMP_DATA=$EXPORT_PATH/tpcc_dump_my_origin
else
  EXPORT_DATA=$EXPORT_PATH/tpcc_export_base_modified
  MY_EXPORT_DATA=$EXPORT_PATH/tpcc_export_my_modified
  DUMP_DATA=$EXPORT_PATH/tpcc_dump_base_modified
  MY_DUMP_DATA=$EXPORT_PATH/tpcc_dump_my_modified
fi

function do_original_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "UNDO TIME : $1"
  print_log "REDO TIME : $2"

  flashback_doit "$1" "$2" "tpcc"
}

function do_modified_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "run db_state..."
  echo $DB_STATE_CHANGE_PATH  --gid 1 -u$MYSQL_ID -p$MYSQL_PW #--graph $BASEDIR/result.svg
  $DB_STATE_CHANGE_PATH --gid 1 -u$MYSQL_ID -p$MYSQL_PW #--graph $BASEDIR/result.svg
  print_log "run db_state...done"
}

kill_mysqld

delete_log

copy_config

run_mysqld

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


print_log "prepare data..."
cd $OLTPBENCH_PATH
./oltpbenchmark -b tpcc -c $BASEDIR/tpcc_config.xml --create=true --load=true -v
cd $BASEDIR
print_log "prepare data...done"

sleep 1

TEST_CASE="HASH"

if [ $TEST_CASE == "HASH" ]; then
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  UNDO_TIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`

  query="SET @@SESSION.state_log_group_flag=1;\
  BEGIN;\
  UPDATE tpcc.WAREHOUSE SET W_YTD = W_YTD + 1500 WHERE W_ID = 1;\
  UPDATE tpcc.WAREHOUSE SET W_YTD = W_YTD - 1500 WHERE W_ID = 1;\
  COMMIT;\
  SET @@SESSION.state_log_group_flag=0;"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  USER_TABLE_NAME="tpcc.HISTORY"
else
  C_ID=`echo "SELECT C_ID FROM tpcc.CUSTOMER WHERE C_W_ID = 1 AND C_D_ID = 6 AND C_LAST = 'PRESCALLYPRES'  ORDER BY C_FIRST LIMIT 1" | mysql -u$MYSQL_ID -p$MYSQL_PW | grep -v C_ID`

  TMP_EPOCH_TIME=`date +"%s.%6N"`
  UNDO_TIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`

  query="SET @@SESSION.state_log_group_flag=1;\
  BEGIN;\
  UPDATE tpcc.DISTRICT SET D_YTD = D_YTD + 1000 WHERE D_W_ID = 1 AND D_ID = 6;\
  UPDATE tpcc.WAREHOUSE SET W_YTD = W_YTD + 1500 WHERE W_ID = 1;\
  UPDATE tpcc.CUSTOMER SET C_BALANCE = -1200, C_YTD_PAYMENT = 1200, C_PAYMENT_CNT = 2 WHERE C_W_ID = 1 AND C_D_ID = 6 AND C_ID = $C_ID;\
  INSERT INTO tpcc.HISTORY (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)  VALUES (6,1,$C_ID,6,1,'2019-12-08 03:58:31',1000,'KIMJH')\
  COMMIT;\
  SET @@SESSION.state_log_group_flag=0;"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  USER_TABLE_NAME="tpcc.HISTORY"
fi

sleep 1

REDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

print_log "update data..."
cd $OLTPBENCH_PATH
for ((i=0;i<1;i++))
do
  $OLTPBENCH_PATH/oltpbenchmark -b tpcc -c $BASEDIR/tpcc_config.xml --execute=true -s 5 -v
  # $OLTPBENCH_PATH/oltpbenchmark -b tpcc -c $BASEDIR/tpcc_config.xml --execute=true -s 5 -v
  # $OLTPBENCH_PATH/oltpbenchmark -b tpcc -c $BASEDIR/tpcc_config.xml --execute=true -s 5 -v
done
cd $BASEDIR
print_log "update data...done"

sleep 3

print_log "export data..."
rm -f $EXPORT_DATA $DUMP_DATA
echo "SELECT * INTO OUTFILE '$EXPORT_DATA' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM $USER_TABLE_NAME" | mysql -u$MYSQL_ID -p$MYSQL_PW
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B tpcc > $DUMP_DATA
print_log "export data...done"

sleep 1

START_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
if [ $DB_MODE == "O" ]; then
  do_original_state_change "$UNDO_TIME" "$REDO_TIME"
else
  do_modified_state_change
fi
END_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

sleep 1

print_log "export data..."
rm -f $MY_EXPORT_DATA $MY_DUMP_DATA
echo "SELECT * INTO OUTFILE '$MY_EXPORT_DATA' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM $USER_TABLE_NAME" | mysql -u$MYSQL_ID -p$MYSQL_PW
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B tpcc > $MY_DUMP_DATA
print_log "export data...done"

# diff $EXPORT_DATA $MY_EXPORT_DATA > $DIFF_RESULT

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0
