#!/bin/bash

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

init_opt $@

if [ $DB_MODE == "O" ]; then
  EXPORT_DATA=$EXPORT_PATH/tatp_export_base_origin
  MY_EXPORT_DATA=$EXPORT_PATH/tatp_export_my_origin
  DUMP_DATA=$EXPORT_PATH/tatp_dump_base_origin
  MY_DUMP_DATA=$EXPORT_PATH/tatp_dump_my_origin
else
  EXPORT_DATA=$EXPORT_PATH/tatp_export_base_modified
  MY_EXPORT_DATA=$EXPORT_PATH/tatp_export_my_modified
  DUMP_DATA=$EXPORT_PATH/tatp_dump_base_modified
  MY_DUMP_DATA=$EXPORT_PATH/tatp_dump_my_modified
fi

KEY_NAME=""

function do_original_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "UNDO TIME : $1"
  print_log "REDO TIME : $2"

  flashback_doit "$1" "$2" "tatp"
}

function do_modified_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "run db_state..."
  echo $DB_STATE_CHANGE_PATH  --gid 1 -u$MYSQL_ID -p$MYSQL_PW --key-name $KEY_NAME --key-type int #--graph $BASEDIR/result.svg
  $DB_STATE_CHANGE_PATH --gid 1 -u$MYSQL_ID -p$MYSQL_PW --key-name $KEY_NAME --key-type int #--graph $BASEDIR/result.svg
  print_log "run db_state...done"
}

kill_mysqld

delete_log

copy_config

run_mysqld

print_log "cleanup tatp database..."
echo "DROP DATABASE IF EXISTS tatp" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS tatp" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup tatp database...done"

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
./oltpbenchmark -b tatp -c $BASEDIR/tatp_config.xml --create=true --load=true -v
cd $BASEDIR
print_log "prepare data...done"

sleep 1

TEST_CASE="HASH"

if [ $TEST_CASE == "HASH" ]; then
  KEY_NAME="tatp.CALL_FORWARDING.s_id"

  # GET ID
  S_ID=`echo "SELECT s_id FROM tatp.SUBSCRIBER LIMIT 1" | mysql -u$MYSQL_ID -p$MYSQL_PW | grep -v s_id`
  SF_TYPE=`echo "SELECT sf_type FROM tatp.SPECIAL_FACILITY WHERE s_id=$S_ID LIMIT 1" | mysql -u$MYSQL_ID -p$MYSQL_PW | grep -v sf_type`
  MY_START_TIME='99'

  TMP_EPOCH_TIME=`date +"%s.%6N"`
  UNDO_TIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`

  # INSERT - DELETE
  query="SET @@SESSION.state_log_group_flag=1;\
  BEGIN;\
  INSERT INTO tatp.CALL_FORWARDING (s_id, sf_type, start_time) VALUES ($S_ID, $SF_TYPE, $MY_START_TIME);\
  DELETE FROM tatp.CALL_FORWARDING WHERE s_id=$S_ID AND sf_type=$SF_TYPE AND start_time=$MY_START_TIME;\
  COMMIT;\
  SET @@SESSION.state_log_group_flag=0;"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  USER_TABLE_NAME="tatp.CALL_FORWARDING"
else
  KEY_NAME="tatp.SUBSCRIBER.s_id"

  # GET ID
  S_ID="10"

  # UPDATE QUERY
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  UNDO_TIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`

  query="SET @@SESSION.state_log_group_flag=1;\
  BEGIN;\
  UPDATE tatp.SUBSCRIBER SET vlr_location = 999999 WHERE s_id = $S_ID LIMIT 1;\
  COMMIT;\
  SET @@SESSION.state_log_group_flag=0;"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  USER_TABLE_NAME="tatp.SUBSCRIBER"
fi

sleep 1

REDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

print_log "update data..."
cd $OLTPBENCH_PATH
for ((i=0;i<1;i++))
do
  $OLTPBENCH_PATH/oltpbenchmark -b tatp -c $BASEDIR/tatp_config.xml --execute=true -s 5 -v
#  $OLTPBENCH_PATH/oltpbenchmark -b tatp -c $BASEDIR/tatp_config.xml --execute=true -s 5 -v
#  $OLTPBENCH_PATH/oltpbenchmark -b tatp -c $BASEDIR/tatp_config.xml --execute=true -s 5 -v
done

cd $BASEDIR
print_log "update data...done"

sleep 3

print_log "export data..."
rm -f $EXPORT_DATA $DUMP_DATA
echo "SELECT * INTO OUTFILE '$EXPORT_DATA' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM $USER_TABLE_NAME" | mysql -u$MYSQL_ID -p$MYSQL_PW
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B tatp > $DUMP_DATA
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
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B tatp > $MY_DUMP_DATA
print_log "export data...done"

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0
