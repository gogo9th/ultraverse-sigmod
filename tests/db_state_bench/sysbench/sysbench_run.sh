#!/bin/bash

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

SYSBENCH_PATH=/root/sysbench

init_opt $@

if [ $DB_MODE == "O" ]; then
  EXPORT_DATA=$EXPORT_PATH/sysbench_export_base_origin
  MY_EXPORT_DATA=$EXPORT_PATH/sysbench_export_my_origin
  DUMP_DATA=$EXPORT_PATH/sysbench_dump_base_origin
  MY_DUMP_DATA=$EXPORT_PATH/sysbench_dump_my_origin
else
  EXPORT_DATA=$EXPORT_PATH/sysbench_export_base_modified
  MY_EXPORT_DATA=$EXPORT_PATH/sysbench_export_my_modified
  DUMP_DATA=$EXPORT_PATH/sysbench_dump_base_modified
  MY_DUMP_DATA=$EXPORT_PATH/sysbench_dump_my_modified
fi

function do_original_state_change {
  print_log "UNDO TIME : $1"
  print_log "REDO TIME : $2"

  flashback_doit "$1" "$2" "sysbench"
}

function do_modified_state_change {
  print_log "run db_state..."
  echo $DB_STATE_CHANGE_PATH  --gid 1 -u$MYSQL_ID -p$MYSQL_PW #--graph $BASEDIR/result.svg
  $DB_STATE_CHANGE_PATH --gid 1 -u$MYSQL_ID -p$MYSQL_PW #--graph $BASEDIR/result.svg
  print_log "run db_state...done"
}

kill_mysqld

delete_log

copy_config

run_mysqld

print_log "cleanup sysbench database..."
echo "DROP DATABASE IF EXISTS sysbench" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS sysbench" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup sysbench database...done"

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
cd $SYSBENCH_PATH
$SYSBENCH_PATH/src/sysbench \
--mysql-host=localhost \
--mysql-port=3310 \
--mysql-socket=/var/lib/mysql/mysql.sock \
--mysql-user=$MYSQL_ID \
--mysql-password=$MYSQL_PW \
--mysql-db=sysbench \
--threads=10 \
--table-size=50000 \
--tables=10 \
$SYSBENCH_PATH/src/lua/oltp_read_write.lua \
prepare
cd $BASEDIR
print_log "prepare data...done"

sleep 1

TMP_EPOCH_TIME=`date +"%s.%6N"`
UNDO_TIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`
query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
UPDATE sysbench.sbtest10 SET pad='3' WHERE id=10"
echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "INSERT INTO STATE_GROUP_DB.STATE_GROUP_TABLE (group_id, time) VALUES (1, '$UNDO_TIME')" | mysql -u$MYSQL_ID -p$MYSQL_PW
USER_TABLE_NAME="sysbench.sbtest10"

sleep 1

REDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

print_log "update data..."
cd $SYSBENCH_PATH
$SYSBENCH_PATH/src/sysbench \
--mysql-host=localhost \
--mysql-port=3310 \
--mysql-socket=/var/lib/mysql/mysql.sock \
--mysql-user=$MYSQL_ID \
--mysql-password=$MYSQL_PW \
--mysql-db=sysbench \
--threads=10 \
--table-size=50000 \
--tables=10 \
$SYSBENCH_PATH/src/lua/oltp_insert.lua \
run
cd $BASEDIR
print_log "update data...done"

sleep 3

print_log "export data..."
rm -f $EXPORT_DATA $DUMP_DATA
echo "SELECT * INTO OUTFILE '$EXPORT_DATA' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM $USER_TABLE_NAME" | mysql -u$MYSQL_ID -p$MYSQL_PW
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B sysbench > $DUMP_DATA
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
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B sysbench > $MY_DUMP_DATA
print_log "export data...done"

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

exit 0
