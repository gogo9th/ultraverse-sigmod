#!/bin/bash

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

init_opt $@

if [ $DB_MODE == "O" ]; then
  EXPORT_DATA=$EXPORT_PATH/ycsb_export_base_origin
  MY_EXPORT_DATA=$EXPORT_PATH/ycsb_export_my_origin
  DUMP_DATA=$EXPORT_PATH/ycsb_dump_base_origin
  MY_DUMP_DATA=$EXPORT_PATH/ycsb_dump_my_origin
else
  EXPORT_DATA=$EXPORT_PATH/ycsb_export_base_modified
  MY_EXPORT_DATA=$EXPORT_PATH/ycsb_export_my_modified
  DUMP_DATA=$EXPORT_PATH/ycsb_dump_base_modified
  MY_DUMP_DATA=$EXPORT_PATH/ycsb_dump_my_modified
fi

function do_original_state_change {
  print_log "UNDO TIME : $1"
  print_log "REDO TIME : $2"

  flashback_doit "$1" "$2" "ycsb"
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

print_log "cleanup ycsb database..."
echo "DROP DATABASE IF EXISTS ycsb" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS ycsb" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup ycsb database...done"

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
./oltpbenchmark -b ycsb -c $BASEDIR/ycsb_config.xml --create=true --load=true -v
cd $BASEDIR
print_log "prepare data...done"

sleep 1

TMP_EPOCH_TIME=`date +"%s.%6N"`
UNDO_TIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`
query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
INSERT INTO ycsb.USERTABLE (YCSB_KEY, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9, FIELD10) VALUES ('9999999', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0')"
echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "INSERT INTO STATE_GROUP_DB.STATE_GROUP_TABLE (group_id, time) VALUES (1, '$UNDO_TIME')" | mysql -u$MYSQL_ID -p$MYSQL_PW
USER_TABLE_NAME="ycsb.USERTABLE"

sleep 1

REDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

print_log "update data..."
cd $OLTPBENCH_PATH
for ((i=0;i<1;i++))
do
  $OLTPBENCH_PATH/oltpbenchmark -b ycsb -c $BASEDIR/ycsb_config.xml --execute=true -s 5 -v
done
cd $BASEDIR
print_log "update data...done"

sleep 3

print_log "export data..."
rm -f $EXPORT_DATA $DUMP_DATA
echo "SELECT * INTO OUTFILE '$EXPORT_DATA' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM $USER_TABLE_NAME" | mysql -u$MYSQL_ID -p$MYSQL_PW
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B ycsb > $DUMP_DATA
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
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B ycsb > $MY_DUMP_DATA
print_log "export data...done"

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0
