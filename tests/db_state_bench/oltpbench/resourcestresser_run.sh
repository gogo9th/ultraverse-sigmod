#!/bin/bash

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

init_opt $@

if [ $DB_MODE == "O" ]; then
  EXPORT_DATA=$EXPORT_PATH/resourcestresser_export_base_origin
  MY_EXPORT_DATA=$EXPORT_PATH/resourcestresser_export_my_origin
  DUMP_DATA=$EXPORT_PATH/resourcestresser_dump_base_origin
  MY_DUMP_DATA=$EXPORT_PATH/resourcestresser_dump_my_origin
else
  EXPORT_DATA=$EXPORT_PATH/resourcestresser_export_base_modified
  MY_EXPORT_DATA=$EXPORT_PATH/resourcestresser_export_my_modified
  DUMP_DATA=$EXPORT_PATH/resourcestresser_dump_base_modified
  MY_DUMP_DATA=$EXPORT_PATH/resourcestresser_dump_my_modified
fi

function do_original_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "UNDO TIME : $1"
  print_log "REDO TIME : $2"

  flashback_doit "$1" "$2" "resourcestresser"
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

print_log "cleanup resourcestresser database..."
echo "DROP DATABASE IF EXISTS resourcestresser" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS resourcestresser" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup resourcestresser database...done"

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
./oltpbenchmark -b resourcestresser -c $BASEDIR/resourcestresser_config.xml --create=true --load=true -v
cd $BASEDIR
print_log "prepare data...done"

sleep 1

TEST_CASE="HASH"

if [ $TEST_CASE == "HASH" ]; then
  # GET VAR
  EMP_ID=`echo "SELECT empid FROM resourcestresser.locktable LIMIT 10,1" | mysql -u$MYSQL_ID -p$MYSQL_PW | grep -v empid`
  ORIGIN_DATA=`echo "SELECT salary FROM resourcestresser.locktable WHERE empid=$EMP_ID" | mysql -u$MYSQL_ID -p$MYSQL_PW | grep -v salary`
  MODIFY_DATA="999999"

  TMP_EPOCH_TIME=`date +"%s.%6N"`
  UNDO_TIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`

  query="SET @@SESSION.state_log_group_flag=1;\
  BEGIN;\
  UPDATE resourcestresser.locktable SET salary = '$MODIFY_DATA' WHERE empid=$EMP_ID;\
  UPDATE resourcestresser.locktable SET salary = '$ORIGIN_DATA' WHERE empid=$EMP_ID;\
  COMMIT;\
  SET @@SESSION.state_log_group_flag=0;"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  USER_TABLE_NAME="resourcestresser.locktable"
else
  # Contention1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  UNDO_TIME=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`

  query="SET @@SESSION.state_log_group_flag=1;\
  BEGIN;\
  UPDATE resourcestresser.locktable SET salary = 9999 WHERE empid IN (SELECT empid FROM resourcestresser.cputable);\
  COMMIT;\
  SET @@SESSION.state_log_group_flag=0;"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  USER_TABLE_NAME="resourcestresser.locktable"
fi

sleep 1

REDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

print_log "update data..."
cd $OLTPBENCH_PATH
for ((i=0;i<1;i++))
do
  $OLTPBENCH_PATH/oltpbenchmark -b resourcestresser -c $BASEDIR/resourcestresser_config.xml --execute=true -s 5 -v
  #$OLTPBENCH_PATH/oltpbenchmark -b resourcestresser -c $BASEDIR/resourcestresser_config.xml --execute=true -s 5 -v
  #$OLTPBENCH_PATH/oltpbenchmark -b resourcestresser -c $BASEDIR/resourcestresser_config.xml --execute=true -s 5 -v
done
cd $BASEDIR
print_log "update data...done"

sleep 3

print_log "export data..."
rm -f $EXPORT_DATA $DUMP_DATA
echo "SELECT * INTO OUTFILE '$EXPORT_DATA' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM $USER_TABLE_NAME" | mysql -u$MYSQL_ID -p$MYSQL_PW
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B resourcestresser > $DUMP_DATA
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
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B resourcestresser > $MY_DUMP_DATA
print_log "export data...done"

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0
