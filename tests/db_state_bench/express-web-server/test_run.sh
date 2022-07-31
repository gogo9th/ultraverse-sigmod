#!/bin/bash

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

init_opt $@

if [ $DB_MODE == "O" ]; then
  EXPORT_DATA=$EXPORT_PATH/express_export_base_origin
  MY_EXPORT_DATA=$EXPORT_PATH/express_export_my_origin
  DUMP_DATA=$EXPORT_PATH/express_dump_base_origin
  MY_DUMP_DATA=$EXPORT_PATH/express_dump_my_origin
else
  EXPORT_DATA=$EXPORT_PATH/express_export_base_modified
  MY_EXPORT_DATA=$EXPORT_PATH/express_export_my_modified
  DUMP_DATA=$EXPORT_PATH/express_dump_base_modified
  MY_DUMP_DATA=$EXPORT_PATH/express_dump_my_modified
fi

function do_original_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "UNDO TIME : $1"
  print_log "REDO TIME : $2"

  flashback_doit "$1" "$2" "express-db"
}

function do_modified_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "run db_state..."

  TEST_CASE="SCN1"
  if [ $TEST_CASE == "SCN1" ]; then
    # 시나리오 1
    # 설정한 gid 삭제
    echo $DB_STATE_CHANGE_PATH  --gid $1 -u$MYSQL_ID -p$MYSQL_PW #--graph $BASEDIR/result.svg
    $DB_STATE_CHANGE_PATH --gid $1 -u$MYSQL_ID -p$MYSQL_PW #--graph $BASEDIR/result.svg

  else
    # 시나리오 2
    # 설정한 gid 삭제후 쿼리 추가

    ID=$2

    echo "SELECT * from \`express-db\`.Items WHERE item_id=$ID" | mysql -u$MYSQL_ID -p$MYSQL_PW > tmp_query

    # item_id item_name       description     is_onetime      price   discount        creator_id
    # 3       item_name_3     item_description_3      0       880     1       173489d3

    NAME=`cat tmp_query | awk '{if (NR == 2) {print $2}}'`
    DESC=`cat tmp_query | awk '{if (NR == 2) {print $3}}'`
    IS_ONETIME=`cat tmp_query | awk '{if (NR == 2) {print $4}}'`
    PRICE=`cat tmp_query | awk '{if (NR == 2) {print $5 + 1000}}'`
    DISCOUNT=`cat tmp_query | awk '{if (NR == 2) {print $6}}'`
    C_ID=`cat tmp_query | awk '{if (NR == 2) {print $7}}'`

    NEW_QUERY="INSERT INTO Items
      (item_id, item_name, description, is_onetime,
      price, discount, creator_id) VALUES
      ($ID, '$NAME', '$DESC', $IS_ONETIME,
    '$PRICE', '$DISCOUNT', '$C_ID');"

    echo "USE express-db;" > new_query
    echo $NEW_QUERY >> new_query

    echo $DB_STATE_CHANGE_PATH  --gid $1 --query $BASEDIR/new_query -u$MYSQL_ID -p$MYSQL_PW #--graph $BASEDIR/result.svg
    $DB_STATE_CHANGE_PATH --gid $1 --query $BASEDIR/new_query -u$MYSQL_ID -p$MYSQL_PW #--graph $BASEDIR/result.svg

    \rm -f tmp_query
    \rm -f new_query
  fi

  print_log "run db_state...done"
}

kill_mysqld

delete_log

copy_config

run_mysqld

print_log "cleanup express database..."
echo "DROP DATABASE IF EXISTS \`express-db\`" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS \`express-db\`" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup express database...done"

print_log "cleanup STATE_LOG_CHANGE_DB database..."
echo "DROP DATABASE IF EXISTS STATE_LOG_CHANGE_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS STATE_LOG_CHANGE_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup STATE_LOG_CHANGE_DB database...done"

print_log "cleanup STATE_LOG_BACKUP_DB database..."
echo "DROP DATABASE IF EXISTS STATE_LOG_BACKUP_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS STATE_LOG_BACKUP_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup STATE_LOG_BACKUP_DB database...done"


print_log "run test..."
node ./test/test.js tmp_id
TARGET_GID=`cat tmp_id | awk '{print $1}'`
TARGET_ITEM_ID=`cat tmp_id | awk '{print $2}'`
UNDO_TIME=`cat tmp_id | awk '{print $3}' | xargs -i date -d '@{}' +"%Y-%m-%d %H:%M:%S.%6N"`
REDO_TIME=`cat tmp_id | awk '{print $4}' | xargs -i date -d '@{}' +"%Y-%m-%d %H:%M:%S.%6N"`

\rm -f tmp_id
print_log "run test...done"

sleep 3


USER_TABLE_NAME="\`express-db\`.Items"

print_log "export data..."
rm -f $EXPORT_DATA $DUMP_DATA
echo "SELECT * INTO OUTFILE '$EXPORT_DATA' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM $USER_TABLE_NAME" | mysql -u$MYSQL_ID -p$MYSQL_PW
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B express-db > $DUMP_DATA
print_log "export data...done"

sleep 1

START_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
if [ $DB_MODE == "O" ]; then
  do_original_state_change "$UNDO_TIME" "$REDO_TIME"
else
  do_modified_state_change "$TARGET_GID" "$TARGET_ITEM_ID"
fi
END_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

sleep 1

print_log "export data..."
rm -f $MY_EXPORT_DATA $MY_DUMP_DATA
echo "SELECT * INTO OUTFILE '$MY_EXPORT_DATA' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM $USER_TABLE_NAME" | mysql -u$MYSQL_ID -p$MYSQL_PW
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B express-db > $MY_DUMP_DATA
print_log "export data...done"

diff $EXPORT_DATA $MY_EXPORT_DATA > $DIFF_RESULT

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0
