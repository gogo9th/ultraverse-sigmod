#!/bin/bash

# set -x

DB="test_data"
POST_COUNT=100
USER_COUNT=10
KEY_NAME=""

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

python3 -m venv venv
source venv/bin/activate
python3 -m pip install -U setuptools wheel pip
python3 -m pip install -r requirements.txt
IS_SPACY_OK=`python3 -m spacy validate | grep en_core_web_sm | grep ✔`
if [ ${#IS_SPACY_OK} -lt 1 ]; then
  python3 -m spacy download en_core_web_sm
fi

init_opt $@

if [ $DB_MODE == "O" ]; then
  DUMP_DATA=$EXPORT_PATH/${DB}_dump_base_origin
  MY_DUMP_DATA=$EXPORT_PATH/${DB}_dump_my_origin
else
  DUMP_DATA=$EXPORT_PATH/${DB}_dump_base_modified
  MY_DUMP_DATA=$EXPORT_PATH/${DB}_dump_my_modified
fi

function do_original_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "UNDO TIME : $1"
  print_log "REDO TIME : $2"

  flashback_doit "$1" "$2" "$DB"
}

function do_modified_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "run db_state..."
  echo $DB_STATE_CHANGE_PATH -u$MYSQL_ID -p$MYSQL_PW --gid 1 --key-name "$KEY_NAME" --key-type "int" #--graph $BASEDIR/result.svg
  $DB_STATE_CHANGE_PATH -u$MYSQL_ID -p$MYSQL_PW --gid 1 --key-name "$KEY_NAME" --key-type "int" #--graph $BASEDIR/result.svg
  print_log "run db_state...done"
}

function do_find_candidate {
  print_log "find candidate..."
  echo $DB_STATE_CHANGE_PATH -u$MYSQL_ID -p$MYSQL_PW --start-datetime "$1" --candidate-db "$DB"
  $DB_STATE_CHANGE_PATH -u$MYSQL_ID -p$MYSQL_PW --start-datetime "$1" --candidate-db "$DB" > candidate_output
  print_log "find candidate...done"

  KEY_NAME=`cat candidate_output | grep 'STRIP CANDIDATE COLUMN' | awk -F ': ' '{print $2}'`
  rm -f candidate_output
}

kill_mysqld

delete_log

copy_config

run_mysqld

print_log "cleanup $DB database..."
echo "DROP DATABASE IF EXISTS $DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS $DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup $DB database...done"

print_log "cleanup STATE_LOG_CHANGE_DB database..."
echo "DROP DATABASE IF EXISTS STATE_LOG_CHANGE_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS STATE_LOG_CHANGE_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup STATE_LOG_CHANGE_DB database...done"

print_log "cleanup STATE_LOG_BACKUP_DB database..."
echo "DROP DATABASE IF EXISTS STATE_LOG_BACKUP_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS STATE_LOG_BACKUP_DB" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup STATE_LOG_BACKUP_DB database...done"

print_log "prepare data..."
python3 MakeTestData.py -t 1 -u $MYSQL_ID -p $MYSQL_PW -d $DB --posts $POST_COUNT --users $USER_COUNT
print_log "prepare data...done"

sleep 1

# 변경 쿼리 실행
# post 의 category 가 변경될 가능성이 큰 쿼리 발생
UNDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
sub_query=`echo "USE $DB; \
  SELECT post_id, user_id, ClusteringModel.word, score * frequency AS total_score \
    FROM Preprocessed JOIN ClusteringModel \
      WHERE Preprocessed.word = ClusteringModel.word \
      ORDER BY total_score DESC LIMIT 1; " | mysql -u$MYSQL_ID -p$MYSQL_PW | grep -v post_id | \
awk -v DB="$DB" '{printf "UPDATE %s.Preprocessed SET frequency=%s WHERE post_id=%s AND user_id=%s AND word=\x27%s\x27", DB, 1, $1, $2, $3}'`
query="SET @@SESSION.state_log_group_flag=1; \
BEGIN; \
$sub_query; \
COMMIT; \
SET @@SESSION.state_log_group_flag=0;"
echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

sleep 1

REDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

sleep 1

print_log "update data..."
python3 MakeTestData.py -t 2 -u $MYSQL_ID -p $MYSQL_PW -d $DB
print_log "update data...done"

sleep 1

print_log "export data..."
rm -f $DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B $DB --skip-extended-insert > $DUMP_DATA
print_log "export data...done"

sleep 5

START_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
if [ $DB_MODE == "O" ]; then
  do_original_state_change "$UNDO_TIME" "$REDO_TIME"
else
  do_find_candidate "$UNDO_TIME"
  do_modified_state_change
fi
END_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

sleep 5

print_log "export data..."
rm -f $MY_DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B $DB --skip-extended-insert > $MY_DUMP_DATA
print_log "export data...done"

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0

