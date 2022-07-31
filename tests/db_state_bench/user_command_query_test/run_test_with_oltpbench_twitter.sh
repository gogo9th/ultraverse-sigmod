#!/bin/bash

# git clone https://github.com/oltpbenchmark/oltpbench
# docker build -t oltpbench .

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

init_opt '-m'

DUMP_DATA=$EXPORT_PATH/twitter_dump_base_modified
MY_DUMP_DATA=$EXPORT_PATH/twitter_dump_my_modified

QUERY2_FILE='user_query2.sql'
echo 'USE twitter;' > $QUERY2_FILE

function do_modified_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "run db_state..."
  echo $DB_STATE_CHANGE_PATH  --query2 $QUERY2_FILE -u$MYSQL_ID -p$MYSQL_PW
  $DB_STATE_CHANGE_PATH  --query2 $QUERY2_FILE -u$MYSQL_ID -p$MYSQL_PW
  print_log "run db_state...done"
}

kill_mysqld

delete_log

copy_config

run_mysqld
sleep 5

print_log "cleanup twitter database..."
echo "DROP DATABASE IF EXISTS twitter" | mysql -u$MYSQL_ID -p$MYSQL_PW
echo "CREATE DATABASE IF NOT EXISTS twitter" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "cleanup twitter database...done"

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
./oltpbenchmark -b twitter -c $BASEDIR/twitter_config.xml --create=true --load=true -v
cd $BASEDIR
# cat twitter_config.xml | docker run -i --rm --network host oltpbench -b twitter --create=true --load=true -v
print_log "prepare data...done"

TEST_CASE="CASE1"

if [ $TEST_CASE == "CASE1" ]; then
  sleep 1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
  INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'origin message 1', '2019-12-08')"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  # 과거 시점의 쿼리를 삭제
  TMP_STR=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`
  echo "DEL, $TMP_STR;" >> $QUERY2_FILE

  sleep 1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
  INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'origin message 2', '2019-12-08')"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  # 과거 시점에 쿼리 추가
  TMP_STR=`date +"%Y-%m-%d %H:%M:%S.%6N"`
  echo "ADD, $TMP_STR, INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'new message 1', '2019-12-08');" >> $QUERY2_FILE
elif [ $TEST_CASE == "CASE2" ]; then
  sleep 1
  # 과거 시점에 쿼리 추가
  TMP_STR=`date +"%Y-%m-%d %H:%M:%S.%6N"`
  echo "ADD, $TMP_STR, INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'new message 1', '2019-12-08');" >> $QUERY2_FILE

  sleep 1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
  INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'origin message 1', '2019-12-08')"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  # 과거 시점의 쿼리를 삭제
  TMP_STR=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`
  echo "DEL, $TMP_STR;" >> $QUERY2_FILE

  sleep 1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
  INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'origin message 2', '2019-12-08')"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  sleep 1
  # 과거 시점에 쿼리 추가
  TMP_STR=`date +"%Y-%m-%d %H:%M:%S.%6N"`
  echo "ADD, $TMP_STR, INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'new message 2', '2019-12-08');" >> $QUERY2_FILE
elif [ $TEST_CASE == "CASE3" ]; then
  sleep 1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
  INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'origin message 1', '2019-12-08')"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  sleep 1
  # 과거 시점에 쿼리 추가
  TMP_STR=`date +"%Y-%m-%d %H:%M:%S.%6N"`
  echo "ADD, $TMP_STR, INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'new message 1', '2019-12-08');" >> $QUERY2_FILE

  sleep 1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
  INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'origin message 2', '2019-12-08')"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW
else
  sleep 1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
  INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'origin message 1', '2019-12-08')"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW

  # 과거 시점의 쿼리를 삭제
  TMP_STR=`date --date="@$TMP_EPOCH_TIME" +"%Y-%m-%d %H:%M:%S.%6N"`
  echo "DEL, $TMP_STR;" >> $QUERY2_FILE

  sleep 1
  TMP_EPOCH_TIME=`date +"%s.%6N"`
  query="SET TIMESTAMP=$TMP_EPOCH_TIME;\
  INSERT INTO twitter.added_tweets (uid,text,createdate) VALUES (123, 'origin message 2', '2019-12-08')"
  echo $query | mysql -u$MYSQL_ID -p$MYSQL_PW
fi

sleep 1

print_log "update data..."
cd $OLTPBENCH_PATH
for ((i=0;i<1;i++))
do
  $OLTPBENCH_PATH/oltpbenchmark -b twitter -c $BASEDIR/twitter_config.xml --execute=true -s 5 -v
done
cd $BASEDIR
# for ((i=0;i<1;i++))
# do
#   cat twitter_config.xml | docker run -i --rm --network host oltpbench -b twitter --execute=true -s 5 -v
# done
print_log "update data...done"

sleep 3

print_log "export data..."
rm -f $DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B twitter --skip-extended-insert > $DUMP_DATA
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$DUMP_DATA.table' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM twitter.added_tweets" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "export data...done"

sleep 1

START_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
do_modified_state_change
END_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

sleep 1

print_log "export data..."
rm -f $MY_DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B twitter --skip-extended-insert > $MY_DUMP_DATA
echo "SELECT *, ROW_START, ROW_END INTO OUTFILE '$MY_DUMP_DATA.table' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' FROM twitter.added_tweets" | mysql -u$MYSQL_ID -p$MYSQL_PW
print_log "export data...done"

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0
