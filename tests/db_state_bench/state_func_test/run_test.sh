#!/bin/bash

DB="test_data"

BASEDIR=`pwd`
source $BASEDIR/../base_script/mysql_env.sh

init_opt '-m'

DUMP_DATA=$EXPORT_PATH/${DB}_dump_base_modified
MY_DUMP_DATA=$EXPORT_PATH/${DB}_dump_my_modified

function do_original_state_change {
  echo 3 > /proc/sys/vm/drop_caches ; sync

  print_log "UNDO TIME : $1"
  print_log "REDO TIME : $2"

  flashback_doit "$1" "$2" "$DB"
}

kill_mysqld

delete_log

copy_config

run_mysqld
sleep 5

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

mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
DROP TABLE IF EXISTS function_table;
CREATE TABLE function_table (
  date_string TEXT NULL DEFAULT NULL,
  date_date DATE NULL DEFAULT NULL,
  time_string TEXT NULL DEFAULT NULL,
  time_time TIME NULL DEFAULT NULL,
  datetime_string TEXT NULL DEFAULT NULL,
  datetime_datetime1 DATETIME NULL DEFAULT NULL,
  datetime_datetime2 DATETIME NULL DEFAULT NULL,
  datetime_datetime3 DATETIME NULL DEFAULT NULL,
  datetime_datetime4 DATETIME NULL DEFAULT NULL,
  rand DOUBLE NULL DEFAULT NULL
);'

mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
DROP PROCEDURE IF EXISTS while_test;
DELIMITER $$
CREATE PROCEDURE while_test(IN var INT)
BEGIN
    DECLARE nCnt INT DEFAULT 0;
    while_xxxx:WHILE (nCnt < var) DO
        SET nCnt = nCnt + 1;
        IF ((nCnt % 2) = 1) THEN
            ITERATE while_xxxx;
        END IF;
        INSERT INTO function_table VALUES (CURDATE(), CURRENT_DATE(), CURTIME(), CURRENT_TIME(), NOW(), CURRENT_TIMESTAMP(), LOCALTIME(), LOCALTIMESTAMP(), SYSDATE(), RAND());
    END WHILE;
END $$'

print_log "prepare data...done"

mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL while_test(10);
INSERT INTO function_table VALUES (CURDATE(), CURRENT_DATE(), CURTIME(), CURRENT_TIME(), NOW(), CURRENT_TIMESTAMP(), LOCALTIME(), LOCALTIMESTAMP(), SYSDATE(), RAND());'

sleep 1
UNDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL while_test(10);
INSERT INTO function_table VALUES (CURDATE(), CURRENT_DATE(), CURTIME(), CURRENT_TIME(), NOW(), CURRENT_TIMESTAMP(), LOCALTIME(), LOCALTIMESTAMP(), SYSDATE(), RAND());'

sleep 1
REDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL while_test(10);
INSERT INTO function_table VALUES (CURDATE(), CURRENT_DATE(), CURTIME(), CURRENT_TIME(), NOW(), CURRENT_TIMESTAMP(), LOCALTIME(), LOCALTIMESTAMP(), SYSDATE(), RAND());'

mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL while_test(10);
INSERT INTO function_table VALUES (CURDATE(), CURRENT_DATE(), CURTIME(), CURRENT_TIME(), NOW(), CURRENT_TIMESTAMP(), LOCALTIME(), LOCALTIMESTAMP(), SYSDATE(), RAND());'

print_log "export data..."
rm -f $DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B $DB --skip-extended-insert > $DUMP_DATA
print_log "export data...done"

sleep 3

START_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
do_original_state_change "$UNDO_TIME" "$REDO_TIME"
END_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

sleep 3

print_log "export data..."
rm -f $MY_DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B $DB --skip-extended-insert > $MY_DUMP_DATA
print_log "export data...done"

echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0

