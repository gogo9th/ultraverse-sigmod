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
  cond INT NULL DEFAULT NULL,
  ind INT NULL DEFAULT NULL,
  rand DOUBLE NULL DEFAULT NULL
);'

mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
DROP PROCEDURE IF EXISTS if_test;
DELIMITER $$
CREATE PROCEDURE if_test(IN var INT)
BEGIN
	DECLARE nCnt INT DEFAULT 1;
	WHILE (nCnt <= var) DO
		SET nCnt = nCnt + 1;
		IF ((SELECT COUNT(*) FROM function_table) < 2) THEN
			INSERT INTO function_table VALUES (1, var, RAND());
		ELSE
			INSERT INTO function_table VALUES (0, var, RAND());
		END IF;
	END WHILE;
END $$'

print_log "prepare data...done"


# insert (..., 1, 1, rand_a0), enter the if block
mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL if_test(1);
'

sleep 1
UNDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

# insert (..., 1, 2, rand_b0), enter the if block
# insert (..., 0, 2, rand_b1), enter the else block
mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL if_test(2);
'

sleep 1
REDO_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

# insert (..., 0, 3, rand_c0), enter the else block
# insert (..., 0, 3, rand_c1), enter the else block
# insert (..., 0, 3, rand_c2), enter the else block
mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL if_test(3);
'

# insert (..., 0, 4, rand_d0), enter the else block
# insert (..., 0, 4, rand_d1), enter the else block
# insert (..., 0, 4, rand_d2), enter the else block
# insert (..., 0, 4, rand_d3), enter the else block
mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL if_test(4);
'
# insert (..., 0, 5, rand_e0), enter the else block
# insert (..., 0, 5, rand_e1), enter the else block
# insert (..., 0, 5, rand_e2), enter the else block
# insert (..., 0, 5, rand_e3), enter the else block
# insert (..., 0, 5, rand_e4), enter the else block
mysql -u$MYSQL_ID -p$MYSQL_PW --database $DB -e '
CALL if_test(5);
'


print_log "export data..."
rm -f $DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B $DB --skip-extended-insert > out1 #$DUMP_DATA
print_log "export data...done"

# <out1's expected state>
# (1, 1, rand_a0) => IF true block에 의해 생성
# (1, 2, rand_b0) =>	IF true block에 의해 생성
# (0, 2, rand_b1) =>	ELSE false block에 의해 생성
# (0, 3, rand_c0) =>	ELSE false block에 의해 생성
# (0, 3, rand_c1) =>	ELSE false block에 의해 생성
# (0, 3, rand_c2) =>	ELSE false block에 의해 생성
# (0, 4, rand_d0) =>	ELSE false block에 의해 생성
# (0, 4, rand_d1) =>	ELSE false block에 의해 생성
# (0, 4, rand_d2) =>	ELSE false block에 의해 생성
# (0, 4, rand_d3) =>	ELSE false block에 의해 생성
# (0, 5, rand_e0) =>	ELSE false block에 의해 생성
# (0, 5, rand_e1) =>	ELSE false block에 의해 생성
# (0, 5, rand_e2) =>	ELSE false block에 의해 생성
# (0, 5, rand_e3) =>	ELSE false block에 의해 생성
# (0, 5, rand_e4) =>	ELSE false block에 의해 생성

sleep 3

START_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`
do_original_state_change "$UNDO_TIME" "$REDO_TIME"
END_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

sleep 3

print_log "export data..."
rm -f $MY_DUMP_DATA
mysqldump -u$MYSQL_ID -p$MYSQL_PW -B $DB --skip-extended-insert > out2 #$MY_DUMP_DATA
print_log "export data...done"

# <out2's expected state>
# (1, 1, rand_a) =>	IF true block에 의해 생성, 이곳까지 UNDO하지 않았으므로 rand_a는 기존과 일치해야 함
# (1, 3, new_rand_c0) => IF true block에 의해 생성, REDO시 기존과 다른 SQL 코드 위치의 WHILE 루프 1번째 iteration에서 RAND() 함수를 호출되므로 새로운 rand_c0값을 리턴해야 함
# (0, 3, new_rand_c1) => ELSE false block에 의해 생성, REDO시 기존과 다른 SQL 코드 위치의 WHILE 루프 2번째 iteration에서 RAND() 함수를 호출되므로 새로운 rand_c1값을 리턴해야 함
# (0, 3, new_rand_c2) => ELSE false block에 의해 생성, REDO시 기존과 다른 SQL 코드 위치의 WHILE 루프 3번째 iteration에서 RAND() 함수를 호출되므로 새로운 rand_c2값을 리턴해야 함
# (0, 4, rand_d0) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 1번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_d0값을 리턴해야 함
# (0, 4, rand_d1) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 2번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_d0값을 리턴해야 함
# (0, 4, rand_d2) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 3번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_d0값을 리턴해야 함
# (0, 4, rand_d3) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 4번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_d0값을 리턴해야 함
# (0, 5, rand_e0) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 1번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_e0값을 리턴해야 함
# (0, 5, rand_e1) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 2번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_e1값을 리턴해야 함
# (0, 5, rand_e2) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 3번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_e2값을 리턴해야 함
# (0, 5, rand_e3) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 4번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_e3값을 리턴해야 함
# (0, 5, rand_e4) =>	ELSE false block에 의해 생성, REDO시 기존과 같은 SQL 코드 위치의 WHILE 루프 5번째 iteration에서 RAND() 함수를 호출하므로 기존과 같은 rand_e4값을 리턴해야 함


echo "SUMMARY"
echo "STATE CHANGE START TIME : $START_TIME"
echo "STATE CHANGE END TIME   : $END_TIME"

kill_mysqld

exit 0

