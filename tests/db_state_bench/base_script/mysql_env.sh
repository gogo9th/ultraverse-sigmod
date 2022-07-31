#!/bin/bash

SCRIPTPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
HOSTNAME=`hostname -s`

MYSQL_ID=root
MYSQL_PW=123456

MYSQL_CONF_PATH=/etc/my.cnf.d
MYSQL_DATA_PATH=/var/lib/mysql
MYSQL_LOG_PATH=/var/lib/mysql/log
MYSQL_HASH_PATH=/var/lib/mysql/log/state_hash
MYSQL_FUNCTION_PATH=/var/lib/mysql/log/state_function
MYSQL_GENERAL_LOG_PATH=$MYSQL_DATA_PATH/$HOSTNAME.log
EXPORT_PATH=$MYSQL_LOG_PATH

# MYSQL_SOURCE_PATH=/home/user/dev/mariadb-server/build
MYSQL_SOURCE_PATH=/root/mariadb-server

MODIFY_MYSQLD_PATH=$MYSQL_SOURCE_PATH/build/sql/mysqld
ORIGIN_MYSQLD_PATH=/usr/sbin/mysqld

MODIFY_MYSQLBINLOG_PATH=$MYSQL_SOURCE_PATH/build/client/mysqlbinlog
ORIGIN_MYSQLBINLOG_PATH=/usr/bin/mysqlbinlog

DB_STATE_CHANGE_PATH=$MYSQL_SOURCE_PATH/build/client/db_state_change

# OLTPBENCH_PATH=/home/user/dev/oltpbench
OLTPBENCH_PATH=/root/oltpbench

function set_test_original {
  # MYSSQLD_PATH=$ORIGIN_MYSQLD_PATH
  # MYSQLBINLOG_PATH=$ORIGIN_MYSQLBINLOG_PATH
  MYSSQLD_PATH=$MODIFY_MYSQLD_PATH
  MYSQLBINLOG_PATH=$MODIFY_MYSQLBINLOG_PATH
}

function set_test_modified {
  MYSSQLD_PATH=$MODIFY_MYSQLD_PATH
  MYSQLBINLOG_PATH=$MODIFY_MYSQLBINLOG_PATH
}

function print_log {
  echo "[`date +"%Y-%m-%d %H:%M:%S.%6N"`] $@"
}

function print_help {
  echo "$0 [-o or -m]"
  echo "    -o    original mariadb test"
  echo "    -m    modified mariadb test"
}

function init_opt {
  while getopts "om" opt
  do
    case $opt in
      o)
        set_test_original
        DB_MODE="O"
        ;;
      m)
        set_test_modified
        DB_MODE="M"
        ;;
    esac
  done

  if [[ -z "$DB_MODE" ]]; then
    print_help
    exit 1
  fi
}

function flashback_doit {
  CURR_TIME=`date +"%Y-%m-%d %H:%M:%S.%6N"`

  print_log "run flashback-redo..."
  echo $DB_STATE_CHANGE_PATH  -u$MYSQL_ID -p$MYSQL_PW --start-datetime="$1" --redo-datetime="$2" --redo-db="$3"
  $DB_STATE_CHANGE_PATH  -u$MYSQL_ID -p$MYSQL_PW --start-datetime="$1" --redo-datetime="$2" --redo-db="$3"
  print_log "run flashback-redo...done"
}

function run_mysqld {
  print_log "start mysql server..."
  nohup $MYSSQLD_PATH --user=mysql &
  print_log "start mysql server...done"

  print_log "wait..."
  sleep 5
  print_log "wait...done"
}

function find_mysqld {
  echo `ps -ef|grep mysqld |grep -v grep|awk '{if ($1 == "mysql") {print $2}}'`
}

function kill_mysqld {
  print_log "kill mysql server..."

  pid=$(find_mysqld)
  len=`echo $pid | awk '{print length}'`

  if [ $len -ge 1 ]; then
    kill $pid

    # WAIT
    counter=0
    ret=-1
    while (( $counter < 10 ))
    do
      let counter=$counter+1

      pid=$(find_mysqld)
      len=`echo $pid | awk '{print length}'`

      if [ $len -ge 1 ]; then
        sleep 1
        echo "wait..."
      else
        echo "stopped..."
        ret=0
        break
      fi
    done

    if (( $ret < 0 )); then
      echo "timeout..."
      exit 1
    fi
  fi

  print_log "kill mysql server...done"
}

function delete_log {
  print_log "delete log..."

  rm -rf $MYSQL_HASH_PATH/*
  rmdir $MYSQL_HASH_PATH

  rm -rf $MYSQL_FUNCTION_PATH/*
  rmdir $MYSQL_FUNCTION_PATH

  rm -rf $MYSQL_LOG_PATH/*
  rm -f $MYSQL_GENERAL_LOG_PATH

  print_log "delete log...done"
}

function copy_config {
  print_log "copy mysql config..."

  rm -f $MYSQL_CONF_PATH/*

  if [ $DB_MODE == "O" ]; then
    cp $SCRIPTPATH/db_state_base.cnf $MYSQL_CONF_PATH/
    cp $SCRIPTPATH/db_state_server.cnf $MYSQL_CONF_PATH/
  else
    cp $SCRIPTPATH/db_state_base.cnf $MYSQL_CONF_PATH/
    cp $SCRIPTPATH/db_state_server.cnf $MYSQL_CONF_PATH/
  fi

  print_log "copy mysql config...done"
}

