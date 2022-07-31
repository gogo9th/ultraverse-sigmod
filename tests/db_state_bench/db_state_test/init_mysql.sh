#!/bin/bash

source ../base_script/mysql_env.sh

init_opt -m

kill_mysqld

delete_log

copy_config

run_mysqld

exit 0

