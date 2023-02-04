# ULTRAVERSE

# SYNOPSIS

```shell
$ sudo apt install build-essential cmake mariadb-server mariadb-client libmariadb-dev libmariadb-dev-compat pkg-config bison flex libboost-all-dev libfmt-dev libspdlog-dev libgvc6 graphviz-dev doxygen

# ...

$ vim /etc/mysql/my.cnf.d/server.cnf

[mariadb]
log-bin=myserver-binlog
binlog_format=ROW
binlog_row_image=FULL
binlog_row_metadata=FULL
binlog-checksum=NONE
max_binlog_size=100M

$ systemctl restart mariadb

# in case of MySQL 

[mysqld]
log-bin=server-binlog
binlog_format=ROW
binlog_row_image=FULL
binlog_row_metadata=FULL
binlog-checksum=NONE
binlog_rows_query_log_events=ON
max_binlog_size=300M

$ systemctl restart mysqld

```

```shell
$ git clone https://github.com/team-unstablers/ultraverse
$ cd ultraverse
$ git submodule init
$ git submodule update
$ mkdir build && cd build 
$ cmake ..
$ make -j8 
```

```shell
$ echo 'CREATE DATABASE benchbase' | sudo mysql
$ benchbase/run-mariadb epinions mariadb 1m prepare
$ sudo mysqldump benchbase > checkpoint-230106.sql
$ systemctl stop mariadbd
$ sudo sh -c "rm -rf /var/lib/mysql/myserver-binlog*"
$ systemctl start mariadbd
$ benchbase/run-mariadb epinions mariadb 1m execute
$ sudo sh -c "cp -rv /var/lib/mysql/myserver-binlog* ."
$ sudo chown user:group myserver-binlog* 

# Reads MySQL-variant binary logs and writes state log into 'benchbase.ultstatelog'
# (see ./statelogd -h for more information)
$ ./statelogd -b myserver-binlog.index -o benchbase

# use -M flag if you're running MySQL
$ ./statelogd -M -b myserver-binlog.index -o benchbase

# Make cluster map & table map before performing change state. 
$ ./db_state_change -i benchbase -d benchbase -k "item2.i_id,useracct.u_id" make_cluster

# Performs change state. (see ./db_state_change -h for more information)
$ ./db_state_change \
    -i benchbase \
    -b checkpoint-230106.sql \
    -d benchbase \
    -k "item2.i_id,useracct.u_id" \
    rollback=2:prepend=16,migration-16.sql:rollback=32
```


# REQUIREMENTS

- build-essential
- cmake (3.0+; tested on 3.23.2)
- mariadb (tested on 15.1)
  - requires FULL ROW IMAGE/METADATA in binary log (see synopsis section)
- libmariadb-dev


# NOTE

- 다량의 데이터 처리 시 기본 malloc 대신 jemalloc를 사용하면 메모리 할당 관련된 시간을 단축할 수 있습니다.

```shell
sudo apt install libjemalloc-dev
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./db_state_change ... 

# 기본 malloc 사용 시
226.48s user 27.56s system 192% cpu 2:12.18 total

# jemalloc 사용 시
123.35s user 30.00s system 127% cpu 1:59.86 total
```

- 문제 파악을 위해 의도적으로 intermediate database를 드랍하지 않도록 했습니다.
- 상기와 같은 이유로, 상태 전환 이후에 REPLACE 쿼리문을 표시하지만 실제로 실행하지는 않습니다.

- 문제 발생시 이 레포지토리에 이슈로 남겨주시면 빠르게 처리해 드리겠습니다.
