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
# Reads MySQL-variant binary logs and writes state log into 'myserver.ultstatelog'
# (see ./statelogd -h for more information)
$ ./statelogd -b /var/lib/mysql/myserver-binlog.index -o myserver

# Make cluster map & table map before performing change state. 
$ ./db_state_change -i cheese-binlog -d benchbase -k "item2.i_id,useracct.u_id" make_cluster

# Performs change state. (see ./db_state_change -h for more information)
$ ./db_state_change \
    -i cheese-binlog \
    -b checkpoint-230106.sql \
    -d benchbase \
    -S 11101,11102,11103,11104,11105,11106,11107,11108,11109,11110,11111,11112 \
    -k "item2.i_id,useracct.u_id" \
    rollback=33049:prepend=33049,migration-33049.sql:rollback=33050
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
