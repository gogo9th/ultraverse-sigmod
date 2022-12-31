# ULTRAVERSE

# SYNOPSIS

```shell
$ sudo apt install build-essential cmake mariadb libmariadb-dev

# ...

$ vim /etc/my.cnf.d/server.cnf

[mariadb]
log-bin=myserver-binlog
binlog_format=ROW
binlog_row_image=FULL
binlog_row_metadata=FULL
binlog-checksum=NONE
max_binlog_size=100M

$ systemctl restart mariadbd

```

```shell
$ git clone $ULTRAVERSE_GIT
$ cd ultraverse
$ mkdir build && cd build 
$ cmake ..
$ make -j8 
```

```shell
# Reads MySQL-variant binary logs and writes state log into 'myserver.ultstatelog'
# (see ./statelogd -h for more information)
$ ./statelogd -b /var/lib/mysql/myserver-binlog.index -o myserver

# Make cluster map & table map before performing change state. 
$ ./db_state_change -i cheese-binlog -d benchbase -k "item2.i_id,useracct.u_id" make_clustermap

# Performs change state. (see ./db_state_change -h for more information)
$ ./db_state_change \
    -i cheese-binlog \
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

- 문제 파악을 위해 의도적으로 intermediate database를 드랍하지 않도록 했습니다.
- 상기와 같은 이유로, 상태 전환 이후에 REPLACE 쿼리문을 표시하지만 실제로 실행하지는 않습니다.

- 문제 발생시 이 레포지토리에 이슈로 남겨주시면 빠르게 처리해 드리겠습니다.