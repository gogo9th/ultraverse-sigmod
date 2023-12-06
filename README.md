# Ultraverse Tutorial

## Purge Existing MySQL & MariaDB (optional)
```console
$ sudo apt purge mysql-server mysql-common mariadb-server mariadb-common
$ sudo apt autoremove
$ sudo apt autoclean
$ sudo rm -rf /var/lib/mysql /var/lib/mysql.* /var/log/mysql /etc/mysql
```


## Install Required Software
```console
$ sudo apt install build-essential cmake mariadb-client libmariadb-dev libmariadb-dev-compat pkg-config bison flex libboost-all-dev libfmt-dev libspdlog-dev libgvc6 graphviz-dev doxygen libjemalloc-dev libmozjs-102-0 libmozjs-102-dev protoc-gen-go python3-dev libmysqlclient-dev build-essential wget python3 g++-12 clang-15 libc++-15-dev cmake libtbb-dev graphviz libgraphviz-dev libboost-all-dev libmysqlclient-dev libprotobuf-dev protobuf-compiler libfmt-dev libspdlog-dev golang-go
$ pip3 install sqlparse 
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
```

## Option 1: Install and Setup MariaDB
```console
$ sudo apt install mariadb-server mariadb-client
```

 Enable binary logging (check the actual including directory name specified in `/etc/mysql/my.cnf`).

```console
$ sudo vim /etc/mysql/mariadb.conf.d/server.cnf
---------------
   [mariadb]
   log-bin=myserver-binlog
   binlog_format=ROW
   binlog_row_image=FULL
   binlog_row_metadata=FULL
   binlog-checksum=NONE
   max_binlog_size=300M
   plugin_load_add = ha_blackhole
   log_bin_trust_function_creators = 1
---------------
```

Enable efficient large memory allocation

```console
$ sudo vim /etc/systemd/system/multi-user.target.wants/mariadb.service
--------------- 
   Environment="LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"
---------------
```

Activate jemalloc & binary logging

```console
$ sudo systemctl daemon-reload 
$ systemctl restart mariadb 
```

Check that the value is not `system`, but `jemalloc`.
```console
$ sudo mariadb -u root -p
> SHOW VARIABLES LIKE 'version_malloc_library'; 
```

Add the default 'admin' user for Benchbase
```bash
sudo mariadb
> CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';
> GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';
```

## Option 2: Install and Setup MySQL (MUST install only either MySQL or MariaDB)

```console
$ sudo apt install mysql-server mysql-client
```

Enable binary logging (check the actual including directory name specified in `/etc/mysql/my.cnf`).

```console
$ sudo vim /etc/mysql/mysql.conf.d/server.cnf
-------------------
   [mysqld]
   log-bin=myserver-binlog
   binlog_format=ROW
   binlog_row_image=FULL
   binlog_row_metadata=FULL
   binlog-checksum=NONE
   binlog_rows_query_log_events=ON
   max_binlog_size=300M
   plugin_load_add = ha_blackhole
   log_bin_trust_function_creators = 1
-------------------
```

Enable efficient large memory allocation
```console
$ sudo vim /etc/systemd/system/multi-user.target.wants/mysql.service
---------------
   Environment="LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"
---------------
```

Activate jemalloc & binary logging
```console
$ sudo systemctl daemon-reload
$ sudo service mysql restart
```

Check that the value is not 'system', but 'jemalloc ...'
```console
$ sudo mysql -u root -p
> SHOW VARIABLES LIKE 'version_malloc_library'; 
```

Add the default 'admin' user for Benchbase
```bash
sudo mysql
> CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';
> GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';
```


## Install Ultraverse
```console
$ git clone https://github.com/team-unstablers/ultraverse
$ cd ultraverse
$ git submodule init
$ git submodule update
$ mkdir build && cd build
$ sed -i "s/python3$/python3.$(echo "$(python3 --version)" | awk '{print $2}' | awk -F . '{print $2}')/g" ../src/CMakeLists.txt
$ CC=clang-15 CXX=clang++-15 cmake ..
$ make -j8
```

## Example: Retroactive Operation on BenchBase's Epinions


**<u>Step 1.</u>** Create the initial database.

```console
$ echo 'CREATE DATABASE benchbase' | sudo mysql
```

**<u>Step 2.</u>** Create Epinion's table schema, the initial database state, and the checkpoint backup DB.

```console
$ cd <BenchBase directory>
$ ./run-mariadb epinions mariadb 1m prepare
$ sudo mysqldump benchbase > checkpoint-epinions.backup
```

<u>**Step 3.**</u> Reset the binary log and run transactions (SQL procedures).

```console
$ sudo systemctl stop mariadb
$ sudo sh -c "rm -rf /var/lib/mysql/myserver-binlog*"
$ sudo systemctl start mariadb
$ ./run-mariadb epinions mariadb 1m execute
```

<u>**Step 4.**</u> Copy the binary log and Ultraverse's binary log into the new working directory.

```console
$ cd <Ultraverse's Directory>/build
$ rm -rf test && mkdir test && cd test
$ sudo cp /var/lib/mysql/myserver-binlog* .
$ sudo chown mysql:mysql myserver-binlog*
$ sudo chmod 777 myserver-binlog*
$ cp <BenchBase Directory> checkpoint-epinions.backup .
```


<u>**Step 5.**</u> Read Ultraverse's binary log and write (or oppend) the state log into `benchbase.ultstatelog` (see `./statelogd -h` for more information).

```console
$ ../src/statelogd -b myserver-binlog.index -o benchbase
 # Manually terminate the daemon after there is no more new logs to parse
```

The output files are as follows: `benchbase.ultstatelog` and `benchbase.ultchpoint`.

(Alternatively, use -M flag if you're running MySQL)

```console
$ ../src/statelogd -M -b myserver-binlog.index -o benchbase
```

<u>**Step 6.**</u> Make a cluster map & table map before performing a state change. 

```console
$ DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=admin DB_PASS=password \
    ../src/db_state_change -i benchbase -d benchbase -k "item2.i_id,useracct.u_id" make_cluster
```
The output files are as follows: `benchbase.ulttables`, `benchbase.ultindex`, `benchbase.ultcolumns`, and `benchbase.ultcluster`. 

<u>**Step 7.**</u> Perform the change state. (see `./db_state_change -h` for more information)

```console
$ echo "UPDATE useracct SET name = 'HELOWRLD' WHERE u_id = 512;" > prepend1.sql 
$ echo "UPDATE item2 SET title = 'HELOWRLD' WHERE i_id = 224;" > prepend2.sql 
$ DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=admin DB_PASS=password \
    ../src/db_state_change \
       -i benchbase \
       -b checkpoint-epinions.backup \
       -d benchbase \
       -k "item2.i_id,useracct.u_id" \
       rollback=2:prepend=16,prepend1.sql:rollback=32
```


## REQUIREMENTS

- build-essential
- cmake (3.0+; tested on 3.23.2)
- mariadb (tested on 15.1)
  - requires FULL ROW IMAGE/METADATA in binary log (see synopsis section)
- libmariadb-dev


## NOTE

- 다량의 데이터 처리 시 기본 malloc 대신 jemalloc를 사용하면 메모리 할당 관련된 시간을 단축할 수 있습니다.

```shell
$ sudo apt install libjemalloc-dev
$ LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./db_state_change ... 
```

    - 기본 malloc 사용 시: 226.48s user 27.56s system 192% cpu 2:12.18 total

    - jemalloc 사용 시: 123.35s user 30.00s system 127% cpu 1:59.86 total


- 문제 파악을 위해 의도적으로 intermediate database를 드랍하지 않도록 했습니다.
- 상기와 같은 이유로, 상태 전환 이후에 REPLACE 쿼리문을 표시하지만 실제로 실행하지는 않습니다.

- 문제 발생시 이 레포지토리에 이슈로 남겨주시면 빠르게 처리해 드리겠습니다.


## MySQL/MariaDB Useful Commands


#### Create a User

```console
CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';
```

#### Run an SQL script
```console
$ mysql -u root
> source /home/skyer/Desktop/script.sql;
```


#### Print SQL variables
```console
> SELECT VARIABLES LIKE "<var name>";
```

#### Print the sizes of all databases
```console
> SELECT table_schema, ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS "Size (MB)"
> FROM information_schema.TABLES
> GROUP BY table_schema;
```



#### Print the sizes of all tables
```console
> SELECT table_schema, table_name, round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB`
> FROM information_schema.TABLES
> ORDER BY (data_length + index_length) DESC;
```



#### Enable (or reset) the general log
```
$ sudo rm -f /var/log/mysql/mylog
$ mysql -uroot -p123456 -e "set global general_log=0; set global general_log=1; set global general_log_file='/var/log/mysql/mylog';"
```

#### Configure DBMS server's listening IP address
```console
$ vim /etc/mysql/my.cnf
> comment out 'bind-address = 182.162.21.181' or set it to the listening (allowed) IP address
$ sudo service mariadb restart # or 'mysql' in case of MySQL

 # check that the deoman is listening to all IPs (or only the bound IP)
$ sudo netstat -alpn | grep mariadbd # or 'mysqld' in case of MySQL 
```




#### Change the DBMS's database directory
- Link: [https://www.digitalocean.com/community/tutorials/how-to-move-a-mysql-data-directory-to-a-new-location-on-ubuntu-16-04](https://www.digitalocean.com/community/tutorials/how-to-move-a-mysql-data-directory-to-a-new-location-on-ubuntu-16-04)

```console
$ sudo service mysql stop
$ sudo rsync -av /var/lib/mysql /mnt/volume-nyc1-01 # move the DB
$ sudo mv /var/lib/mysql /var/lib/mysql.bak # invalidate the DB
$ sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
------------
   +++ datadir=/mnt/volume-nyc1-01/mysql
   +++[mysqld]
   +++disable_log_bin
------------
$ sudo vim /etc/apparmor.d/tunables/alias
------------
   +++ [label /etc/apparmor.d/tunables/alias]
   +++ alias /var/lib/mysql/ -> /mnt/volume-nyc1-01/mysql/,
------------
$ sudo systemctl restart apparmor
$ sudo mkdir /var/lib/mysql/mysql -p
$ sudo systemctl start mysql
$ mysql -u root -p
```



#### Purge broken MariaDB (10.3)
- Link: [https://askubuntu.com/questions/946646/install-of-mysql-server-after-mariadb-fails/948428#948428](https://askubuntu.com/questions/946646/install-of-mysql-server-after-mariadb-fails/948428#948428)
```console
$ apt search mariadb | grep "\[install"
$ apt search mysql | grep "\[install"
$ sudo dpkg --force depends --purge <package> <package> ...
$ sudo rm -rf /var/lib/mysql* /etc/mysql
$ sudo apt-get --fix-broken install
$ sudo apt autoremove
$ sudo reboot
$ sudo apt-get install mariadb-server
```

#### Root login failure (or lost root login)
- Error message: ERROR 1045 (28000): Access denied for user 'root'@'localhost' (using password: NO)

- Solution: [https://stackoverflow.com/questions/17975120/access-denied-for-user-rootlocalhost-using-password-yes-no-privileges](https://stackoverflow.com/questions/17975120/access-denied-for-user-rootlocalhost-using-password-yes-no-privileges)

```console
$ sudo service mysql stop # or ps aux | grep mysql & kill it
$ sudo mysqld --skip-grant-tables
$ mysql -u root
> use mysql;
> update user set password=PASSWORD("root") where User='root';
> flush privileges;
$ sudo killall mysqld
$ sudo service mysql start
$ mysql -u root -p
```
