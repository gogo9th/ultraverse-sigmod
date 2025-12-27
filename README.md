# Ultraverse Tutorial

## Purge Existing MySQL 
```console
$ sudo apt purge mysql-server mysql-common mariadb-server mariadb-common
$ sudo apt autoremove
$ sudo apt autoclean
$ sudo rm -rf /var/lib/mysql /var/lib/mysql.* /var/log/mysql /etc/mysql
```


## Install Required Software
```console
$ sudo apt install build-essential cmake pkg-config bison flex libboost-all-dev libfmt-dev libspdlog-dev libgvc6 graphviz-dev doxygen libjemalloc-dev libmozjs-102-0 libmozjs-102-dev protoc-gen-go python3-dev libmysqlclient-dev build-essential wget python3 g++-12 clang-15 libc++-15-dev cmake libtbb-dev graphviz libgraphviz-dev libboost-all-dev libmysqlclient-dev libprotobuf-dev protobuf-compiler libfmt-dev libspdlog-dev golang-go
$ pip3 install sqlparse 
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
$ sudo ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1    # if libasio.so.1 does not exist
```



## Install and Setup MySQL (MUST install only either MySQL or MariaDB)

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
$ git clone https://github.com/gogo9th/ultraverse-sigmod
$ cd ultraverse
$ git submodule init
$ git submodule update
$ mkdir build && cd build
$ sed -i "s/python3$/python3.$(echo "$(python3 --version)" | awk '{print $2}' | awk -F . '{print $2}')/g" ../src/CMakeLists.txt
$ CC=clang-15 CXX=clang++-15 cmake ..
$ make -j8
```

## Run BechBase Automatically
When running this automatic test, `/etc/mysql/mysql.conf.d/server.cnf` and `/etc/mysql/mysql.conf.d/server.cnf` should be empty, because the test runs fresh mysql binary and the existing configuration files cause a conflict.

```
$ cd script/esperanza
$ vim envfile
   export ULTRAVERSE_HOME=/root/ultraverse-sigmod/build/src # EDIT THE PATH
   export BENCHBASE_HOME=/root/ultraverse-benchbase  # EDIT THE PATH
   export BENCHBASE_NODE_HOME=/root/benchbase-nodejs # EDIT THE PATH
$ source envfile
$ rm -rf runs cache
$ python3 epinions.py # tpcc.py, tatp.py, seats.py, astore.py
```




## MySQL Useful Commands


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
$ sudo service mysql restart # or 'mysql' in case of MySQL

 # check that the deoman is listening to all IPs (or only the bound IP)
$ sudo netstat -alpn | grep mysqld # or 'mysqld' in case of MySQL 
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
