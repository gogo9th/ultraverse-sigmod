
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <fstream>
#include <sys/time.h>
#include <unistd.h>

#include "db_mysql.h"

#define TABLE_NAME "db_test"
#define COLUMN_C_LEN 120

using namespace std;

db_mysql *dmysql = NULL;

int tableNum, rowNum, tableDepandency, tableQueryNum, depandencyQueryNum;
int port;

string server, username, password, database, db_state_change, user_sqlfile, init_mysql_sh, mysqlbinlog, binlogfile;

char file_init[128], file_undo[128], file_redo[128], str_time_undo[32], str_time_redo[32], str_time_start[32];

struct tm time_start, time_undo, time_redo;

double operating_time_mysql;
double operating_time_dbc;

string getIniData(string _title, string _data, string _path)
{
  bool bCheck = false;
  string strTemp;
  string strErase;

  ifstream inFile(_path);

  if (inFile.is_open())
  {
    while (!inFile.eof())
    {
      getline(inFile, strTemp);

      if (strTemp.find(_title) != std::string::npos)
      {
        bCheck = true;
        continue;
      }

      if (bCheck)
      {
        if (strTemp.find(_data) != std::string::npos)
        {
          if (strTemp.find('=') != std::string::npos)
          {
            strErase = strtok((char *)strTemp.c_str(), "=");
            strTemp = strtok(NULL, "");
            break;
          }
        }
      }
    }
    inFile.close();
  }

  return strTemp;
}

void readConfigFile(string &filepath)
{
  server = getIniData("mysql", "host", filepath);
  username = getIniData("mysql", "username", filepath);
  password = getIniData("mysql", "password", filepath);
  database = getIniData("mysql", "database", filepath);
  port = atoi(getIniData("mysql", "port", filepath).c_str());

  tableNum = atoi(getIniData("test", "tables", filepath).c_str());
  rowNum = atoi(getIniData("test", "rows", filepath).c_str());
  tableDepandency = atoi(getIniData("test", "tableDepandency", filepath).c_str());
  tableQueryNum = atoi(getIniData("test", "tableQuerys", filepath).c_str());
  depandencyQueryNum = atoi(getIniData("test", "depandencyQuerys", filepath).c_str());

  db_state_change = getIniData("path", "db_state_change", filepath);
  mysqlbinlog = getIniData("path", "mysqlbinlog", filepath);
  binlogfile = getIniData("path", "binlogfile", filepath);
  user_sqlfile = getIniData("path", "user_sqlfile", filepath);
  init_mysql_sh = getIniData("path", "init_mysql_sh", filepath);

  if (tableNum < tableDepandency || tableDepandency < 0)
    tableDepandency = tableNum;
  if (rowNum < depandencyQueryNum)
    tableDepandency = rowNum;
  if (depandencyQueryNum < 0)
    depandencyQueryNum = 0;
}

int createDatabase()
{
  char sqlQuery[512];

  memset(sqlQuery, 0, sizeof(char) * 512);
  sprintf(sqlQuery, "DROP DATABASE IF EXISTS %s;", database.c_str());

  if (dmysql->sqlQuery(sqlQuery) < 0)
    return -1;

  memset(sqlQuery, 0, sizeof(char) * 512);
  sprintf(sqlQuery, "CREATE DATABASE IF NOT EXISTS %s;", database.c_str());

  if (dmysql->sqlQuery(sqlQuery) < 0)
    return -1;

  memset(sqlQuery, 0, sizeof(char) * 512);
  sprintf(sqlQuery, "USE %s;", database.c_str());

  if (dmysql->sqlQuery(sqlQuery) < 0)
    return -1;

  return 0;
}

int createTable()
{
  char sqlQuery[512];

  for (int i = 1; i <= tableNum; i++)
  {
    memset(sqlQuery, 0, sizeof(char) * 512);
    sprintf(sqlQuery,
            "CREATE TABLE %s%d( \
    id  INTEGER, \
    k   INTEGER DEFAULT '0' NOT NULL, \
    c   CHAR(%d) DEFAULT '' NOT NULL, \
    PRIMARY KEY (id) \
    );",
            TABLE_NAME, i, COLUMN_C_LEN);

    if (dmysql->sqlQuery(sqlQuery) < 0)
      return -1;
  }

  return 0;
}

string getRandomString(int length)
{
  string str;
  char c;
  for (int i = 0; i < length; i++)
  {
    c = 'a' + (rand() % 26);
    str.push_back(c);
  }

  return str;
}

int createRow()
{
  char sqlQuery[512];

  srand((unsigned int)time(NULL));

  for (int i = 1; i <= tableNum; i++)
  {
    for (int j = 1; j <= rowNum; j++)
    {
      memset(sqlQuery, 0, sizeof(char) * 512);
      sprintf(sqlQuery,
              "INSERT INTO %s%d \
      (id, k, c) VALUES \
      (%d, %d%d, '%d%d%d%d%d %d'); ",
              TABLE_NAME, i, j, i, j, i, i, i, i, i, j);

      if (dmysql->sqlQuery(sqlQuery) < 0)
        return -1;
    }
  }

  return 0;
}

int createData()
{
  //기존 데이터베이스 삭제 및 생성
  if (createDatabase() < 0)
  {
    printf("  - createDatabase Fail!\n");
    return -1;
  }
  printf("  - createDatabase [%s] OK!\n", database.c_str());

  //테이블 생성
  if (createTable() < 0)
  {
    printf("  - createTable Fail!\n");
    return -1;
  }
  printf("  - createTable [%d] OK!\n", tableNum);

  //row 생성
  if (createRow() < 0)
  {
    printf("  - createRow Fail!\n");
    return -1;
  }
  printf("  - createRow [%d] OK!\n", rowNum);

  return 0;
}

int runQuerys(string filepath)
{
  int runQueryNum = 0;
  int runDepandencyQueryNum = 0;

  char sqlQuery[512];

  bool dep_flag = false;
  int dep_table = 0;

  ofstream file(filepath.data());
  if (!file.is_open())
  {
    printf("%s open fail\n", filepath.c_str());
    goto err;
  }

  for (int i = 1; i <= tableNum; i++)
  {
    if (i < tableNum)
      dep_table = i + 1;
    if (i == tableNum)
      dep_table = 1;

    for (int j = 1; j <= tableQueryNum; j++)
    {
      memset(sqlQuery, 0, sizeof(char) * 512);

      dep_flag = false;

      if ((j <= depandencyQueryNum && i <= tableDepandency) || (j <= depandencyQueryNum && tableNum == tableDepandency))
        dep_flag = true;

      if (dep_flag)
      {
        sprintf(sqlQuery,
                "UPDATE `%s`.%s%d a, `%s`.%s%d b SET a.k=(SELECT k FROM `%s`.%s%d b WHERE b.id=%d), a.c=(SELECT c FROM `%s`.%s%d b WHERE b.id=%d) WHERE a.id = %d;",
                database.c_str(), TABLE_NAME, i, database.c_str(), TABLE_NAME, dep_table, database.c_str(), TABLE_NAME, dep_table, j, database.c_str(), TABLE_NAME, dep_table, j, j);

        runDepandencyQueryNum++;
      }
      else
      {
        sprintf(sqlQuery,
                "UPDATE `%s`.%s%d SET k=%d, c='%s' WHERE id=%d;",
                database.c_str(), TABLE_NAME, i, rand(), getRandomString(COLUMN_C_LEN).c_str(), j);
      }

      file << sqlQuery << "\n";

      if (dmysql->sqlQuery(sqlQuery) < 0)
        goto err;

      runQueryNum++;
    }
  }

  printf("  - Run Update Query, tableDepandency[%d] tableQuerys[%d] depandencyQuerys[%d] OK!\n", tableDepandency, tableQueryNum, depandencyQueryNum);
  printf("  - Run Result, run Querys[%d] runDepanQuerys[%d] OK!\n", runQueryNum, runDepandencyQueryNum);

  return 0;
err:
  file.close();
  return -1;
}

int mysqlOpen()
{
  if (dmysql == NULL)
  {
    dmysql = new db_mysql();

    if (dmysql->sqlOpen(server.c_str(), username.c_str(), password.c_str(), database.c_str(), port) != DB_MYSQL_SUCCESS)
    {
      printf("  - sqlOpen Fail!\n");
      return -1;
    }

    printf("  - sqlOpen OK!\n");
  }
  return 0;
}

int runSystem(string cmd)
{
  int ret = system(cmd.c_str());
  printf("  - ret[%d], cmd[%s]\n", ret, cmd.c_str());
  return ret;
}

int mysqldump(string filename)
{
  char str[512];

  memset(str, 0, 512);
  sprintf(str, "mysqldump -u%s -p%s --databases %s > %s", username.c_str(), password.c_str(), database.c_str(), filename.c_str());
  if (runSystem(str) < 0)
  {
    printf("  - mysqldump err!\n");
    return -1;
  }
  printf("  - %s Export OK!\n", filename.c_str());

  return 0;
}
//1.
int prepare()
{
  printf("\n 1. prepare Run!\n");

  char filename[128];

  time_t t = time(NULL);
  time_start = *localtime(&t);

  memset(str_time_start, 0, 32);
  sprintf(str_time_start, "%04d%02d%02d-%02d%02d%02d", time_start.tm_year + 1900, time_start.tm_mon + 1, time_start.tm_mday, time_start.tm_hour, time_start.tm_min, time_start.tm_sec);

  if (mysqlOpen() < 0)
    goto err;

  memset(file_init, 0, 128);
  sprintf(file_init, "%s_init.sql", str_time_start);

  //1) table 및 row 생성
  if (createData() < 0)
  {
    printf("  - createData Fail!\n");
    goto err;
  }

  printf("  - create Table[%d]&Row[%d] OK!\n", tableNum, rowNum);

  //2) init sql 파일 생성
  if (mysqldump(file_init) < 0)
    goto err;

  //3) undo 전 Update Query 실행 & update 쿼리 파일로 생성
  memset(file_undo, 0, 128);
  sprintf(file_undo, "%s_undo", str_time_start);
  if (runQuerys(file_undo) < 0)
  {
    printf("  - undo run query Fail!\n");
    goto err;
  }
  printf("  - undo run query file create OK!\n");

  memset(filename, 0, 128);
  sprintf(filename, "%s_undo.sql", str_time_start);
  if (mysqldump(filename) < 0)
    goto err;

  //4) update query 실행 & export sql file
  memset(file_redo, 0, 128);
  sprintf(file_redo, "%s_redo", str_time_start);
  if (runQuerys(file_redo) < 0)
  {
    printf("  - redo run query Fail!\n");
    goto err;
  }
  printf("  - redo run query file create OK!\n");

  memset(filename, 0, 128);
  sprintf(filename, "%s_redo.sql", str_time_start);
  if (mysqldump(filename) < 0)
    goto err;

  printf("  - prepare OK!\n");

  return 0;
err:

  return -1;
}

int runQuery(string filepath)
{
  ifstream file(filepath.data());

  if (!file.is_open())
  {
    printf("%s open fail\n", filepath.c_str());
    return -1;
  }

  string line;
  while (getline(file, line))
  {
    if (dmysql->sqlQuery(line.c_str()) < 0)
    {
      printf("fail query - %s\n", line.c_str());
      file.close();
      return -1;
    }
  }

  file.close();

  return 0;
}

int initMysql()
{
  char str[512];

  dmysql->sqlClose();
  dmysql = NULL;

  //1) mysql 초기화
  if (runSystem(init_mysql_sh.c_str()) < 0)
  {
    printf("  - init mysql err!\n");
    return -1;
  }
  printf("  - init mysql OK!\n");

  if (mysqlOpen() < 0)
    return -1;

  createDatabase();

  //2) init.sql 삽입
  memset(str, 0, 512);
  sprintf(str, "mysql -u%s -p%s < %s", username.c_str(), password.c_str(), file_init);

  if (runSystem(str) < 0)
  {
    printf("  - init sql import err!\n");
    return -1;
  }
  printf("  - init sql import OK!\n");

  //3) undo query 실행
  if (runQuery(file_undo) < 0)
  {
    printf("  - undo query excute err!\n");
    return -1;
  }
  printf("  - undo query excute OK!\n");

  //4) undo 시간 기록
  time_t t = time(NULL);
  time_undo = *localtime(&t);

  memset(str_time_undo, 0, 32);
  sprintf(str_time_undo, "%04d-%02d-%02d %02d:%02d:%02d.000", time_undo.tm_year + 1900, time_undo.tm_mon + 1, time_undo.tm_mday, time_undo.tm_hour, time_undo.tm_min, time_undo.tm_sec);
  printf("  - undo time [%s]\n", str_time_undo);

  //5) redo query 실행
  if (runQuery(file_redo) < 0)
  {
    printf("  - redo query excute err!\n");
    return -1;
  }
  printf("  - redo query excute OK!\n");

  //6) redo 시간 기록
  t = time(NULL);
  time_redo = *localtime(&t);

  memset(str_time_redo, 0, 32);
  sprintf(str_time_redo, "%04d-%02d-%02d %02d:%02d:%02d.000", time_redo.tm_year + 1900, time_redo.tm_mon + 1, time_redo.tm_mday, time_redo.tm_hour, time_redo.tm_min, time_redo.tm_sec);
  printf("  - redo time [%s]\n", str_time_redo);

  return 0;
}

double elapsed_time(const struct timeval &start_point, const struct timeval &end_point)
{
  return (double)(end_point.tv_sec) + (double)(end_point.tv_usec) / 1000000.0 - (double)(start_point.tv_sec) - (double)(start_point.tv_usec) / 1000000.0;
}

int runMysql()
{
  printf("\n 2. runMysql Run!\n");

  char str[512];

  //1) 초기화
  if (initMysql() < 0)
    return -1;

  //2) 시간 측정 시작
  struct timeval start_point, end_point;
  struct timeval end_undo_point, end_user_point;
  gettimeofday(&start_point, NULL);

  //3) mysql flesh back
  memset(str, 0, 512);
  sprintf(str, "%s %s --flashback --start-datetime=\"%s\" --stop-datetime=\"%s\" | mysql -u%s -p%s", mysqlbinlog.c_str(), binlogfile.c_str(), str_time_undo, str_time_redo, username.c_str(), password.c_str());

  if (runSystem(str) < 0)
  {
    printf("  - undo flashback err!\n");
    return -1;
  }
  printf("  - undo flashback OK!\n");
  gettimeofday(&end_undo_point, NULL);

  //4) usersql 실행
  memset(str, 0, 512);
  sprintf(str, "mysql -u%s -p%s < %s", username.c_str(), password.c_str(), user_sqlfile.c_str());

  if (runSystem(str) < 0)
  {
    printf("  - import user.sql err!\n");
    return -1;
  }
  printf("  - import user.sql OK!\n");
  gettimeofday(&end_user_point, NULL);

  //5) mysql redo
  memset(str, 0, 512);
  sprintf(str, "%s %s --start-datetime=\"%s\" --stop-datetime=\"%s\" | mysql -u%s -p%s", mysqlbinlog.c_str(), binlogfile.c_str(), str_time_undo, str_time_redo, username.c_str(), password.c_str());

  if (runSystem(str) < 0)
  {
    printf("  - redo flashback err!\n");
    return -1;
  }
  printf("  - redo flashback OK!\n");

  //6) run 시간 체크
  gettimeofday(&end_point, NULL);
  operating_time_mysql = elapsed_time(start_point, end_point);

  printf("  - undo Time [%f]sec\n", elapsed_time(start_point, end_undo_point));
  printf("  - user Time [%f]sec\n", elapsed_time(end_undo_point, end_user_point));
  printf("  - redo Time [%f]sec\n", elapsed_time(end_user_point, end_point));
  printf("  - run Time [%f]sec\n", operating_time_mysql);

  //7) 결과 저장
  memset(str, 0, 512);
  sprintf(str, "%s_result_mysql.sql", str_time_start);

  if (mysqldump(str) < 0)
  {
    printf("  - mysqldump err!\n");
    return -1;
  }
  printf("  - mysqldump OK!\n");

  printf("  - runMysql OK!\n");

  return 0;
}

int runDbStateCange()
{
  printf("\n 3. runDbStateCange Run!\n");

  char str[512];

  //1) 초기화
  if (initMysql() < 0)
    return -1;

  //2) 시간 측정 시작
  struct timeval start_point, end_point;
  gettimeofday(&start_point, NULL);

  //3) undo db_state_change
  memset(str, 0, 512);
  sprintf(str, "%s  --query %s -u%s -p%s --start-datetime=\"%s\" --graph %s.svg",
          db_state_change.c_str(), user_sqlfile.c_str(), username.c_str(), password.c_str(), str_time_undo, str_time_start);

  if (runSystem(str) < 0)
  {
    printf("  - undo db_state_change err!\n");
    return -1;
  }
  printf("  - undo db_state_change OK!\n");

  //4) run 시간 체크
  gettimeofday(&end_point, NULL);
  operating_time_dbc = elapsed_time(start_point, end_point);

  printf("  - run Time [%f]sec\n", operating_time_dbc);

  //5) 결과 저장
  memset(str, 0, 512);
  sprintf(str, "%s_result_dbc.sql", str_time_start);

  if (mysqldump(str) < 0)
  {
    printf("  - exportResult err!\n");
    return -1;
  }
  printf("  - exportResult OK!\n");

  printf("  - runDbStateCange OK!\n");
  return 0;
}

//4.
int resultDiff()
{
  printf("\n 4. resultDiff Run!\n");

  char str[512];

  //1) diff 실행
  memset(str, 0, 512);
  sprintf(str, "diff %s_result_dbc.sql %s_result_mysql.sql > %s_diff", str_time_start, str_time_start, str_time_start);

  if (runSystem(str) < 0)
  {
    printf("  - resultDiff err!\n");
    return -1;
  }

  printf("  * check diff file[%s_diff], mysql run time[%f]sec, db_state_change run time[%f]sec\n", str_time_start, operating_time_mysql, operating_time_dbc);

  printf("  - resultDiff OK!\n");
  return 0;
}

int main(void)
{
  string filepath = "./db_state_test.ini";
  //printf("\n Read Ini File [db_state_test.ini]\n");

  readConfigFile(filepath);

  printf(" - host=%s, username=%s, password=%s, database=%s, port=%d\n", server.c_str(), username.c_str(), password.c_str(), database.c_str(), port);
  printf(" - tables=%d, rows=%d, tableDepandency=%d, tableQuerys=%d, depandencyQuerys=%d\n", tableNum, rowNum, tableDepandency, tableQueryNum, depandencyQueryNum);

  //1. prepare
  if (prepare() < 0)
  {
    printf("  - prepare Fail!\n");
    goto err;
  }

  //2. run mysql
  if (runMysql() < 0)
  {
    printf("  - runMysql Fail!\n");
    goto err;
  }

  //3. run db_state_change
  if (runDbStateCange() < 0)
  {
    printf("  - runDbStateCange Fail!\n");
    goto err;
  }

  //4. diff
  if (resultDiff() < 0)
  {
    printf("  - resultDiff Fail!\n");
    goto err;
  }

  //종료
  if (dmysql != NULL)
  {
    dmysql->sqlClose();
    dmysql = NULL;
  }

  return EXIT_SUCCESS;

err:
  if (dmysql != NULL)
  {
    dmysql->sqlClose();
    dmysql = NULL;
  }
  return EXIT_FAILURE;
}
