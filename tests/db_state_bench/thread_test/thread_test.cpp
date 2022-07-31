
#include <bits/stdc++.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <fstream>
#include <sys/time.h>
#include <unistd.h>

#include "db_mysql.h"
#include "ThreadPool.hpp"

#define TABLE_NAME "table_"
#define COLUMN_C_LEN 120

using namespace std;

size_t threadNum, rowNum;
int port;
string server, username, password, database;

ThreadPool *ThreadPool::instance_ = NULL;

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

void readConfigFile(const string &filepath)
{
  server = getIniData("mysql", "host", filepath);
  username = getIniData("mysql", "username", filepath);
  password = getIniData("mysql", "password", filepath);
  database = getIniData("mysql", "database", filepath);
  port = atoi(getIniData("mysql", "port", filepath).c_str());

  threadNum = atoll(getIniData("test", "threads", filepath).c_str());
  rowNum = atoll(getIniData("test", "rows", filepath).c_str());
}

int createDatabase(db_mysql *mysql)
{
  char sqlQuery[512];

  memset(sqlQuery, 0, sizeof(char) * 512);
  sprintf(sqlQuery, "DROP DATABASE IF EXISTS %s;", database.c_str());

  if (mysql->sqlQuery(sqlQuery) < 0)
    return -1;

  memset(sqlQuery, 0, sizeof(char) * 512);
  sprintf(sqlQuery, "CREATE DATABASE IF NOT EXISTS %s;", database.c_str());

  if (mysql->sqlQuery(sqlQuery) < 0)
    return -1;

  memset(sqlQuery, 0, sizeof(char) * 512);
  sprintf(sqlQuery, "USE %s;", database.c_str());

  if (mysql->sqlQuery(sqlQuery) < 0)
    return -1;

  return 0;
}

int createTable(db_mysql *mysql)
{
  char sqlQuery[512];

  for (size_t i = 1; i <= threadNum; i++)
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

    if (mysql->sqlQuery(sqlQuery) < 0)
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

int createRow(db_mysql *mysql, int i, size_t count)
{
  char sqlQuery[512];

  for (size_t j = 1; j <= count; j++)
  {
    //memset(sqlQuery, 0, sizeof(char) * 512);
    sprintf(sqlQuery,
            "INSERT INTO %s%d \
    (id, k, c) VALUES \
    (%d, %d%d, '%d%d%d%d%d %d'); ",
            TABLE_NAME, i, j, i, j, i, i, i, i, i, j);

    if (mysql->sqlQuery(sqlQuery) < 0)
      return -1;
  }

  return 0;
}

double elapsed_time(const struct timeval &start_point, const struct timeval &end_point)
{
  return (double)(end_point.tv_sec) + (double)(end_point.tv_usec) / 1000000.0 - (double)(start_point.tv_sec) - (double)(start_point.tv_usec) / 1000000.0;
}

int main(void)
{
  readConfigFile("./thread_test.ini");

  printf(" - threads=%u, rows=%u\n", threadNum, rowNum);

  {
    db_mysql mysql;
    if (DB_MYSQL_SUCCESS != mysql.sqlOpen(server.c_str(), username.c_str(), password.c_str(), database.c_str(), port))
    {
      printf("  - sqlOpen Fail!\n");
      return -1;
    }

    if (createDatabase(&mysql) < 0)
    {
      printf("  - createDatabase Fail!\n");
      return -1;
    }

    mysql.sqlUse();

    if (createTable(&mysql) < 0)
    {
      printf("  - createTable Fail!\n");
      return -1;
    }

    mysql.sqlClose();
  }

  ThreadPool::Instance().Resize(threadNum);

  std::vector<std::future<void>> thread_futures;

  time_t t;
  struct timeval start_point, end_point;

  t = time(NULL);
  auto start_time = *localtime(&t);
  printf("  - start time [%04d-%02d-%02d %02d:%02d:%02d]\n",
    start_time.tm_year + 1900,
    start_time.tm_mon + 1,
    start_time.tm_mday,
    start_time.tm_hour,
    start_time.tm_min,
    start_time.tm_sec);

  gettimeofday(&start_point, NULL);

  for (size_t i = 0; i < threadNum; ++i)
  {
    thread_futures.emplace_back(ThreadPool::Instance().EnqueueJob([](int i)
    {
      db_mysql mysql;
      mysql.sqlOpen(server.c_str(), username.c_str(), password.c_str(), database.c_str(), port);
      mysql.sqlUse();

      mysql.sqlQuery("START TRANSACTION");

      createRow(&mysql, i + 1, rowNum / threadNum);

      mysql.sqlQuery("COMMIT");

      mysql.sqlClose();

    }, i));
  }

  for (auto &f : thread_futures)
  {
    f.get();
  }

  gettimeofday(&end_point, NULL);

  t = time(NULL);
  auto end_time = *localtime(&t);
  printf("  - end time [%04d-%02d-%02d %02d:%02d:%02d]\n",
    end_time.tm_year + 1900,
    end_time.tm_mon + 1,
    end_time.tm_mday,
    end_time.tm_hour,
    end_time.tm_min,
    end_time.tm_sec);

  printf("  - elapsed time [%f]sec\n", elapsed_time(start_point, end_point));

  return EXIT_SUCCESS;
}
