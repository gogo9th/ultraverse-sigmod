
#include <bits/stdc++.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <fstream>
#include <sys/time.h>
#include <unistd.h>

#include "db_mysql.h"
#include "ThreadPool.hpp"


using namespace std;

char str_buff[512];
size_t test_count;
int port, test_case, express_bill_id, express_item_id;
string server, username, password, database, init_file;
string express_user_id, express_user_pw;

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
      if (strTemp[0] == '#')
        continue;

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

  test_case = atoi(getIniData("mysql", "case", filepath).c_str());
  test_count = atoll(getIniData("test", "count", filepath).c_str());
  init_file = getIniData("test", "init_file", filepath);
  express_user_id = getIniData("test", "express_user_id", filepath);
  express_user_pw = getIniData("test", "express_user_pw", filepath);
  express_bill_id = atoi(getIniData("mysql", "express_bill_id", filepath).c_str());
  express_item_id = atoi(getIniData("mysql", "express_item_id", filepath).c_str());
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

void Login(db_mysql *mysql)
{
  memset(str_buff, 0, sizeof(str_buff));
  snprintf(str_buff, sizeof(str_buff) - 1, "CALL Login('%s', '%s', @session_id)",
    express_user_id.c_str(), express_user_pw.c_str());
  mysql->sqlExecute(str_buff);
  mysql->sqlClear();
}

void Logout(db_mysql *mysql)
{
  memset(str_buff, 0, sizeof(str_buff));
  snprintf(str_buff, sizeof(str_buff) - 1, "CALL Logout('%s', @session_id)",
    express_user_id.c_str());
  mysql->sqlExecute(str_buff);
  mysql->sqlClear();
}

double elapsed_time(const struct timeval &start_point, const struct timeval &end_point)
{
  return (double)(end_point.tv_sec) + (double)(end_point.tv_usec) / 1000000.0 - (double)(start_point.tv_sec) - (double)(start_point.tv_usec) / 1000000.0;
}

void test_Login_Logout(db_mysql *mysql)
{
  printf(" - test_Login_Logout\n");

  for (size_t i = 0; i < test_count; ++i)
  {
    mysql->sqlQuery("START TRANSACTION");

    Login(mysql);
    Logout(mysql);

    mysql->sqlQuery("COMMIT");
  }
}

void test_GetUserID(db_mysql *mysql)
{
  printf(" - test_GetUserID\n");

  mysql->sqlQuery("START TRANSACTION");
  Login(mysql);
  mysql->sqlQuery("COMMIT");

  for (size_t i = 0; i < test_count; ++i)
  {
    mysql->sqlQuery("START TRANSACTION");

    memset(str_buff, 0, sizeof(str_buff));
    snprintf(str_buff, sizeof(str_buff) - 1, "CALL GetUserID(@session_id)");
    mysql->sqlExecute(str_buff);
    mysql->sqlClear();

    mysql->sqlQuery("COMMIT");
  }

  mysql->sqlQuery("START TRANSACTION");
  Logout(mysql);
  mysql->sqlQuery("COMMIT");
}

void test_VerifyUserID(db_mysql *mysql)
{
  printf(" - test_VerifyUserID\n");

  mysql->sqlQuery("START TRANSACTION");
  Login(mysql);
  mysql->sqlQuery("COMMIT");

  for (size_t i = 0; i < test_count; ++i)
  {
    mysql->sqlQuery("START TRANSACTION");

    memset(str_buff, 0, sizeof(str_buff));
    snprintf(str_buff, sizeof(str_buff) - 1, "CALL VerifyUserID(%s)", express_user_id.c_str());
    mysql->sqlExecute(str_buff);
    mysql->sqlClear();

    mysql->sqlQuery("COMMIT");
  }

  mysql->sqlQuery("START TRANSACTION");
  Logout(mysql);
  mysql->sqlQuery("COMMIT");
}

void test_VerifyBillCreatorID(db_mysql *mysql)
{
  printf(" - test_VerifyBillCreatorID\n");

  mysql->sqlQuery("START TRANSACTION");
  Login(mysql);
  mysql->sqlQuery("COMMIT");

  for (size_t i = 0; i < test_count; ++i)
  {
    mysql->sqlQuery("START TRANSACTION");

    memset(str_buff, 0, sizeof(str_buff));
    snprintf(str_buff, sizeof(str_buff) - 1, "CALL VerifyBillCreatorID(%d, %s)", express_bill_id, express_user_id.c_str());
    mysql->sqlExecute(str_buff);
    mysql->sqlClear();

    mysql->sqlQuery("COMMIT");
  }

  mysql->sqlQuery("START TRANSACTION");
  Logout(mysql);
  mysql->sqlQuery("COMMIT");
}

void test_VerifyItemCreatorID(db_mysql *mysql)
{
  printf(" - test_VerifyItemCreatorID\n");

  mysql->sqlQuery("START TRANSACTION");
  Login(mysql);
  mysql->sqlQuery("COMMIT");

  for (size_t i = 0; i < test_count; ++i)
  {
    mysql->sqlQuery("START TRANSACTION");

    memset(str_buff, 0, sizeof(str_buff));
    snprintf(str_buff, sizeof(str_buff) - 1, "CALL VerifyItemCreatorID(%d, %s)", express_item_id, express_user_id.c_str());
    mysql->sqlExecute(str_buff);
    mysql->sqlClear();

    mysql->sqlQuery("COMMIT");
  }

  mysql->sqlQuery("START TRANSACTION");
  Logout(mysql);
  mysql->sqlQuery("COMMIT");
}

void test_VerifyBillItem(db_mysql *mysql)
{
  printf(" - test_VerifyBillItem\n");

  mysql->sqlQuery("START TRANSACTION");
  Login(mysql);
  mysql->sqlQuery("COMMIT");

  for (size_t i = 0; i < test_count; ++i)
  {
    mysql->sqlQuery("START TRANSACTION");

    memset(str_buff, 0, sizeof(str_buff));
    snprintf(str_buff, sizeof(str_buff) - 1, "CALL VerifyBillItem(%d, %d)", express_bill_id, express_item_id);
    mysql->sqlExecute(str_buff);
    mysql->sqlClear();

    mysql->sqlQuery("COMMIT");
  }

  mysql->sqlQuery("START TRANSACTION");
  Logout(mysql);
  mysql->sqlQuery("COMMIT");
}

void test_VerifyBillPayerID(db_mysql *mysql)
{
  printf(" - test_VerifyBillPayerID\n");

  mysql->sqlQuery("START TRANSACTION");
  Login(mysql);
  mysql->sqlQuery("COMMIT");

  for (size_t i = 0; i < test_count; ++i)
  {
    mysql->sqlQuery("START TRANSACTION");

    memset(str_buff, 0, sizeof(str_buff));
    snprintf(str_buff, sizeof(str_buff) - 1, "CALL VerifyBillPayerID(%d, %s)", express_bill_id, express_user_id.c_str());
    mysql->sqlExecute(str_buff);
    mysql->sqlClear();

    mysql->sqlQuery("COMMIT");
  }

  mysql->sqlQuery("START TRANSACTION");
  Logout(mysql);
  mysql->sqlQuery("COMMIT");
}

void test_CookieUpdate(db_mysql *mysql)
{
  printf(" - test_CookieUpdate\n");

  mysql->sqlQuery("START TRANSACTION");
  Login(mysql);
  mysql->sqlQuery("COMMIT");

  for (size_t i = 0; i < test_count; ++i)
  {
    mysql->sqlQuery("START TRANSACTION");

    memset(str_buff, 0, sizeof(str_buff));
    snprintf(str_buff, sizeof(str_buff) - 1, "CALL CookieUpdate(@sesson_id, %d)", express_bill_id);
    mysql->sqlExecute(str_buff);
    mysql->sqlClear();

    mysql->sqlQuery("COMMIT");
  }

  mysql->sqlQuery("START TRANSACTION");
  Logout(mysql);
  mysql->sqlQuery("COMMIT");
}

int main(void)
{
  readConfigFile("./procedure_test.ini");

  memset(str_buff, 0, sizeof(str_buff));
  snprintf(str_buff, sizeof(str_buff) - 1, "mysql -u%s -p%s < %s",
    username.c_str(), password.c_str(), init_file.c_str());

  if (system(str_buff) < 0)
  {
    printf(" - init express-db failed\n");
    return -1;
  }

  printf(" - count=%lu\n", test_count);

  db_mysql mysql;
  if (DB_MYSQL_SUCCESS != mysql.sqlOpen(server.c_str(), username.c_str(), password.c_str(), database.c_str(), port))
  {
    printf("  - sqlOpen Fail!\n");
    return -1;
  }
  mysql.sqlUse();

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

  switch(test_case)
  {
    case 1:
      test_Login_Logout(&mysql);
      break;
    case 2:
      test_GetUserID(&mysql);
      break;
    case 3:
      test_VerifyUserID(&mysql);
      break;
    case 4:
      test_VerifyBillCreatorID(&mysql);
      break;
    case 5:
      test_VerifyItemCreatorID(&mysql);
      break;
    case 6:
      test_VerifyBillItem(&mysql);
      break;
    case 7:
      test_VerifyBillPayerID(&mysql);
      break;
    case 8:
      test_CookieUpdate(&mysql);
      break;

    default:
      printf("  - unknown case\n");
      return -1;
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

  mysql.sqlClose();

  return EXIT_SUCCESS;
}
