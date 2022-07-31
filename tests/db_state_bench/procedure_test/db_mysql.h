#ifndef __DBMYSQL_H__
#define __DBMYSQL_H__

#include "mysql.h"
#include <iostream>

#define DB_MYSQL_SUCCESS 0
#define DB_MYSQL_FAILURE (-1)

using namespace std;

class db_mysql
{
public:
  db_mysql();
  ~db_mysql(void);

  int sqlOpen(void);
  int sqlOpen(const char *host, const char *user, const char *pwd, const char *dname, const int port);
  int sqlUse(const char *dname = NULL);
  int sqlClose();

  int sqlConnecting();

  void sqlClear();

  unsigned long sqlEscapeString(char *to, const char *from, unsigned long size);

  int sqlExecute(const char *sql);
  int sqlQuery(const char *sql);
  int sqlQuery(const char *sql, unsigned int len);
  int sqlUpdate(const char *sql);
  int sqlUpdate(const char *sql, unsigned int len);
  int sqlSelect(const char *sql);

  int sqlResultCount();
  short sqlFieldCount();

  bool sqlBegin_row();
  bool sqlNext_row();
  bool sqlPrev_row();
  bool sqlSet_row(int pos);
  bool sqlEnd_row();

  const char *sqlGetValue(unsigned int Index);
  const char *sqlGetValue(const char *name);

  const char *sqlGetValue(unsigned int Index, unsigned long *len);
  const char *sqlGetValue(const char *name, unsigned long *len);

  const char *sqlGetField(unsigned int index);
  bool sqlExistTable(const char *fname);

  void sqlConfig(const char *host, const char *user, const char *pwd, const char *db, const int port)
  {
    db_host = host;
    db_username = user;
    db_password = pwd;
    db_name = db;
    db_port = port;
  }
  void sqlConfig(const char *db)
  {
    db_name = db;
  }

  db_mysql *clone();

private:
  short field_count;
  int result_count;
  int current_row;

protected:
  bool connected;

  MYSQL connection;

  MYSQL_RES *sql_result;
  MYSQL_ROW sql_row;
  MYSQL_FIELD *sql_field;
  unsigned long *lengths;

protected:
  std::string db_host;
  std::string db_username;
  std::string db_password;
  std::string db_name;
  int db_port;
};

#endif
