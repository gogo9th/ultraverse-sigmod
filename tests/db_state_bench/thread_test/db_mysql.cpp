
#include "db_mysql.h"
#include <stdio.h>
#include <string.h>

db_mysql::db_mysql()
{
  result_count = 0;
  current_row = -1;
  connected = false;

  //connection 		= NULL;

  sql_result = NULL;
  sql_row = NULL;
  sql_field = NULL;
}

db_mysql::~db_mysql(void)
{
  if (&connection != NULL)
  {
    sqlClose();
  }
}

int db_mysql::sqlOpen(void)
{
  if (&connection == NULL)
  {
    printf("MySQL connection Null error: no[%d], reason[%s]\n", mysql_errno(&connection), mysql_error(&connection));
    return DB_MYSQL_FAILURE;
  }

  mysql_init(&connection);

  mysql_options(&connection, MYSQL_READ_DEFAULT_GROUP, "client");

  // connect
  if (!mysql_real_connect(&connection, db_host.c_str(), db_username.c_str(), db_password.c_str(), "", db_port, (char *)NULL, 0))
  //if ( !mysql_real_connect(&connection, db_host.c_str(),	db_username.c_str(), db_password.c_str(), db_name.c_str(), db_port, (char*)NULL, 0) )
  {
    printf("MySQL Connection error: no[%d], reason[%s]\n", mysql_errno(&connection), mysql_error(&connection));
    return DB_MYSQL_FAILURE;
  }

  connected = true;

  sqlQuery("set names utf8");

  return DB_MYSQL_SUCCESS;
}

int db_mysql::sqlOpen(const char *host, const char *user, const char *pwd, const char *dname, const int port)
{
  db_host = host;
  db_username = user;
  db_password = pwd;
  db_name = dname;
  db_port = port;

  return sqlOpen();
}

int db_mysql::sqlUse(const char *dname)
{
  if (dname != NULL)
  {
    db_name = dname;
  }

  if (mysql_select_db(&connection, db_name.c_str()))
  {
    printf("MySQL use error: no[%d], reason[%s]\n", mysql_errno(&connection), mysql_error(&connection));
    return DB_MYSQL_FAILURE;
  }

  return DB_MYSQL_SUCCESS;
}

int db_mysql::sqlClose()
{
  if (&connection)
  {
    if (sql_result)
    {
      mysql_free_result(sql_result);
      sql_result = NULL;
    }

    if (connected)
    {
      mysql_close(&connection);
    }

    connected = false;
  }
  return DB_MYSQL_SUCCESS;
}

int db_mysql::sqlConnecting()
{
  if (&connection != NULL && connected)
  {
    if (mysql_ping(&connection) != -1)
      return DB_MYSQL_SUCCESS;
  }
  return DB_MYSQL_FAILURE;
}

void upperCase(char *s, int size)
{
  int i = 0;
  while (*s && i < size)
  {
    *s = toupper(*s);
    s++;
    i++;
  }
}

void lowerCase(char *s, int size)
{
  int i = 0;
  while (*s && i < size)
  {
    *s = tolower(*s);
    s++;
    i++;
  }
}

bool isSelect(const char *strsql)
{
  char command[7];
  char select_command[] = "select";

#ifdef WIN32
  strncpy_s(command, 6, strsql, 6);
#else
  strncpy(command, strsql, 6);
#endif
  command[6] = '\0';

  for (int i = 0; i < 6; i++, strsql++)
  {
    if (!*strsql)
      return false;
    if (tolower(*strsql) != select_command[i])
      return false;
  }
  //  printf("select\n");
  return true;
}

// query execute
int db_mysql::sqlQuery(const char *sql)
{
  // reset
  result_count = 0;
  field_count = 0;
  current_row = -1;

  if (sql_result)
  {
    mysql_free_result(sql_result);
    sql_result = NULL;
  }

  sql_row = NULL;
  sql_field = NULL;

  if (&connection != NULL && connected)
  {
    int querystat = mysql_query(&connection, sql);

    if (querystat == 0)
    {
      sql_result = mysql_store_result(&connection);

      if (sql_result)
      {
        // select
        result_count = (int)mysql_num_rows(sql_result);
        field_count = mysql_num_fields(sql_result);
        sql_field = mysql_fetch_fields(sql_result);

        current_row = -1;

        return DB_MYSQL_SUCCESS;
      }
      else
      {
        if (mysql_field_count(&connection) == 0)
        {
          // Update, Delete, insert query
          result_count = (int)mysql_affected_rows(&connection);
          return DB_MYSQL_SUCCESS;
        }
      }
    }
  }

  printf("MySQL query error: no[%d], reason[%s], query[%s]\n", mysql_errno(&connection), mysql_error(&connection), sql);

  return DB_MYSQL_FAILURE;
}

// query execute
int db_mysql::sqlQuery(const char *sql, unsigned int len)
{
  // reset
  result_count = 0;
  field_count = 0;
  current_row = -1;

  if (sql_result)
  {
    mysql_free_result(sql_result);
    sql_result = NULL;
  }

  sql_row = NULL;
  sql_field = NULL;

  if (&connection != NULL && connected)
  {
    int querystat = mysql_real_query(&connection, sql, len);

    if (querystat == 0)
    {
      sql_result = mysql_store_result(&connection);

      if (sql_result)
      {
        // select
        result_count = (int)mysql_num_rows(sql_result);
        field_count = mysql_num_fields(sql_result);
        sql_field = mysql_fetch_fields(sql_result);

        current_row = -1;

        return DB_MYSQL_SUCCESS;
      }
      else
      {
        if (mysql_field_count(&connection) == 0)
        {
          // Update, Delete, insert query
          result_count = (int)mysql_affected_rows(&connection);
          return DB_MYSQL_SUCCESS;
        }
      }
    }
  }

  printf("MySQL query error: no[%d], reason[%s], query[%s]\n", mysql_errno(&connection), mysql_error(&connection), sql);

  return DB_MYSQL_FAILURE;
}

// insert/update/delete query execute
int db_mysql::sqlUpdate(const char *sql)
{
  result_count = 0;

  if (&connection != NULL && connected)
  {
    int querystat = mysql_query(&connection, sql);
    if (querystat == 0)
    {
      // Update, Delete, insert query
      result_count = (int)mysql_affected_rows(&connection);
      return DB_MYSQL_SUCCESS;
    }
  }
  printf("MySQL update query error: no[%d], reason[%s], query[%s]\n", mysql_errno(&connection), mysql_error(&connection), sql);

  return DB_MYSQL_FAILURE;
}

unsigned long db_mysql::sqlEscapeString(char *to, const char *from, unsigned long size)
{
  if (&connection != NULL && connected)
  {
    return mysql_real_escape_string(&connection, to, from, size);
  }
  return 0;
}

int db_mysql::sqlUpdate(const char *sql, unsigned int len)
{
  result_count = 0;

  if (&connection != NULL && connected)
  {
    int querystat = mysql_real_query(&connection, sql, len);
    if (querystat == 0)
    {
      // Update, Delete, insert query
      result_count = (int)mysql_affected_rows(&connection);
      return DB_MYSQL_SUCCESS;
    }
  }
  printf("MySQL update query error: no[%d], reason[%s], query[%s]\n", mysql_errno(&connection), mysql_error(&connection), sql);

  return DB_MYSQL_FAILURE;
}

int db_mysql::sqlSelect(const char *sql)
{
  result_count = 0;
  field_count = 0;
  current_row = -1;

  if (sql_result)
  {
    mysql_free_result(sql_result);
    sql_result = NULL;
  }

  sql_row = NULL;
  sql_field = NULL;

  if (connected)
  {
    int querystat = mysql_query(&connection, sql);
    if (querystat == 0)
    {
      sql_result = mysql_store_result(&connection);

      if (sql_result)
      {
        result_count = (int)mysql_num_rows(sql_result);
        field_count = mysql_num_fields(sql_result);
        sql_field = mysql_fetch_fields(sql_result);

        current_row = -1;
        return DB_MYSQL_SUCCESS;
      }
    }
  }

  printf("MySQL select query error: no[%d], reason[%s], query[%s]\n", mysql_errno(&connection), mysql_error(&connection), sql);

  return DB_MYSQL_FAILURE;
}

short db_mysql::sqlFieldCount()
{
  if (sql_result)
    return field_count;
  return 0;
}

int db_mysql::sqlResultCount()
{
  if (sql_result)
    return result_count;
  return 0;
}

bool db_mysql::sqlBegin_row()
{
  if (sql_result && result_count > 0)
  {
    current_row = 0;
    mysql_data_seek(sql_result, current_row);
    sql_row = mysql_fetch_row(sql_result);
    lengths = mysql_fetch_lengths(sql_result);
    return true;
  }
  return false;
}

bool db_mysql::sqlNext_row()
{
  if (sql_result && result_count > 0)
  {
    if (current_row >= result_count)
      return false;
    if (current_row == -1)
      return sqlBegin_row();

    sql_row = mysql_fetch_row(sql_result);
    lengths = mysql_fetch_lengths(sql_result);
    current_row++;
    return (sql_row != NULL);
  }
  return false;
}

bool db_mysql::sqlPrev_row()
{
  if (sql_result && result_count > 0)
  {
    if (current_row < 0)
      return false;
    if (current_row >= result_count)
      return sqlEnd_row();
    --current_row;
    mysql_data_seek(sql_result, current_row);
    sql_row = mysql_fetch_row(sql_result);
    lengths = mysql_fetch_lengths(sql_result);
    return (sql_row != NULL);
  }
  return false;
}

bool db_mysql::sqlSet_row(int pos)
{
  if (sql_result && result_count > 0)
  {
    if (pos >= 0 && pos < result_count)
    {
      current_row = pos;
      mysql_data_seek(sql_result, current_row);
      sql_row = mysql_fetch_row(sql_result);
      lengths = mysql_fetch_lengths(sql_result);
      return true;
    }
  }
  return false;
}

bool db_mysql::sqlEnd_row()
{
  if (sql_result && result_count > 0)
  {
    current_row = result_count - 1;
    mysql_data_seek(sql_result, current_row);
    sql_row = mysql_fetch_row(sql_result);
    lengths = mysql_fetch_lengths(sql_result);
    return true;
  }
  return false;
}

const char *db_mysql::sqlGetValue(unsigned int _index)
{
  if (sql_result && sql_row)
  {
    return sql_row[_index];
  }
  return NULL;
}

const char *db_mysql::sqlGetValue(const char *_name)
{
  if (sql_result && sql_row)
  {
    for (short i = 0; i < field_count; i++)
    {
      if (strcmp(sql_field[i].name, _name) == 0)
      {
        return sql_row[i];
      }
    }
  }
  return NULL;
}

const char *db_mysql::sqlGetValue(unsigned int _index, unsigned long *len)
{
  if (sql_result && sql_row)
  {
    *len = lengths[_index];
    return sql_row[_index];
  }
  return NULL;
}

const char *db_mysql::sqlGetValue(const char *_name, unsigned long *len)
{
  if (sql_result && sql_row)
  {
    for (short i = 0; i < field_count; i++)
    {
      if (strcmp(sql_field[i].name, _name) == 0)
      {
        *len = lengths[i];
        return sql_row[i];
      }
    }
  }
  return NULL;
}

const char *db_mysql::sqlGetField(unsigned int index)
{
  if (index >= 0 && index < (unsigned int)field_count)
  {
    return sql_field[index].name;
  }
  return NULL;
}

db_mysql *db_mysql::clone()
{
  db_mysql *tmp = new db_mysql();

  tmp->sqlConfig(db_host.c_str(), db_username.c_str(), db_password.c_str(), db_name.c_str(), db_port);

  return tmp;
}

bool db_mysql::sqlExistTable(const char *fname)
{
  char sql[256];

  sprintf(sql, "show tables like '%s'", fname);

  if (connected)
  {
    int querystat = mysql_query(&connection, sql);
    if (querystat == 0)
    {
      MYSQL_RES *res = mysql_store_result(&connection);
      if (res)
      {
        // select
        result_count = (int)mysql_num_rows(res);
        mysql_free_result(res);

        if (result_count > 0)
          return true;
      }
    }
  }
  return false;
}
