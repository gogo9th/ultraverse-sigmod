#include <sstream>
#include <string>
#include <chrono>
#include <iomanip>
#include <spawn.h>
#include <sys/wait.h>

#include "db_state_api.h"
#include "state_log_hdr.h"

inline int run_query(MYSQL *mysql, const std::string &query)
{
  if (mysql_real_query(mysql, query.c_str(), query.size()) != 0)
  {
    return -1;
  }

  return 0;
}

inline static std::string format_time(time_t t, const std::string &format)
{
  struct tm timeinfo;
  if (localtime_r(&t, &timeinfo) == NULL)
    return std::string();

  std::stringstream ss;
  ss << std::put_time(&timeinfo, format.c_str());
  return ss.str();
}

inline int execute(const char *path, const char * const argv[])
{
  pid_t pid;
  int ret = posix_spawn(&pid, path, NULL, NULL, (char * const *)argv, NULL);

  int status;
  waitpid(pid, &status, 0);

  return ret;
}

int Stitch_SQL_Start(MYSQL *mysql)
{
  std::stringstream ss;

  ss.str(std::string());
  ss << "CREATE DATABASE IF NOT EXISTS " STATE_GROUP_DATABASE;
  if (run_query(mysql, ss.str()) != 0)
    return -1;

  ss.str(std::string());
  ss << "CREATE TABLE IF NOT EXISTS " STATE_GROUP_DATABASE ".`" STATE_GROUP_TABLE
        "` (`group_id` INT NOT NULL AUTO_INCREMENT, "
        "`time` TIMESTAMP(6) NULL DEFAULT NULL, "
        "INDEX `group_id` (`group_id`, `time`))";
  if (run_query(mysql, ss.str()) != 0)
    return -1;

  MYSQL_RES *query_result = NULL;
  MYSQL_ROW query_row;

  int id = -1;
  ss.str(std::string());
  ss << "SELECT AUTO_INCREMENT "
        "FROM information_schema.`TABLES` "
        "WHERE TABLE_SCHEMA='" STATE_GROUP_DATABASE "' "
        "AND TABLE_NAME='" STATE_GROUP_TABLE "'";
  if (run_query(mysql, ss.str()) != 0)
    return -1;
  if ((query_result = mysql_store_result(mysql)) == NULL)
    return -1;
  while ((query_row = mysql_fetch_row(query_result)))
  {
    char *end;
    id = std::strtol(query_row[0], &end, 10);
  }
  mysql_free_result(query_result);
  if (id <= 0)
    id = 1;

  // 할당받은 id 로 기록된 예전 내용은 전부 삭제
  ss.str(std::string());
  ss << "DELETE FROM " STATE_GROUP_DATABASE ".`" STATE_GROUP_TABLE
        "` WHERE `group_id`="
     << id;
  if (run_query(mysql, ss.str()) != 0)
    return -1;

  // auto increment 로 할당받은 id 를 확보
  // 같은 id 를 할당받지 못하도록 방지
  auto now = std::chrono::high_resolution_clock::now();
  auto sec = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
  auto sec_part = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

  ss.str(std::string());
  ss << "INSERT INTO " STATE_GROUP_DATABASE ".`" STATE_GROUP_TABLE
        "` (`group_id`, `time`) VALUES ('"
     << id << "', '"
     << format_time(sec, "%Y-%m-%d %H:%M:%S") << '.' << sec_part << "')";
  if (run_query(mysql, ss.str()) != 0)
    return -1;

  return id;
}

int Stitch_SQL_Command(MYSQL *mysql, int id, const char *query)
{
  auto now = std::chrono::high_resolution_clock::now();
  auto sec = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
  auto sec_part = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

  std::stringstream ss;

  ss.str(std::string());
  ss << "SET TIMESTAMP=" + sec << '.' << sec_part;
  if (run_query(mysql, ss.str()) != 0)
    return -1;

  ss.str(std::string());
  ss << "INSERT INTO " STATE_GROUP_DATABASE ".`" STATE_GROUP_TABLE
        "` (`group_id`, `time`) VALUES ('"
     << id << "', '"
     << format_time(sec, "%Y-%m-%d %H:%M:%S") << '.' << sec_part << "')";
  if (run_query(mysql, ss.str()) != 0)
    return -1;

  ss.str(std::string());
  ss << STATE_GROUP_COMMENT_STRING << query;

  return run_query(mysql, ss.str());
}

int Stitch_SQL_Abort(MYSQL *mysql, int id)
{
  std::string str_id = std::to_string(id);
  const char *path = "db_state_change";
  const char * const argv[] = {path, "-uroot", "-p123456", "--gid", str_id.c_str(), NULL};

  return execute(path, argv);
}
