#include <boost/program_options.hpp>

#include <stdarg.h>
#include <iostream>
#include <fstream>

#include "nlohmann/json.hpp"

#include "client_priv.h"
#include "my_dir.h"

#include "StateThreadPool.h"
#include "StateUserQuery.h"
#include "StateTable.h"
#include "StateGraphBoost.h"

#ifndef QUERY_ANALYZER_VERSION
#define QUERY_ANALYZER_VERSION 1.0
#endif

#define STRINGIZE(x) #x
#define STRINGIZE_VALUE_OF(x) STRINGIZE(x)

StateThreadPool *StateThreadPool::instance_ = NULL;
static std::string host = "127.0.0.1";
static std::string user = "root";
static std::string pass = "123456";
static std::string query_filepath = "";
static std::string graph_filepath = "graph.svg";
static std::string info_filepath = "info.json";
static std::string key_column_name = "";

StateUserQuery *state_user_query = NULL;
StateGraphBoost *state_graph = NULL;

static void error_or_warning(const char *format, va_list args, const char *msg)
{
  fprintf(stderr, "%s: ", msg);
  vfprintf(stderr, format, args);
  fprintf(stderr, "\n");
  fflush(stderr);
}

void debug(const char *format, ...)
{
  va_list args;
  va_start(args, format);
  fprintf(stdout, "DEBUG: ");
  vfprintf(stdout, format, args);
  fprintf(stdout, "\n");
  fflush(stdout);
  va_end(args);
}

void error(const char *format, ...)
{
  va_list args;
  va_start(args, format);
  error_or_warning(format, args, "ERROR");
  va_end(args);
}

state_log_time null_time;
state_log_time &get_start_time()
{
  return null_time;
}

state_log_time &get_end_time()
{
  return null_time;
}

state_log_time &get_hash_query_time()
{
  return null_time;
}

void convert_str_to_timestamp(const char *const str, state_log_time *time)
{
}

bool is_hash_matched()
{
  return false;
}

bool is_using_candidate()
{
  return true;
}

const char *get_key_column_table_name()
{
  return NULL;
}

static const char *global_key_column_name = NULL;
const char *get_key_column_name()
{
  return global_key_column_name;
}

void set_key_column_name(const char *name)
{
  global_key_column_name = name;
}

en_state_log_column_data_type get_key_column_type()
{
  return en_column_data_null;
}

bool process_binary_log(const std::string &filename, BINARY_FUNC_TYPE process)
{
  return false;
}

MYSQL *open_mysql()
{
  MYSQL *local_mysql = mysql_init(NULL);

  if (!local_mysql)
  {
    error("Failed on mysql_init.");
    exit(1);
  }

  unsigned int timeout = 15;
  mysql_options(local_mysql, MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&timeout);
  mysql_options(local_mysql, MYSQL_OPT_CONNECT_ATTR_RESET, 0);
  mysql_options4(local_mysql, MYSQL_OPT_CONNECT_ATTR_ADD,
                 "program_name", PROGRAM_NAME);

  if (!mysql_real_connect(local_mysql, host.c_str(), user.c_str(), pass.c_str(), STATE_CHANGE_DATABASE, 0, NULL, 0))
  {
    error("Failed on connect: %s", mysql_error(local_mysql));
    exit(1);
  }

  my_bool reconnect = 1;
  mysql_options(local_mysql, MYSQL_OPT_RECONNECT, &reconnect);

  if (mysql_autocommit(local_mysql, false) != 0)
  {
    error("failed to autocommit: %s", mysql_error(local_mysql));
    exit(1);
  }

  return local_mysql;
}

void close_mysql(MYSQL *local_mysql)
{
  mysql_close(local_mysql);
}

int main(int argc, char **argv)
{
  // 파라미터 파싱
  boost::program_options::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce a help screen")
    ("version,v", "print the version number")
    ("user,u", boost::program_options::value<std::string>(), "Connect to the remote server as username")
    ("password,p", boost::program_options::value<std::string>(), "Password to connect to remote server")
    ("query,q", boost::program_options::value<std::string>()->required(), "input query file path")
    ("graph,g", boost::program_options::value<std::string>(), "output svg graph file path")
    ("info,i", boost::program_options::value<std::string>(), "output json info file path")
    ("key-name", boost::program_options::value<std::string>()->required(), "custom key column name")
  ;

  boost::program_options::variables_map vm;
  try
  {
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  }
  catch (std::exception &e)
  {
    std::cout << "parse_command_line failed" << std::endl;
    std::cout << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  if (vm.count("help"))
  {
    std::cout << "Usage: query_analyzer [options]" << std::endl;
    std::cout << desc << std::endl;
    return EXIT_FAILURE;
  }
  else if (vm.count("version"))
  {
    std::cout << "Version " << STRINGIZE_VALUE_OF(QUERY_ANALYZER_VERSION) << std::endl;
    return EXIT_FAILURE;
  }

  if (vm.count("query"))
  {
    query_filepath = vm["query"].as<std::string>();
  }
  else
  {
    std::cout << "Usage: query_analyzer [options]" << std::endl;
    std::cout << desc << std::endl;
    return EXIT_FAILURE;
  }

  if (vm.count("user"))
  {
    user = vm["user"].as<std::string>();
  }
  if (vm.count("password"))
  {
    pass = vm["password"].as<std::string>();
  }
  if (vm.count("graph"))
  {
    graph_filepath = vm["graph"].as<std::string>();
  }
  if (vm.count("info"))
  {
    info_filepath = vm["info"].as<std::string>();
  }
  if (vm.count("key-name"))
  {
    key_column_name = vm["key-name"].as<std::string>();
    auto vec = StateUserQuery::SplitDBNameAndTableName(key_column_name);
    if (vec.size() != 3)
    {
      error("You must specify the column name in the following format");
      error("DB.TABLE.COLUMN");
      return EXIT_FAILURE;
    }
    key_column_name = vec[0] + ".`" + vec[1] + "`." + vec[2];
  }

  StateThreadPool::Instance().Resize(std::thread::hardware_concurrency() + 1);
  // StateThreadPool::Instance().Resize(1);

  // 쿼리 파일 파싱
  // 쿼리 서버로 요청
  state_user_query = new StateUserQuery();
  if (state_user_query->SetQueryFile(query_filepath, true) != EXIT_SUCCESS)
  {
    error("[main] StateUserQuery::SetQueryFile error");
    StateThreadPool::Instance().Release();
    StateThreadPool::Instance().ReleaseMySql();
    return EXIT_FAILURE;
  }

  MY_STAT my_file_stat;

  auto info_log_filepath = info_filepath + ".log";

  if (my_stat(info_filepath.c_str(), &my_file_stat, 0) &&
      remove(info_filepath.c_str()) != 0)
  {
    error("[main] Unable to remove the info_filepath (%s)(%s)", info_filepath, strerror(errno));
    StateThreadPool::Instance().Release();
    StateThreadPool::Instance().ReleaseMySql();
    return EXIT_FAILURE;
  }

  if (my_stat(info_log_filepath.c_str(), &my_file_stat, 0) &&
      remove(info_log_filepath.c_str()) != 0)
  {
    error("[main] Unable to remove the info_log_filepath (%s)(%s)", info_log_filepath, strerror(errno));
    StateThreadPool::Instance().Release();
    StateThreadPool::Instance().ReleaseMySql();
    return EXIT_FAILURE;
  }

  auto user_transaction_list = state_user_query->GetUserTransactionList();

  bool is_valid = StateTable::ApplyCandidateColumn(user_transaction_list, key_column_name);
  set_key_column_name(key_column_name.c_str());

  bool is_empty_partition = false;
  if (!is_valid)
  {
    // WHERE절에 여러 개의 OR가 있고 이 중 한 OR에서라도 key_column_name를 명시하지 않았다면,
    // 이 쿼리는 파티션 컬럼값이 null이어야 합니다 (그렇지 않으면 의존 분석시 문제 발생 가능).
    is_empty_partition = true;
  }
  if (key_column_name.size() == 0)
  {
    is_empty_partition = true;
  }

  // 쿼리 정보 출력
  {
    // json output
    std::ofstream fout;
    fout.open(info_filepath);
    size_t idx = 0;
    nlohmann::json j;
    for (auto &t : user_transaction_list)
    {
      ++idx;

      t->xid = idx;

      nlohmann::json obj;
      obj["idx"] = idx;
      obj["query"] = t->transactions.front().query;
      obj["readset"] = t->read_set;
      obj["writeset"] = t->write_set;
      obj["columns"] = nlohmann::json();

      size_t cols_count = 0;
      size_t range_count = 0;
      for (auto &f : t->transactions.front().foreign_set)
      {
        std::vector<std::string> cols;
        for (auto &c : f.columns)
        {
          if (c == "*" && t->transactions.front().command != SQLCOM_SELECT)
          {
            is_empty_partition = true;
            continue;
          }

          cols.emplace_back(f.table + "." + c);
        }

        if (cols.size() > 0)
        {
          cols_count += cols.size();
          range_count += t->transactions.front().item_set.size();
          range_count += t->transactions.front().where_set.size();

          for (auto &c : cols)
          {
            nlohmann::json c_obj;
            c_obj["access_type"] = f.access_type == en_table_access_read ? "read" : "write";
            c_obj["name"] = c;

            obj["columns"].emplace_back(c_obj);
          }
        }
      }

      // column 정보가 있는데 partition column 을 찾지 못했다면 partition 정보 출력하지 않음
      if (cols_count > 0 && range_count == 0)
      {
        is_empty_partition = true;
      }
      else if (t->range.size() > 0)
      {
        nlohmann::json partition_obj;
        partition_obj["name"] = key_column_name;
        partition_obj["range"] = nlohmann::json();

        for (auto &r : t->range)
        {
          partition_obj["range"].push_back(r.MakeWhereQuery());
        }

        obj["partition"] = partition_obj;
      }
      else
      {
        obj["partition"] = nlohmann::json();
      }

      j.emplace_back(obj);
    }

    // 하나라도 partition 을 적용하지 못한다면 모두 초기화
    if (is_empty_partition)
    {
      for (auto &obj : j)
      {
        obj["partition"] = nlohmann::json();
      }
    }

    fout << std::setw(4) << j << std::endl;
  }

#if 0
  {
    // log output
    std::ofstream fout;
    fout.open(info_log_filepath);
    size_t idx = 0;
    for (auto &t : user_transaction_list)
    {
      ++idx;

      fout << "[IDX: " << idx << "] ";
      fout << "[Q: " << t->transactions.front().query << "] ";

      fout << "[R_SET: ";
      for (auto &s : t->read_set)
      {
        fout << s << ", ";
      }
      fout << "] ";

      fout << "[W_SET: ";
      for (auto &s : t->write_set)
      {
        fout << s << ", ";
      }
      fout << "]" << std::endl;

      fout << "[R_COLUMN: ";
      for (auto &f : t->transactions.front().foreign_set)
      {
        for (auto &c : f.columns)
        {
          fout << f.table << "." << c << ", ";
        }
      }
      fout << "]" << std::endl;
    }
  }
#endif

  // 의존도 분석 & 그래프 작성
  state_graph = new StateGraphBoost();
  state_graph->AddQueries(user_transaction_list);
  state_graph->GetQueries();

  if (my_stat(graph_filepath.c_str(), &my_file_stat, 0) &&
      remove(graph_filepath.c_str()) != 0)
  {
    error("[main] Unable to remove the graph_filepath (%s)(%s)", graph_filepath, strerror(errno));
    StateThreadPool::Instance().Release();
    StateThreadPool::Instance().ReleaseMySql();
    return EXIT_FAILURE;
  }

  debug("[main] Make graph image");
  state_graph->MakeOutputFilename("svg", graph_filepath);
  state_graph->MakeEdgeOutputFile("json", graph_filepath);
  state_graph->MakeEdgeOutputFile("txt", graph_filepath);

  StateThreadPool::Instance().Release();
  StateThreadPool::Instance().ReleaseMySql();
  return EXIT_SUCCESS;
}
