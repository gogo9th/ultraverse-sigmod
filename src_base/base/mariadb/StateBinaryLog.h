#ifndef STATE_BINARY_LOG_INCLUDED
#define STATE_BINARY_LOG_INCLUDED

#include <string>
#include <vector>
#include <functional>

#include "db_state_change.h"
#include "state_log_hdr.h"

class StateBinaryLog
{
public:
  struct log_s
  {
    std::string filepath;
    state_log_time start;
    state_log_time end;
  };
  typedef struct log_s LOG;

  StateBinaryLog(const std::string &path, BINARY_FILE_FUNC_TYPE func);
  ~StateBinaryLog();

  // binary log 파일 목록 갱신
  size_t UpdateList();

  // binary log 파일 전체 요청
  std::vector<LOG> GetList();

  // start 부터 현재 까지의 binary log 파일 요청
  std::vector<LOG> GetList(const state_log_time &start);

  // start 부터 end 까지의 binary log 파일 요청
  std::vector<LOG> GetList(const state_log_time &start, const state_log_time &end);

private:
  std::vector<std::string> glob(const std::string &pattern);
  std::vector<std::string> GetAllLogFiles();
  void AddLogFile(const LOG &log);

  BINARY_FILE_FUNC_TYPE binary_read_function;
  std::string binary_log_path;
  std::vector<LOG> log_file_list;
};

#endif /* STATE_BINARY_LOG_INCLUDED */
