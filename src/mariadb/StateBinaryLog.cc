#include "StateBinaryLog.h"

#include <glob.h>
#include <string.h>
#include <limits.h>
#include <assert.h>

#include <algorithm>

#define BIN_LOG_HEADER_SIZE 4
#define PROBE_HEADER_LEN (EVENT_LEN_OFFSET + 4)

StateBinaryLog::StateBinaryLog(const std::string &path, BINARY_FILE_FUNC_TYPE func)
    : binary_read_function(func), binary_log_path(path)
{
  UpdateList();
}

StateBinaryLog::~StateBinaryLog()
{
}

std::vector<std::string> StateBinaryLog::glob(const std::string &pattern)
{
  std::vector<std::string> filenames;
  char resolved_path[PATH_MAX];

  // glob struct resides on the stack
  glob_t glob_result;
  memset(&glob_result, 0, sizeof(glob_result));

  // do the glob operation
  int return_value = ::glob(pattern.c_str(), GLOB_TILDE, NULL, &glob_result);
  if (return_value != 0)
  {
    globfree(&glob_result);
    return filenames;
  }

  // collect all the filenames into a std::vector<std::string>
  for (size_t i = 0; i < glob_result.gl_pathc; ++i)
  {
    memset(resolved_path, 0, sizeof(resolved_path));
    if (NULL == realpath(glob_result.gl_pathv[i], resolved_path))
    {
      continue;
    }
    filenames.push_back(resolved_path);
  }

  // cleanup
  globfree(&glob_result);

  // done
  return filenames;
}

std::vector<std::string> StateBinaryLog::GetAllLogFiles()
{
  // binary_log_filepath -> /opt/mysql/log/mariadb-bin
  auto filenames = glob(std::string(binary_log_path) + "*");
  if (filenames.size() == 0)
    return filenames;

  struct find
  {
    std::string target;
    find(const std::string &s) : target(s) {}
    bool operator()(const std::string &s) const
    {
      return s.find(target) == s.size() - target.size();
    }
  };

  filenames.erase(std::find_if(filenames.begin(), filenames.end(), find(".index")));

  return filenames;
}

void StateBinaryLog::AddLogFile(const LOG &log)
{
  struct find
  {
    std::string target;
    find(const std::string &s) : target(s) {}
    bool operator()(const LOG &s) const
    {
      return s.filepath == target;
    }
  };

  auto iter = std::find_if(log_file_list.begin(), log_file_list.end(), find(log.filepath));
  if (iter == log_file_list.end())
  {
    log_file_list.emplace_back(log);
  }
  else
  {
    (*iter) = log;
  }
}

size_t StateBinaryLog::UpdateList()
{
  auto filenames = GetAllLogFiles();

  auto process = [](long sec, ulong sec_part, state_log_time *start, state_log_time *end, uint64_t xid __attribute__((unused))) -> bool {
    state_log_time curr({sec, sec_part});
    if (start->sec == 0)
    {
      *start = curr;
      *end = curr;
      return true;
    }

    if (curr < *start)
      *start = curr;
    else if (curr > *end)
      *end = curr;

    return true;
  };

  assert(filenames.size() >= log_file_list.size());

  size_t idx = log_file_list.size();
  if (idx > 0)
    idx -= 1;

  for (; idx < filenames.size(); ++idx)
  {
    LOG log;
    log.filepath = filenames[idx];
    log.start = {0, 0};
    log.end = {0, 0};

    if (true == binary_read_function(
                     log.filepath,
                     std::bind(process,
                               std::placeholders::_1,
                               std::placeholders::_2,
                               &log.start,
                               &log.end,
                               std::placeholders::_3)))
    {
      AddLogFile(log);
    }
  }

  return filenames.size();
}

std::vector<StateBinaryLog::LOG> StateBinaryLog::GetList()
{
  return log_file_list;
}

std::vector<StateBinaryLog::LOG> StateBinaryLog::GetList(const state_log_time &start)
{
  std::vector<StateBinaryLog::LOG> list;

  for (auto &i : log_file_list)
  {
    if (start > i.end)
      continue;

    list.emplace_back(i);
  }

  return list;
}

std::vector<StateBinaryLog::LOG> StateBinaryLog::GetList(const state_log_time &start, const state_log_time &end)
{
  std::vector<StateBinaryLog::LOG> list;

  for (auto &i : log_file_list)
  {
    if (start > i.end)
      continue;
    if (end < i.start)
      continue;

    list.emplace_back(i);
  }

  return list;
}
