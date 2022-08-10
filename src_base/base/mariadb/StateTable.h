#ifndef STATE_VIEW_TABLE_INCLUDED
#define STATE_VIEW_TABLE_INCLUDED

#include <string>
#include <vector>
#include <tuple>
#include <list>
#include <memory>
#include <map>
#include <functional>

#include "mysql.h"

#include "state_log_hdr.h"

#include "StateUtil.h"
#include "StateQuery.hpp"

class StateGraph;
class StateBinaryLog;

struct StateTableInfo
{
  // include "state_log_hdr.h"
  // struct state_hash_create_table

  en_state_hash_table_from from;
  uint32_t from_sec;
  uint32_t from_sec_part;
  std::string name;
};

class StateTable
{
public:
  typedef std::shared_ptr<StateQuery> Query;
  typedef std::vector<Query> QueryList;

  enum LogType
  {
    EN_STATE_LOG_BIN = 1,
    EN_STATE_LOG_TABLE,
  };

  StateTable(Query _user_transaction);
  StateTable();
  StateTable(const StateTable &table);
  ~StateTable();

  bool SetGroupID(const int id);
  void ClearGroupID();

  void SetTime(const state_log_time &st_time, const state_log_time &ed_time);
  void SetBinaryLog(StateBinaryLog *log);
  bool UpdateWriteUserTableList(const std::vector<std::string> &write_set);

  bool ReadTableLog(const std::string &log_table_filepath);
  bool ReadGroupLog(const std::string &log_bin_filepath);
  std::tuple<bool, QueryList> ReadUserDelLog(const std::string &log_bin_filepath, std::shared_ptr<StateQuery> user_del_transaction);
  bool ReadLog(const std::string &log_bin_filepath);

  bool ValidLog();
  bool AnalyzeLog();

  static bool ApplyCandidateColumn(QueryList &query_list, const std::string &column_name);

  bool FindCandidateColumn(std::string &column);
  void ReduceLogForTest(int ratio);

  void AddQueries(StateGraph *graph);
  size_t RunQueries();
  size_t GetQueries(std::function<size_t(StateQuery *)> func);
  QueryList& GetValidQueryList();

  bool CreateUndoTables(const std::vector<std::tuple<std::string, std::string>> &table_list, std::vector<StateTableInfo> *info_list = NULL);
  static bool CreateUndoDB(MYSQL *mysql);
  bool IsWriteTable(const std::string &fullname);

  std::vector<std::string> GetUserTableList();
  static Query ReadLogBlock(const char *ptr);

  void ClearReadTableList();

  Query GetCurrTransaction();

  static void SortList(QueryList &list);
  static bool IsContinueDB(LogType type, const Query &data);
  static void MarkList(long sec, ulong sec_part, uint64_t xid, QueryList &list);
  static void MoveMarkList(QueryList &from_list, QueryList &to_list);
  static void MoveMarkListAndMakeTransaction(QueryList &from_list, QueryList &to_list);

private:
  void RemoveGroupQuery(QueryList &list);

  bool ValidTableLogLow();
  bool ValidGroupLogLow();
  bool ValidLogLow();

  static void FillFromLogBlock(const char *ptr, off_t curr, size_t data_size, std::vector<std::string> &set);

  bool ReadLogLow(LogType type, const std::string &path);
  bool ReadGroupLogLow(const std::string &path);

  bool ProcessTableLog();

  void PrepareAnalyze(QueryList &list);
  void DoneAnalyze(QueryList &list);

  static QueryList::iterator BinaryFindList(QueryList &list, const state_log_time &t);
  static QueryList::iterator BinaryFindList(QueryList::iterator begin, QueryList::iterator end, const state_log_time &t);
  static void SortQuery(Query &q);
  static void RemoveList(QueryList &list, int create_command, int drop_command);


  bool CreateUndoTablesLow(int command, MYSQL *mysql, const std::string &fullname, const state_log_time &query_time, const state_log_time &undo_time, StateTableInfo *info);
  bool UndoVersionTable(MYSQL *mysql, const std::string &fullname, const state_log_time &query_time, const state_log_time &undo_time);
  bool UndoBackupTable(MYSQL *mysql, const std::string &fullname, const state_log_time &query_time, const state_log_time &undo_time);

  bool RunCreateTable(QueryList &list);
  bool RunCreateCommand(const std::vector<std::tuple<std::string, std::string>> &table_list, QueryList &list, int command, bool is_check);

  std::vector<QueryList *> GetMatchList(const LogType &type, const uint16_t command);

  static std::string GetDbNameFromDropDbQuery(const std::string &query);
  bool IsFindRangeVector(const std::vector<std::string> &data, const std::vector<std::string> &range);

  void MakeRenameHistory();
  std::string GetBeginTableName(const std::string &fullname, const state_log_time &query_time);
  std::string GetEndTableName(const std::string &fullname, const state_log_time &query_time);
  std::vector<std::string> GetAllTableName(const std::string &fullname, const state_log_time &query_time, const state_log_time &undo_time);

  bool UpdateUserTableList(const std::vector<std::string> &read_set, const std::vector<std::string> &write_set);
  bool UpdateReadUserTableList(const std::vector<std::string> &read_set);

  void UpdateUserTableListFromTableLog();
  void UpdateUserTableListFromLog_1();
  void UpdateUserTableListFromLog_2(
      std::map<std::string, StateForeign> &read_foreign_map,
      std::map<std::string, StateForeign> &write_foreign_map,
      std::vector<StateForeign> &foreign_set);

  void MakeForeignMap();
  void MakeRelatedTableMap(const QueryList &list);

  void FindSubTable(const std::string &table, std::vector<std::string> &read_set, std::vector<std::string> &write_set);

  void FindSubRelatedTable(uint16_t command, const StateForeign curr_foreign,
                           std::vector<std::string> &read_set, std::vector<std::string> &write_set,
                           std::vector<StateForeign> &foreign_set);


  int group_id;
  bool is_user_query2;

  state_log_time start_time;
  state_log_time end_time;

  StateBinaryLog *binary_log;

  //사용자 쿼리 관련 테이블
  std::vector<std::string> user_table_list;
  std::vector<std::string> read_user_table_list;
  std::vector<std::string> write_user_table_list;

  Query user_transaction;
  Query group_transaction;

  QueryList group_list;
  QueryList procedure_list;
  QueryList view_list;
  QueryList trigger_list;
  QueryList table_list;
  QueryList log_list;

  QueryList valid_group_list;
  QueryList valid_procedure_list;
  QueryList valid_view_list;
  QueryList valid_trigger_list;
  QueryList valid_table_list;
  QueryList valid_log_list;

  QueryList before_undo_procedure_list;
  QueryList before_undo_table_list;
  QueryList before_undo_view_list;
  QueryList before_undo_trigger_list;

  std::vector<StateRange> current_range_set;

  class rename_info
  {
  public:
    rename_info(const state_log_time &time, const std::string &name)
        : time(time), name(name) {}
    state_log_time time;
    std::string name;
  };
  std::map<std::string, std::list<rename_info>> rename_history;

  class related_set
  {
  public:
    related_set(const std::vector<std::string> *r, const std::vector<std::string> *w)
    {
      if (r)
        add_read_set(*r);
      if (w)
        add_write_set(*w);
    }

    void add_read_set(const std::vector<std::string> &t)
    {
      add_set(read_set, t);
    }
    void add_read_set(const std::string &t)
    {
      add_set(read_set, t);
    }

    void add_write_set(const std::vector<std::string> &t)
    {
      add_set(write_set, t);
    }
    void add_write_set(const std::string &t)
    {
      add_set(write_set, t);
    }

    void add_reference_set(const StateForeign &foreign)
    {
      auto iter = std::find_if(reference_set.begin(), reference_set.end(), [&foreign](const StateForeign &f) {
        if (f.table == foreign.table)
        {
          auto c = StateUtil::return_sub_range_idx(f.columns, foreign.columns);
          if (c.size() == f.columns.size())
            return true;
        }
        
        return false;
      });

      if (iter == reference_set.end())
      {
        reference_set.emplace_back(foreign);
      }
      else if (foreign.name.size() > 0)
      {
        *iter = foreign;
      }
    }

    void remove_reference_set(const StateForeign &foreign)
    {
      auto iter = std::find_if(reference_set.begin(), reference_set.end(), [&foreign](const StateForeign &f) {
        if (f.name == foreign.name)
          return true;
        else
          return false;
      });

      if (iter != reference_set.end())
      {
        reference_set.erase(iter);
      }
    }

    void add_referenced_set(const StateForeign &foreign)
    {
      auto iter = std::find_if(referenced_set.begin(), referenced_set.end(), [&foreign](const StateForeign &f) {
        if (f.table == foreign.table)
        {
          auto c = StateUtil::return_sub_range_idx(f.columns, foreign.columns);
          if (c.size() == f.columns.size())
            return true;
        }
        
        return false;
      });

      if (iter == referenced_set.end())
      {
        referenced_set.emplace_back(foreign);
      }
      else if (foreign.name.size() > 0)
      {
        *iter = foreign;
      }
    }

    static void add_set(std::vector<std::string> &list, const std::vector<std::string> &t)
    {
      list.insert(list.end(), t.begin(), t.end());
      StateUtil::unique_vector(list);
    }

    static void add_set(std::vector<std::string> &list, const std::string &t)
    {
      list.push_back(t);
      StateUtil::unique_vector(list);
    }

    const std::vector<std::string> *get_read_set()
    {
      return &read_set;
    }
    const std::vector<std::string> *get_write_set()
    {
      return &write_set;
    }
    const std::vector<StateForeign> *get_reference_set()
    {
      return &reference_set;
    }
    const std::vector<StateForeign> *get_referenced_set()
    {
      return &referenced_set;
    }

  private:
    std::vector<std::string> read_set;
    std::vector<std::string> write_set;

    std::vector<StateForeign> reference_set;
    std::vector<StateForeign> referenced_set;
  };
  std::map<std::string, related_set> related_table_map;
};

#endif /* STATE_VIEW_TABLE_INCLUDED */
