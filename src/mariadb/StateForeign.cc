#include <assert.h>

#include <map>

#include "StateForeign.h"
#include "StateUtil.h"
#include "StateUserQuery.h"

StateForeign::StateForeign(const std::string &table)
    : table(table),
      table_type(en_foreign_key_none),
      update_type(en_foreign_key_opt_none),
      delete_type(en_foreign_key_opt_none),
      access_type(en_table_access_read)
{
}

StateForeign::StateForeign(en_state_log_foreign_key_type _table_type,
                           en_state_log_foreign_key_opt_type _update_type, en_state_log_foreign_key_opt_type _delete_type,
                           en_state_log_table_access_type _access_type,
                           const char *foreign_name, size_t foreign_name_length,
                           const char *foreign_table, size_t foreign_table_length,
                           const char *_columns, size_t columns_length,
                           const char *_foreign_columns, size_t foreign_columns_length)
    : table_type(_table_type),
      update_type(_update_type),
      delete_type(_delete_type),
      access_type(_access_type)
{
  name.assign(foreign_name, foreign_name + foreign_name_length);
  table.assign(foreign_table, foreign_table + foreign_table_length);

  auto st_ptr = _columns;
  auto ptr = _columns;
  while (ptr != _columns + columns_length)
  {
    if (*ptr == ',')
    {
      columns.emplace_back(st_ptr, ptr);
      st_ptr = ptr + 1;
    }
    ++ptr;
  }
  if (st_ptr != _columns + columns_length)
    columns.emplace_back(st_ptr, _columns + columns_length);

  st_ptr = _foreign_columns;
  ptr = _foreign_columns;
  while (ptr != _foreign_columns + foreign_columns_length)
  {
    if (*ptr == ',')
    {
      foreign_columns.emplace_back(st_ptr, ptr);
      st_ptr = ptr + 1;
    }
    ++ptr;
  }
  if (st_ptr != _foreign_columns + foreign_columns_length)
    foreign_columns.emplace_back(st_ptr, _foreign_columns + foreign_columns_length);

  StateUtil::unique_vector(columns);
  StateUtil::unique_vector(foreign_columns);
}

thread_local static std::vector<StateForeign> new_list;

struct ST_TMP
{
  ST_TMP(std::vector<std::string> *read_col, std::vector<std::string> *write_col)
      : read_col(read_col), write_col(write_col)
  {
  }
  std::vector<std::string> *read_col;
  std::vector<std::string> *write_col;
};
thread_local static std::map<std::string, ST_TMP> new_table_map;

void StateForeign::MergeList(std::vector<StateForeign> &list)
{
  // 중복된 정보가 있으면 삭제하고, 합칠수 있으면 합침

  // static thread_local std::map<std::string, StateForeign> read_foreign_map;
  // static thread_local std::map<std::string, StateForeign> write_foreign_map;

  auto add_foreign_item = [](std::vector<StateForeign> &target_list, const StateForeign &my) {
    for (auto &i : target_list)
    {
      // 외래키 정보는 테이블내 외래키 이름이 동일하면 merge 가능
      if (i.table == my.table && i.name == my.name)
      {
        assert(i.table_type == my.table_type);

        i.columns.insert(i.columns.end(), my.columns.begin(), my.columns.end());
        i.foreign_columns.insert(i.foreign_columns.end(), my.foreign_columns.begin(), my.foreign_columns.end());
        return true;
      }
    }

    return false;
  };

  auto add_column_item = [](std::vector<StateForeign> &target_list, const StateForeign &my) {
    for (auto &i : target_list)
    {
      // 컬럼 정보는 테이블내 read/write 타입이 동일하면 merge 가능
      if (i.table == my.table && i.access_type == my.access_type)
      {
        i.columns.insert(i.columns.end(), my.columns.begin(), my.columns.end());
        i.foreign_columns.insert(i.foreign_columns.end(), my.foreign_columns.begin(), my.foreign_columns.end());
        return true;
      }
    }

    return false;
  };

  new_list.clear();

  for (auto &i : list)
  {
    if (i.name.size() > 0)
    {
      //외래키 정보
      if (add_foreign_item(new_list, i) == false)
      {
        new_list.emplace_back(i);
      }
    }
    else
    {
      //연관 컬럼 정보
      if (add_column_item(new_list, i) == false)
      {
        new_list.emplace_back(i);
      }
    }
  }

  new_table_map.clear();

  for (auto &i : new_list)
  {
    StateUtil::unique_vector(i.columns);
    StateUtil::unique_vector(i.foreign_columns);

    auto iter = std::find(i.columns.begin(), i.columns.end(), "row_start");
    if (iter != i.columns.end())
    {
      i.columns.erase(iter);
    }

    //연관 컬럼 정보
    if (i.name.size() == 0)
    {
      if (i.access_type == en_table_access_read)
      {
        auto iter = new_table_map.emplace(i.table, ST_TMP(&i.columns, NULL));
        if (iter.second == false)
        {
          iter.first->second.read_col = &i.columns;
        }
      }
      else
      {
        auto iter = new_table_map.emplace(i.table, ST_TMP(NULL, &i.columns));
        if (iter.second == false)
        {
          iter.first->second.write_col = &i.columns;
        }
      }
    }
  }

  for (auto &t : new_table_map)
  {
    if (t.second.read_col != NULL && t.second.write_col != NULL)
    {
      StateUserQuery::MergeTableSet(*t.second.read_col, *t.second.write_col);
    }
  }

  for (auto iter = new_list.begin(); iter != new_list.end(); )
  {
    if (iter->name.size() == 0 && iter->columns.size() == 0)
    {
      iter = new_list.erase(iter);
    }
    else
    {
      ++iter;
    }
  }

  list = new_list;
}
