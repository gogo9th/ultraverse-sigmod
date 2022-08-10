#ifndef STATE_HASH_TABLE_INFO_INCLUDED
#define STATE_HASH_TABLE_INFO_INCLUDED

#include <map>
#include <string>
#include <memory>

#include "StateQuery.h"

class StateHashTableInfo
{
public:
  StateHashTableInfo() : is_false(false)
  {
  }

  bool IsMatch(const std::string &table_name)
  {
    auto iter = table_map.find(table_name);
    if (iter == table_map.end())
      return false;

    return iter->second.is_match;
  }

  void FindMatch(const std::string &table_name)
  {
    auto iter = table_map.find(table_name);
    if (iter == table_map.end())
      return;

    iter->second.is_match = true;
  }

  void AddTable(const std::string &table_name, std::vector<std::shared_ptr<StateQuery>>::iterator end)
  {
    if (table_map.find(table_name) == table_map.end())
    {
      table_map.emplace(std::make_pair(table_name, end));
    }
    last_query_iter_end = end;
  }

  void CheckIter(std::vector<std::shared_ptr<StateQuery>>::iterator query_iter)
  {
    (*query_iter)->is_hash_match = (bool*)&is_false;

    if ((*query_iter)->read_set.size() == 0)
    {
      for (auto &t : (*query_iter)->write_set)
      {
        auto iter = table_map.find(t);
        if (iter == table_map.end())
          continue;

        if (iter->second.last_query_iter == last_query_iter_end)
          iter->second.last_query_iter = query_iter;
      }
    }
    else
    {
      for (auto &t : (*query_iter)->write_set)
      {
        auto iter = table_map.find(t);
        if (iter == table_map.end())
          continue;

        iter->second.last_query_iter = last_query_iter_end;
      }
    }
  }

  void SetMatchPointer()
  {
    for (auto &i : table_map)
    {
      for (auto iter = i.second.last_query_iter; iter != last_query_iter_end; ++iter)
      {
        (*iter)->is_hash_match = &i.second.is_match;
      }
    }
  }

  class HashTable
  {
  public:
    HashTable(std::vector<std::shared_ptr<StateQuery>>::iterator end)
        : is_match(false)
    {
      last_query_iter = end;
    }

    bool is_match;
    std::vector<std::shared_ptr<StateQuery>>::iterator last_query_iter;
  };

  const bool is_false;
  std::map<std::string, HashTable> table_map;
  std::vector<std::shared_ptr<StateQuery>>::iterator last_query_iter_end;
};

#endif /* STATE_HASH_TABLE_INFO_INCLUDED */
