#ifndef STATE_UTIL_INCLUDED
#define STATE_UTIL_INCLUDED

#include <memory>
#include <string>
#include <vector>
#include <algorithm>
#include <iomanip>

#include "state_log_hdr.h"

template <typename C>
struct reverse_wrapper {

    C & c_;
    reverse_wrapper(C & c) :  c_(c) {}

    typename C::reverse_iterator begin() {return c_.rbegin();}
    typename C::reverse_iterator end() {return c_.rend(); }
};

template <typename C, size_t N>
struct reverse_wrapper< C[N] >{

    C (&c_)[N];
    reverse_wrapper( C(&c)[N] ) : c_(c) {}

    typename std::reverse_iterator<const C *> begin() { return std::rbegin(c_); }
    typename std::reverse_iterator<const C *> end() { return std::rend(c_); }
};


template <typename C>
reverse_wrapper<C> r_wrap(C & c) {
    return reverse_wrapper<C>(c);
}

class StateUtil
{
public:
  inline static std::string &ltrim(std::string &s)
  {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) {
              return !std::isspace(ch);
            }));
    return s;
  }

  inline static std::string &rtrim(std::string &s)
  {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) {
              return !std::isspace(ch);
            })
                .base(),
            s.end());
    return s;
  }

  inline static std::string &trim(std::string &s)
  {
    return ltrim(rtrim(s));
  }

  template <typename T>
  inline static void unique_vector(std::vector<T> &vec)
  {
    std::sort(vec.begin(), vec.end());
    vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
  }

  inline static bool iequals(const std::string &str1, const std::string &str2)
  {
    return str1.size() == str2.size() &&
           std::equal(str1.begin(), str1.end(), str2.begin(),
                      [](char c1, char c2) { return (c1 == c2 || std::toupper(c1) == std::toupper(c2)); });
  }

  inline static std::vector<std::string> split(const std::string &sep, const std::string &str)
  {
    std::vector<std::string> vec;

    std::string::size_type s_pos = 0;
    std::string::size_type e_pos = std::string::npos;
    while ((e_pos = str.find_first_of(sep, s_pos)) != std::string::npos)
    {
      if (s_pos != e_pos)
        vec.push_back(str.substr(s_pos, e_pos - s_pos));

      s_pos = e_pos + 1;
    }

    if (s_pos != str.size())
      vec.push_back(str.substr(s_pos));

    return vec;
  }

  inline static std::vector<std::string> space_split(const std::string &str)
  {
    return split(" \t\n\v\f\r", str);
  }

  inline static std::string format_time(time_t t, const std::string &format)
  {
    std::tm timeinfo;
    if (localtime_r(&t, &timeinfo) == NULL)
      return std::string();

    std::ostringstream os;
    os << std::put_time(&timeinfo, format.c_str());
    return os.str();
  }

  inline static time_t format_time(const std::string &t, const std::string &format)
  {
    std::tm timeinfo;
    std::istringstream is(t);
    is >> std::get_time(&timeinfo, format.c_str());

    return std::mktime(&timeinfo);
  }

  template <typename T>
  struct find_range
  {
    const std::vector<T> *range;
    find_range(const std::vector<T> *r) : range(r) {}
    bool operator()(const T &s) const
    {
      return std::find(range->begin(), range->end(), s) != range->end();
    }
  };

  template <typename T>
  inline static bool find_sub_range(const std::vector<T> &range1, const std::vector<T> &range2)
  {
    return range1.end() != std::find_if(range1.begin(), range1.end(), StateUtil::find_range<T>(&range2));
  }

  template <typename T>
  inline static std::vector<T> return_sub_range(const std::vector<T> &range1, const std::vector<T> &range2)
  {
    std::vector<T> ret;
    for (auto &i : range1)
    {
      if (std::find(range2.begin(), range2.end(), i) != range2.end())
      {
        ret.push_back(i);
      }
    }

    return ret;
  }

  template <typename T>
  inline static std::vector<size_t> return_sub_range_idx(const std::vector<T> &my_range, const std::vector<T> &target_range)
  {
    std::vector<size_t> ret;
    for (auto iter = my_range.begin(); iter != my_range.end(); ++iter)
    {
      auto sub_iter = std::find(target_range.begin(), target_range.end(), *iter);
      if (sub_iter != target_range.end())
      {
        ret.push_back(std::distance(target_range.begin(), sub_iter));
      }
    }

    return ret;
  }

  static const char *GetCommandName(int command)
  {
    struct st_command_name
    {
      const char *name;
      int command;
    };

    static const struct st_command_name comm_name_list[] = {
        {"ALTER_DB", SQLCOM_ALTER_DB},
        {"ALTER_DB_UPGRADE", SQLCOM_ALTER_DB_UPGRADE},
        {"ALTER_EVENT", SQLCOM_ALTER_EVENT},
        {"ALTER_FUNCTION", SQLCOM_ALTER_FUNCTION},
        {"ALTER_PROCEDURE", SQLCOM_ALTER_PROCEDURE},
        {"ALTER_SERVER", SQLCOM_ALTER_SERVER},
        {"ALTER_SEQUENCE", SQLCOM_ALTER_SEQUENCE},
        {"ALTER_TABLE", SQLCOM_ALTER_TABLE},
        {"ALTER_TABLESPACE", SQLCOM_ALTER_TABLESPACE},
        {"ALTER_USER", SQLCOM_ALTER_USER},
        {"ANALYZE", SQLCOM_ANALYZE},
        {"ASSIGN_TO_KEYCACHE", SQLCOM_ASSIGN_TO_KEYCACHE},
        {"BEGIN", SQLCOM_BEGIN},
        {"BINLOG", SQLCOM_BINLOG_BASE64_EVENT},
        {"CALL_PROCEDURE", SQLCOM_CALL},
        {"CHANGE_DB", SQLCOM_CHANGE_DB},
        {"CHANGE_MASTER", SQLCOM_CHANGE_MASTER},
        {"CHECK", SQLCOM_CHECK},
        {"CHECKSUM", SQLCOM_CHECKSUM},
        {"COMMIT", SQLCOM_COMMIT},
        {"COMPOUND_SQL", SQLCOM_COMPOUND},
        {"CREATE_DB", SQLCOM_CREATE_DB},
        {"CREATE_EVENT", SQLCOM_CREATE_EVENT},
        {"CREATE_FUNCTION", SQLCOM_CREATE_SPFUNCTION},
        {"CREATE_INDEX", SQLCOM_CREATE_INDEX},
        {"CREATE_PACKAGE", SQLCOM_CREATE_PACKAGE},
        {"CREATE_PACKAGE_BODY", SQLCOM_CREATE_PACKAGE_BODY},
        {"CREATE_PROCEDURE", SQLCOM_CREATE_PROCEDURE},
        {"CREATE_ROLE", SQLCOM_CREATE_ROLE},
        {"CREATE_SEQUENCE", SQLCOM_CREATE_SEQUENCE},
        {"CREATE_SERVER", SQLCOM_CREATE_SERVER},
        {"CREATE_TABLE", SQLCOM_CREATE_TABLE},
        {"CREATE_TRIGGER", SQLCOM_CREATE_TRIGGER},
        {"CREATE_UDF", SQLCOM_CREATE_FUNCTION},
        {"CREATE_USER", SQLCOM_CREATE_USER},
        {"CREATE_VIEW", SQLCOM_CREATE_VIEW},
        {"DEALLOC_SQL", SQLCOM_DEALLOCATE_PREPARE},
        {"DELETE", SQLCOM_DELETE},
        {"DELETE_MULTI", SQLCOM_DELETE_MULTI},
        {"DO", SQLCOM_DO},
        {"DROP_DB", SQLCOM_DROP_DB},
        {"DROP_EVENT", SQLCOM_DROP_EVENT},
        {"DROP_FUNCTION", SQLCOM_DROP_FUNCTION},
        {"DROP_INDEX", SQLCOM_DROP_INDEX},
        {"DROP_PROCEDURE", SQLCOM_DROP_PROCEDURE},
        {"DROP_PACKAGE", SQLCOM_DROP_PACKAGE},
        {"DROP_PACKAGE_BODY", SQLCOM_DROP_PACKAGE_BODY},
        {"DROP_ROLE", SQLCOM_DROP_ROLE},
        {"DROP_SERVER", SQLCOM_DROP_SERVER},
        {"DROP_SEQUENCE", SQLCOM_DROP_SEQUENCE},
        {"DROP_TABLE", SQLCOM_DROP_TABLE},
        {"DROP_TRIGGER", SQLCOM_DROP_TRIGGER},
        {"DROP_USER", SQLCOM_DROP_USER},
        {"DROP_VIEW", SQLCOM_DROP_VIEW},
        {"EMPTY_QUERY", SQLCOM_EMPTY_QUERY},
        {"EXECUTE_IMMEDIATE", SQLCOM_EXECUTE_IMMEDIATE},
        {"EXECUTE_SQL", SQLCOM_EXECUTE},
        {"FLUSH", SQLCOM_FLUSH},
        {"GET_DIAGNOSTICS", SQLCOM_GET_DIAGNOSTICS},
        {"GRANT", SQLCOM_GRANT},
        {"GRANT_ROLE", SQLCOM_GRANT_ROLE},
        {"HA_CLOSE", SQLCOM_HA_CLOSE},
        {"HA_OPEN", SQLCOM_HA_OPEN},
        {"HA_READ", SQLCOM_HA_READ},
        {"HELP", SQLCOM_HELP},
        {"INSERT", SQLCOM_INSERT},
        {"INSERT_SELECT", SQLCOM_INSERT_SELECT},
        {"INSTALL_PLUGIN", SQLCOM_INSTALL_PLUGIN},
        {"KILL", SQLCOM_KILL},
        {"LOAD", SQLCOM_LOAD},
        {"LOCK_TABLES", SQLCOM_LOCK_TABLES},
        {"OPTIMIZE", SQLCOM_OPTIMIZE},
        {"PRELOAD_KEYS", SQLCOM_PRELOAD_KEYS},
        {"PREPARE_SQL", SQLCOM_PREPARE},
        {"PURGE", SQLCOM_PURGE},
        {"PURGE_BEFORE_DATE", SQLCOM_PURGE_BEFORE},
        {"RELEASE_SAVEPOINT", SQLCOM_RELEASE_SAVEPOINT},
        {"RENAME_TABLE", SQLCOM_RENAME_TABLE},
        {"RENAME_USER", SQLCOM_RENAME_USER},
        {"REPAIR", SQLCOM_REPAIR},
        {"REPLACE", SQLCOM_REPLACE},
        {"REPLACE_SELECT", SQLCOM_REPLACE_SELECT},
        {"RESET", SQLCOM_RESET},
        {"RESIGNAL", SQLCOM_RESIGNAL},
        {"REVOKE", SQLCOM_REVOKE},
        {"REVOKE_ALL", SQLCOM_REVOKE_ALL},
        {"REVOKE_ROLE", SQLCOM_REVOKE_ROLE},
        {"ROLLBACK", SQLCOM_ROLLBACK},
        {"ROLLBACK_TO_SAVEPOINT", SQLCOM_ROLLBACK_TO_SAVEPOINT},
        {"SAVEPOINT", SQLCOM_SAVEPOINT},
        {"SELECT", SQLCOM_SELECT},
        {"SET_OPTION", SQLCOM_SET_OPTION},
        {"SHOW_AUTHORS", SQLCOM_SHOW_AUTHORS},
        {"SHOW_BINLOG_EVENTS", SQLCOM_SHOW_BINLOG_EVENTS},
        {"SHOW_BINLOGS", SQLCOM_SHOW_BINLOGS},
        {"SHOW_CHARSETS", SQLCOM_SHOW_CHARSETS},
        {"SHOW_COLLATIONS", SQLCOM_SHOW_COLLATIONS},
        {"SHOW_CONTRIBUTORS", SQLCOM_SHOW_CONTRIBUTORS},
        {"SHOW_CREATE_DB", SQLCOM_SHOW_CREATE_DB},
        {"SHOW_CREATE_EVENT", SQLCOM_SHOW_CREATE_EVENT},
        {"SHOW_CREATE_FUNC", SQLCOM_SHOW_CREATE_FUNC},
        {"SHOW_CREATE_PACKAGE", SQLCOM_SHOW_CREATE_PACKAGE},
        {"SHOW_CREATE_PACKAGE_BODY", SQLCOM_SHOW_CREATE_PACKAGE_BODY},
        {"SHOW_CREATE_PROC", SQLCOM_SHOW_CREATE_PROC},
        {"SHOW_CREATE_TABLE", SQLCOM_SHOW_CREATE},
        {"SHOW_CREATE_TRIGGER", SQLCOM_SHOW_CREATE_TRIGGER},
        {"SHOW_CREATE_USER", SQLCOM_SHOW_CREATE_USER},
        {"SHOW_DATABASES", SQLCOM_SHOW_DATABASES},
        {"SHOW_ENGINE_LOGS", SQLCOM_SHOW_ENGINE_LOGS},
        {"SHOW_ENGINE_MUTEX", SQLCOM_SHOW_ENGINE_MUTEX},
        {"SHOW_ENGINE_STATUS", SQLCOM_SHOW_ENGINE_STATUS},
        {"SHOW_ERRORS", SQLCOM_SHOW_ERRORS},
        {"SHOW_EVENTS", SQLCOM_SHOW_EVENTS},
        {"SHOW_EXPLAIN", SQLCOM_SHOW_EXPLAIN},
        {"SHOW_FIELDS", SQLCOM_SHOW_FIELDS},
        {"SHOW_FUNCTION_CODE", SQLCOM_SHOW_FUNC_CODE},
        {"SHOW_FUNCTION_STATUS", SQLCOM_SHOW_STATUS_FUNC},
        {"SHOW_GENERIC", SQLCOM_SHOW_GENERIC},
        {"SHOW_GRANTS", SQLCOM_SHOW_GRANTS},
        {"SHOW_KEYS", SQLCOM_SHOW_KEYS},
        {"SHOW_MASTER_STATUS", SQLCOM_SHOW_MASTER_STAT},
        {"SHOW_OPEN_TABLES", SQLCOM_SHOW_OPEN_TABLES},
        {"SHOW_PACKAGE_STATUS", SQLCOM_SHOW_STATUS_PACKAGE},
        {"SHOW_PACKAGE_BODY_CODE", SQLCOM_SHOW_PACKAGE_BODY_CODE},
        {"SHOW_PACKAGE_BODY_STATUS", SQLCOM_SHOW_STATUS_PACKAGE_BODY},
        {"SHOW_PLUGINS", SQLCOM_SHOW_PLUGINS},
        {"SHOW_PRIVILEGES", SQLCOM_SHOW_PRIVILEGES},
        {"SHOW_PROCEDURE_CODE", SQLCOM_SHOW_PROC_CODE},
        {"SHOW_PROCEDURE_STATUS", SQLCOM_SHOW_STATUS_PROC},
        {"SHOW_PROCESSLIST", SQLCOM_SHOW_PROCESSLIST},
        {"SHOW_PROFILE", SQLCOM_SHOW_PROFILE},
        {"SHOW_PROFILES", SQLCOM_SHOW_PROFILES},
        {"SHOW_RELAYLOG_EVENTS", SQLCOM_SHOW_RELAYLOG_EVENTS},
        {"SHOW_SLAVE_HOSTS", SQLCOM_SHOW_SLAVE_HOSTS},
        {"SHOW_SLAVE_STATUS", SQLCOM_SHOW_SLAVE_STAT},
        {"SHOW_STATUS", SQLCOM_SHOW_STATUS},
        {"SHOW_STORAGE_ENGINES", SQLCOM_SHOW_STORAGE_ENGINES},
        {"SHOW_TABLE_STATUS", SQLCOM_SHOW_TABLE_STATUS},
        {"SHOW_TABLES", SQLCOM_SHOW_TABLES},
        {"SHOW_TRIGGERS", SQLCOM_SHOW_TRIGGERS},
        {"SHOW_VARIABLES", SQLCOM_SHOW_VARIABLES},
        {"SHOW_WARNINGS", SQLCOM_SHOW_WARNS},
        {"SHUTDOWN", SQLCOM_SHUTDOWN},
        {"SIGNAL", SQLCOM_SIGNAL},
        {"START_ALL_SLAVES", SQLCOM_SLAVE_ALL_START},
        {"START_SLAVE", SQLCOM_SLAVE_START},
        {"STOP_ALL_SLAVES", SQLCOM_SLAVE_ALL_STOP},
        {"STOP_SLAVE", SQLCOM_SLAVE_STOP},
        {"TRUNCATE", SQLCOM_TRUNCATE},
        {"UNINSTALL_PLUGIN", SQLCOM_UNINSTALL_PLUGIN},
        {"UNLOCK_TABLES", SQLCOM_UNLOCK_TABLES},
        {"UPDATE", SQLCOM_UPDATE},
        {"UPDATE_MULTI", SQLCOM_UPDATE_MULTI},
        {"XA_COMMIT", SQLCOM_XA_COMMIT},
        {"XA_END", SQLCOM_XA_END},
        {"XA_PREPARE", SQLCOM_XA_PREPARE},
        {"XA_RECOVER", SQLCOM_XA_RECOVER},
        {"XA_ROLLBACK", SQLCOM_XA_ROLLBACK},
        {"XA_START", SQLCOM_XA_START},
    };

    static const size_t size = sizeof(comm_name_list) / sizeof(*comm_name_list);
    for (size_t i = 0; i < size; ++i)
    {
      if (comm_name_list[i].command == command)
        return comm_name_list[i].name;
    }

    static char unk_name[16] = {
        0,
    };
    snprintf(unk_name, sizeof(unk_name) - 1, "UNKNOWN %d", command);
    return unk_name;
  }
};

#endif /* STATE_UTIL_INCLUDED */
