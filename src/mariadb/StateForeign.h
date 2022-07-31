#ifndef STATE_FOREIGN_INCLUDED
#define STATE_FOREIGN_INCLUDED

#include <string>
#include <vector>
#include <algorithm>

#include "state_log_hdr.h"

class StateForeign
{
public:
  StateForeign(const std::string &table);
  StateForeign(en_state_log_foreign_key_type _table_type,
               en_state_log_foreign_key_opt_type _update_type, en_state_log_foreign_key_opt_type _delete_type,
               en_state_log_table_access_type _access_type,
               const char *foreign_name, size_t foreign_name_length,
               const char *foreign_table, size_t foreign_table_length,
               const char *_columns, size_t columns_length,
               const char *_foreign_columns, size_t foreign_columns_length);

  static void MergeList(std::vector<StateForeign> &list);

  std::string name;
  std::string table;
  std::vector<std::string> columns;         //외래키 컬럼명
  std::vector<std::string> foreign_columns; //타겟 테이블의 외래키 컬럼명

  en_state_log_foreign_key_type table_type;
  en_state_log_foreign_key_opt_type update_type;
  en_state_log_foreign_key_opt_type delete_type;
  en_state_log_table_access_type access_type;
};

#endif /* STATE_FOREIGN_INCLUDED */
