#ifndef DB_STATE_CHANGE_INCLUDED
#define DB_STATE_CHANGE_INCLUDED

#include <functional>
#include "mysql.h"
#include "state_log_hdr.h"

#define PROGRAM_NAME "DB_STATE_CHANGE"

typedef std::function<void(long, ulong, uint64_t)> BINARY_FUNC_TYPE;
typedef std::function<bool(const std::string &, BINARY_FUNC_TYPE)> BINARY_FILE_FUNC_TYPE;

void error(const char *format, ...);
void warning(const char *format, ...);
void debug(const char *format, ...);
void convert_str_to_timestamp(const char* const str, state_log_time *time);
bool process_binary_log(const std::string &filename, BINARY_FUNC_TYPE process);
state_log_time &get_start_time();
state_log_time &get_end_time();
state_log_time &get_hash_query_time();
bool is_hash_matched();
bool is_using_candidate();
const char *get_key_column_name();
const char *get_key_column_table_name();
en_state_log_column_data_type get_key_column_type();

MYSQL *open_mysql();
void close_mysql(MYSQL *mysql);

#endif /* DB_STATE_CHANGE_INCLUDED */
