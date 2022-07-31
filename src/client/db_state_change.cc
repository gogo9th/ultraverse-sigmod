
#define MYSQL_CLIENT
#undef MYSQL_SERVER
#define TABLE TABLE_CLIENT
/* This hack is here to avoid adding COMPRESSED data types to libmariadb. */
#define MYSQL_TYPE_TIME2 MYSQL_TYPE_TIME2, MYSQL_TYPE_BLOB_COMPRESSED = 140, MYSQL_TYPE_VARCHAR_COMPRESSED = 141
#include "client_priv.h"
#undef MYSQL_TYPE_TIME2
#include <my_time.h>
#include "sql_priv.h"
#include "sql_basic_types.h"
#include "log_event.h"
#include "compat56.h"
#include "sql_common.h"
#include "my_dir.h"
#include <welcome_copyright_notice.h> // ORACLE_WELCOME_COPYRIGHT_NOTICE

#include "StateTable.h"
#include "StateUserQuery.h"
#include "StateGraphBoost.h"
#include "StateGraphGvc.h"
#include "StateThreadPool.h"
#include "StateBinaryLog.h"
#include "StateUtil.h"
#include "state_log_hdr.h"
#include "db_state_change.h"
#include "mysqld.h"

#include <sys/ipc.h>
#include <sys/msg.h>
#include <netinet/in.h>
#include <vector>
#include <string>
#include <algorithm>

#define BIN_LOG_HEADER_SIZE 4
#define PROBE_HEADER_LEN (EVENT_LEN_OFFSET + 4)

static char *start_datetime_str;
static char *redo_datetime_str;
static char *query_filepath;
static char *query_filepath2;
static char *binary_log_filepath;
static char *state_log_table_filepath;
static char *state_log_filepath;
static char *graph_filepath;
static char *redo_db;
static char *candidate_db;

static char *key_column_table_name = NULL;
static char *key_column_name = NULL;
static char *key_column_type_str = NULL;
en_state_log_column_data_type key_column_type = en_column_data_null;

static bool opt_version = false;
static const char *load_groups[] =
    {"client", "client-server", "client-mariadb",
     "mysqld",
     0};

static state_log_time start_time = {0, 0};
static state_log_time end_time = {0, 0};
static state_log_time redo_time = {0, 0};
static state_log_time hash_query_time = {0, 0};
static bool b_hash_matched = false;
static char *host = NULL;
static char *user = NULL;
static char *pass = NULL;
static int port = 0;
static char *sock = NULL;
static char *s_gid = NULL;
static int i_gid = 0;

StateThreadPool *StateThreadPool::instance_ = NULL;
const size_t StateThreadPool::MAX_MYSQL_CONN_COUNT;
StateBinaryLog *state_binary_log = NULL;
StateUserQuery *state_user_query = NULL;
StateGraph *state_graph = NULL;
StateTable *state_table = NULL;
StateTable *sync_state_table = NULL;

std::vector<std::string> get_table_list(MYSQL *mysql, const std::string &db);
std::vector<std::tuple<std::string, std::string>> get_all_table_list();


std::chrono::time_point<std::chrono::high_resolution_clock>
    start_time_point,
    end_load_log_time_point,
    end_query_analyze_time_point,
    end_undo_time_point,
    end_redo_time_point,
    end_sync_table_time_point,
    end_swap_table_time_point,
    end_time_point;

/**
  Pointer to the Format_description_log_event of the currently active binlog.

  This will be changed each time a new Format_description_log_event is
  found in the binlog. It is finally destroyed at program termination.
*/
static Format_description_log_event *glob_description_event = NULL;

/**
  This block is used in log_event.cc
 */
static my_bool force_opt = false;
static my_bool short_form = false;
ulong opt_binlog_rows_event_max_size = 0;
ulong opt_binlog_rows_event_max_encoded_size = MAX_MAX_ALLOWED_PACKET;
char server_version[SERVER_VERSION_LENGTH];

CHARSET_INFO *system_charset_info = &my_charset_utf8_general_ci;
extern "C" unsigned char *mysql_net_store_length(unsigned char *packet, size_t length);
#define net_store_length mysql_net_store_length

uint e_key_get_latest_version_func(uint)
{
  return 1;
}
uint e_key_get_func(uint, uint, uchar *, uint *) { return 1; }
uint e_ctx_size_func(uint, uint) { return 1; }
int e_ctx_init_func(void *, const uchar *, uint, const uchar *, uint,
                    int, uint, uint) { return 1; }
int e_ctx_update_func(void *, const uchar *, uint, uchar *, uint *) { return 1; }
int e_ctx_finish_func(void *, uchar *, uint *) { return 1; }
uint e_encrypted_length_func(uint, uint, uint) { return 1; }

struct encryption_service_st encryption_handler =
    {
        e_key_get_latest_version_func,
        e_key_get_func,
        e_ctx_size_func,
        e_ctx_init_func,
        e_ctx_update_func,
        e_ctx_finish_func,
        e_encrypted_length_func};

static struct my_option my_options[] =
    {
        {"help", '?', "Display this help and exit.",
         0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},
        {"version", 'V', "Print version and exit.", 0, 0, 0, GET_NO_ARG, NO_ARG, 0,
         0, 0, 0, 0, 0},

        {"start-datetime", OPT_START_DATETIME,
         "Start reading the binlog at first event having a datetime equal or "
         "posterior to the argument; the argument must be a date and time "
         "in the local time zone, in any format accepted by the MySQL server "
         "for DATETIME and TIMESTAMP types, for example: 2004-12-25 11:25:56 "
         "(you should probably use quotes for your shell to set it properly).",
         &start_datetime_str, &start_datetime_str,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"query", OPT_MAX_CLIENT_OPTION,
         "query file path",
         &query_filepath, &query_filepath,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"graph", OPT_MAX_CLIENT_OPTION,
         "svg graph file path",
         &graph_filepath, &graph_filepath,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

        {"query2", OPT_MAX_CLIENT_OPTION,
         "query file path for add/delete query",
         &query_filepath2, &query_filepath2,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

        {"log_bin", OPT_MAX_CLIENT_OPTION,
         "binary log file path",
         &binary_log_filepath, &binary_log_filepath,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"state_log_table_filename", OPT_MAX_CLIENT_OPTION,
         "table state log file path",
         &state_log_table_filepath, &state_log_table_filepath,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"state_log_filename", OPT_MAX_CLIENT_OPTION,
         "state log file path",
         &state_log_filepath, &state_log_filepath,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

        {"gid", 'g', "Set group ID", &s_gid, &s_gid,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"host", 'h', "Connect to the remote server as hostname", &host, &host,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"user", 'u', "Connect to the remote server as username.",
         &user, &user, 0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0,
         0, 0},
        {"password", 'p', "Password to connect to remote server.",
         0, 0, 0, GET_STR, OPT_ARG, 0, 0, 0, 0, 0, 0},
        {"port", 'P', "Port number to use for connection or 0 for default to, in "
                      "order of preference, my.cnf, $MYSQL_TCP_PORT, "
#if MYSQL_PORT_DEFAULT == 0
                      "/etc/services, "
#endif
                      "built-in default (" STRINGIFY_ARG(MYSQL_PORT) ").",
         &port, &port, 0, GET_INT, REQUIRED_ARG,
         0, 0, 0, 0, 0, 0},
        {"socket", 'S', "The socket file to use for connection.",
         &sock, &sock, 0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0,
         0, 0},

        {"key-name", OPT_MAX_CLIENT_OPTION,
         "key column name",
         &key_column_name, &key_column_name,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"key-type", OPT_MAX_CLIENT_OPTION,
         "key column type",
         &key_column_type_str, &key_column_type_str,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

        {"candidate-db", OPT_MAX_CLIENT_OPTION,
         "database for determining candidate columns",
         &candidate_db, &candidate_db,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

        {"redo-db", 'd',
         "redo database",
         &redo_db, &redo_db,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {"redo-datetime", 'r',
         "redo datetime",
         &redo_datetime_str, &redo_datetime_str,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
        {0, 0, 0, 0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0}};

/**
  Auxiliary function used by error() and warning().

  Prints the given text (normally "WARNING: " or "ERROR: "), followed
  by the given vprintf-style string, followed by a newline.

  @param format Printf-style format string.
  @param args List of arguments for the format string.
  @param msg Text to print before the string.
*/
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

/**
  This function is used in log_event.cc to report errors.

  @param format Printf-style format string, followed by printf
  varargs.
*/
static void sql_print_error(const char *format, ...)
{
  va_list args;
  va_start(args, format);
  error_or_warning(format, args, "ERROR");
  va_end(args);
}

/**
  Prints a message to stderr, prefixed with the text "ERROR: " and
  suffixed with a newline.

  @param format Printf-style format string, followed by printf
  varargs.
*/
void error(const char *format, ...)
{
  va_list args;
  va_start(args, format);
  error_or_warning(format, args, "ERROR");
  va_end(args);
}

/**
  Prints a message to stderr, prefixed with the text "WARNING: " and
  suffixed with a newline.

  @param format Printf-style format string, followed by printf
  varargs.
*/
void warning(const char *format, ...)
{
  va_list args;
  va_start(args, format);
  error_or_warning(format, args, "WARNING");
  va_end(args);
}

/**
  Frees memory for global variables in this file.
*/
static void cleanup()
{
  DBUG_ENTER("cleanup");
  my_free(start_datetime_str);
  my_free(redo_datetime_str);
  my_free(query_filepath);
  my_free(query_filepath2);
  my_free(graph_filepath);
  my_free(redo_db);
  my_free(candidate_db);
  my_free(binary_log_filepath);
  my_free(state_log_table_filepath);
  my_free(state_log_filepath);

  my_free(key_column_table_name);
  my_free(key_column_name);
  my_free(key_column_type_str);

  my_free(s_gid);
  my_free(host);
  my_free(user);
  my_free(pass);
  my_free(sock);

  if (glob_description_event)
    delete glob_description_event;

  DBUG_VOID_RETURN;
}

static void print_version()
{
  printf("%s Ver 1.0 for %s at %s\n", my_progname, SYSTEM_TYPE, MACHINE_TYPE);
}

static void usage()
{
  print_version();
  puts(ORACLE_WELCOME_COPYRIGHT_NOTICE("2000"));
  printf("\
DB State change tool.\n\n");
  printf("Usage: %s [start-datetime] [stop-datetime]\n", my_progname);
  print_defaults("my", load_groups);
  puts("");
  my_print_help(my_options);
  my_print_variables(my_options);
}

void convert_str_to_timestamp(const char* const str, state_log_time *time)
{
  MYSQL_TIME_STATUS status;
  MYSQL_TIME l_time;
  long dummy_my_timezone;
  uint dummy_in_dst_time_gap;

  /* We require a total specification (date AND time) */
  if (str_to_datetime(str, (uint)strlen(str), &l_time, 0, &status) ||
      l_time.time_type != MYSQL_TIMESTAMP_DATETIME || status.warnings)
  {
    error("Incorrect date and time argument: %s", str);
    exit(1);
  }
  /*
    Note that Feb 30th, Apr 31st cause no error messages and are mapped to
    the next existing day, like in mysqld. Maybe this could be changed when
    mysqld is changed too (with its "strict" mode?).
  */

  time->sec = my_system_gmt_sec(&l_time, &dummy_my_timezone, &dummy_in_dst_time_gap);
  time->sec_part = l_time.second_part;
}

extern "C" my_bool
get_one_option(int optid, const struct my_option *opt __attribute__((unused)),
               char *argument)
{
  bool tty_password = 0;

  switch (optid)
  {
  case OPT_START_DATETIME:
    convert_str_to_timestamp(start_datetime_str, &start_time);
    break;
  case 'r':
    convert_str_to_timestamp(redo_datetime_str, &redo_time);
    break;
  case 'p':
    if (argument == disabled_my_option)
      argument = (char *)""; // Don't require password
    if (argument)
    {
      my_free(pass);
      char *start = argument;
      pass = my_strdup(argument, MYF(MY_FAE));
      while (*argument)
        *argument++ = 'x'; /* Destroy argument */
      if (*start)
        start[1] = 0; /* Cut length of argument */
    }
    else
      tty_password = 1;
    break;
  case 'V':
    print_version();
    opt_version = true;
    break;
  case '?':
    usage();
    opt_version = true;
    break;
  }

  if (tty_password)
    pass = get_tty_password(NullS);

  return 0;
}

bool parse_args(int *argc, char ***argv)
{
  my_getopt_skip_unknown = true;

  int ret = handle_options(argc, argv, my_options, get_one_option);
  if (ret != 0)
  {
    // error
    error("handle_options");
    return false;
  }

  my_getopt_skip_unknown = false;

  return true;
}

bool check_args()
{
  MY_STAT my_file_stat;

  if (!my_stat(state_log_table_filepath, &my_file_stat, MYF(MY_WME)))
  {
    error("Unable to stat the state_log_table_filepath (%s)", state_log_table_filepath);
    return false;
  }

  if (!my_stat(state_log_filepath, &my_file_stat, MYF(MY_WME)))
  {
    error("Unable to stat the state_log_filepath (%s)", state_log_filepath);
    return false;
  }

  if (key_column_name != NULL)
  {
    if (key_column_type_str == NULL)
    {
      error("unknown key column type %s", "NULL");
      error("allow only (int, uint, double, string)");
      return false;
    }

    auto vec = StateUtil::split(".", key_column_name);
    if (vec.size() < 3)
    {
      error("invalid key name %s", key_column_name);
      error("must -> DB.TABLE.COLUMN");
      return false;
    }
    std::string new_db_table_name = *vec.begin() + ".`";
    for (auto iter = vec.begin() + 1; iter != vec.end() - 1; ++iter)
    {
      new_db_table_name += (*iter) + ".";
    }
    new_db_table_name = new_db_table_name.substr(0, new_db_table_name.size() - 1);
    new_db_table_name += "`";

    key_column_table_name = (char *)my_malloc(new_db_table_name.size() + 1, 0);
    memcpy(key_column_table_name, new_db_table_name.c_str(), new_db_table_name.size());
    key_column_table_name[new_db_table_name.size()] = '\0';

    std::string new_name = new_db_table_name + "." + *(vec.end() - 1);

    key_column_name = (char *)my_realloc(key_column_name, new_name.size() + 1, 0);
    memcpy(key_column_name, new_name.c_str(), new_name.size());
    key_column_name[new_name.size()] = '\0';

    if (StateUtil::iequals(key_column_type_str, "uint") ||
        StateUtil::iequals(key_column_type_str, "unsigned"))
    {
      key_column_type = en_column_data_uint;
    }
    else if (StateUtil::iequals(key_column_type_str, "int"))
    {
      key_column_type = en_column_data_int;
    }
    else if (StateUtil::iequals(key_column_type_str, "float") ||
             StateUtil::iequals(key_column_type_str, "double"))
    {
      key_column_type = en_column_data_double;
    }
    else if (StateUtil::iequals(key_column_type_str, "str") ||
             StateUtil::iequals(key_column_type_str, "string"))
    {
      key_column_type = en_column_data_string;
    }
    else
    {
      error("unknown key column type %s", key_column_type_str);
      error("allow only (int, uint, double, string)");
      return false;
    }
  }

  if (redo_datetime_str != NULL)
  {
    // redo 목적으로 사용하는 경우
    return true;
  }

  if (graph_filepath != NULL)
  {
    if (my_stat(graph_filepath, &my_file_stat, 0) &&
        remove(graph_filepath) != 0)
    {
      error("Unable to remove the graph_filepath (%s)(%s)", graph_filepath, strerror(errno));
      return false;
    }
  }

  if (s_gid != NULL)
  {
    char *end = NULL;
    i_gid = std::strtol(s_gid, &end, 10);
    if (*end != '\0')
    {
      error("Failed to convert group id %s", s_gid);
      return false;
    }

    if (i_gid < 1)
    {
      error("Invalid group id %d", i_gid);
      return false;
    }
  }

  if (query_filepath && !my_stat(query_filepath, &my_file_stat, MYF(MY_WME)))
  {
    error("Unable to stat the query_filepath (%s)", query_filepath);
    return false;
  }

  return true;
}

bool check_header(IO_CACHE *file)
{
  uchar header[BIN_LOG_HEADER_SIZE];
  uchar buf[PROBE_HEADER_LEN];
  my_off_t tmp_pos, pos;
  MY_STAT my_file_stat;

  delete glob_description_event;
  if (!(glob_description_event = new Format_description_log_event(3)))
  {
    error("Failed creating Format_description_log_event; out of memory?");
    return false;
  }

  pos = my_b_tell(file);

  /* fstat the file to check if the file is a regular file. */
  if (my_fstat(file->file, &my_file_stat, MYF(0)) == -1)
  {
    error("Unable to stat the file.");
    return false;
  }
  if ((my_file_stat.st_mode & S_IFMT) == S_IFREG)
    my_b_seek(file, (my_off_t)0);

  if (my_b_read(file, header, sizeof(header)))
  {
    error("Failed reading header; probably an empty file.");
    return false;
  }
  if (memcmp(header, BINLOG_MAGIC, sizeof(header)))
  {
    error("File is not a binary log file.");
    return false;
  }

  /*
    Imagine we are running with --start-position=1000. We still need
    to know the binlog format's. So we still need to find, if there is
    one, the Format_desc event, or to know if this is a 3.23
    binlog. So we need to first read the first events of the log,
    those around offset 4.  Even if we are reading a 3.23 binlog from
    the start (no --start-position): we need to know the header length
    (which is 13 in 3.23, 19 in 4.x) to be able to successfully print
    the first event (Start_log_event_v3). So even in this case, we
    need to "probe" the first bytes of the log *before* we do a real
    read_log_event(). Because read_log_event() needs to know the
    header's length to work fine.
  */
  for (;;)
  {
    tmp_pos = my_b_tell(file); /* should be 4 the first time */
    if (my_b_read(file, buf, sizeof(buf)))
    {
      if (file->error)
      {
        error("Could not read entry at offset %llu: "
              "Error in log format or read error.",
              (ulonglong)tmp_pos);
        return false;
      }
      /*
        Otherwise this is just EOF : this log currently contains 0-2
        events.  Maybe it's going to be filled in the next
        milliseconds; then we are going to have a problem if this a
        3.23 log (imagine we are locally reading a 3.23 binlog which
        is being written presently): we won't know it in
        read_log_event() and will fail().  Similar problems could
        happen with hot relay logs if --start-position is used (but a
        --start-position which is posterior to the current size of the log).
        These are rare problems anyway (reading a hot log + when we
        read the first events there are not all there yet + when we
        read a bit later there are more events + using a strange
        --start-position).
      */
      break;
    }
    else
    {
      DBUG_PRINT("info", ("buf[EVENT_TYPE_OFFSET=%d]=%d",
                          EVENT_TYPE_OFFSET, buf[EVENT_TYPE_OFFSET]));
      /* always test for a Start_v3, even if no --start-position */
      if (buf[EVENT_TYPE_OFFSET] == START_EVENT_V3)
      {
        /* This is 3.23 or 4.x */
        if (uint4korr(buf + EVENT_LEN_OFFSET) <
            (LOG_EVENT_MINIMAL_HEADER_LEN + START_V3_HEADER_LEN))
        {
          /* This is 3.23 (format 1) */
          delete glob_description_event;
          if (!(glob_description_event = new Format_description_log_event(1)))
          {
            error("Failed creating Format_description_log_event; "
                  "out of memory?");
            return false;
          }
        }
        break;
      }
      else if (buf[EVENT_TYPE_OFFSET] == FORMAT_DESCRIPTION_EVENT)
      {
        /* This is 5.0 */
        Format_description_log_event *new_description_event;
        my_b_seek(file, tmp_pos); /* seek back to event's start */
        if (!(new_description_event = (Format_description_log_event *)
                  Log_event::read_log_event(file, glob_description_event, 1)))
        /* EOF can't be hit here normally, so it's a real error */
        {
          error("Could not read a Format_description_log_event event at "
                "offset %llu; this could be a log format error or read error.",
                (ulonglong)tmp_pos);
          return false;
        }

        delete glob_description_event;
        glob_description_event = new_description_event;

        DBUG_PRINT("info", ("Setting description_event"));
      }
      else if (buf[EVENT_TYPE_OFFSET] == ROTATE_EVENT)
      {
        Log_event *ev;
        my_b_seek(file, tmp_pos); /* seek back to event's start */
        if (!(ev = Log_event::read_log_event(file, glob_description_event, 1)))
        {
          /* EOF can't be hit here normally, so it's a real error */
          error("Could not read a Rotate_log_event event at offset %llu;"
                " this could be a log format error or read error.",
                (ulonglong)tmp_pos);
          return false;
        }
        delete ev;
      }
      else
        break;
    }
  }
  my_b_seek(file, pos);
  return true;
}

bool process_binary_log(const std::string &filename, BINARY_FUNC_TYPE process)
{
  uchar tmp_buff[BIN_LOG_HEADER_SIZE];

  File fd = my_open(filename.c_str(), O_RDONLY | O_BINARY, MYF(MY_WME));
  if (fd < 0)
  {
    return false;
  }

  IO_CACHE cache;
  if (init_io_cache(&cache, fd, 0, READ_CACHE, 0, 0, MYF(MY_WME | MY_NABP)))
  {
    my_close(fd, MYF(MY_WME));
    return false;
  }

  if (check_header(&cache) != true)
  {
    my_close(fd, MYF(MY_WME));
    end_io_cache(&cache);
    return false;
  }

  if (!glob_description_event || !glob_description_event->is_valid())
  {
    error("Invalid Format_description log event; could be out of memory.");
    my_close(fd, MYF(MY_WME));
    end_io_cache(&cache);
    return false;
  }

  if (my_b_read(&cache, tmp_buff, BIN_LOG_HEADER_SIZE))
  {
    error("Failed reading from file.");
    my_close(fd, MYF(MY_WME));
    end_io_cache(&cache);
    return false;
  }

  for (;;)
  {
    char llbuff[21];
    my_off_t old_off = my_b_tell(&cache);

    Log_event *ev = Log_event::read_log_event(&cache, glob_description_event, 1);
    if (!ev)
    {
      //if binlog wasn't closed properly ("in use" flag is set) don't complain
      //about a corruption, but treat it as EOF and move to the next binlog.

      if (glob_description_event->flags & LOG_EVENT_BINLOG_IN_USE_F)
        cache.error = 0;
      else if (cache.error)
      {
        error("Could not read entry at offset %s: "
              "Error in log format or read error.",
              llstr(old_off, llbuff));
        my_close(fd, MYF(MY_WME));
        end_io_cache(&cache);
        return false;
      }
      // file->error == 0 means EOF, that's OK, we break in this case
      break;
    }
    switch (ev->get_type_code())
    {
    case MARIA_EVENTS_BEGIN:
      delete ev;
      continue;
    default:
      break;
    }

    process(ev->when, ev->when_sec_part, ev->xid);
    delete ev;
  }

  my_close(fd, MYF(MY_WME));
  end_io_cache(&cache);

  return true;
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

  if (!mysql_real_connect(local_mysql, host, user, pass, STATE_CHANGE_DATABASE, port, sock, 0))
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

state_log_time &get_start_time()
{
  return start_time;
}

state_log_time &get_end_time()
{
  return end_time;
}

state_log_time &get_hash_query_time()
{
  return hash_query_time;
}

bool is_hash_matched()
{
  return b_hash_matched;
}

bool is_using_candidate()
{
  return candidate_db == NULL ? false : true;
}

const char *get_key_column_table_name()
{
  return key_column_table_name;
}

const char *get_key_column_name()
{
  return key_column_name;
}

en_state_log_column_data_type get_key_column_type()
{
  return key_column_type;
}

std::vector<std::tuple<std::string, std::string>> get_all_table_list()
{
  auto mysql = StateThreadPool::Instance().GetMySql();

  std::vector<std::tuple<std::string, std::string>> table_list;
  std::string dbname;
  std::string tablename;

  auto t = state_table->GetCurrTransaction();
  if (t != NULL)
  {
    if (t->read_set.size() > 0)
    {
      StateUserQuery::SplitDBNameAndTableName(t->read_set[0], dbname, tablename);
    }
    else if (t->write_set.size() > 0)
    {
      StateUserQuery::SplitDBNameAndTableName(t->write_set[0], dbname, tablename);
    }
  }
  else
  {
    dbname = redo_db;
  }


  if (dbname.size() == 0)
  {
    error("[main::get_all_table_list] Failed to get db name from read / write set");
    return table_list;
  }

  auto list = get_table_list(mysql, dbname);

  for (auto &t : list)
  {
    StateUserQuery::SplitDBNameAndTableName(t, dbname, tablename);
    table_list.emplace_back(std::make_tuple(dbname, tablename));
  }

  return table_list;
}

template <typename T>
void add_buffer(std::vector<char> &buffer, T &data)
{
  char *ptr = (char *)&data;
  buffer.insert(buffer.end(), ptr, ptr + sizeof(data));
}

void add_buffer(std::vector<char> &buffer, const char *data, size_t len)
{
  buffer.insert(buffer.end(), data, data + len);
}

void replace_function(StateQuery *query)
{
  while (query)
  {
    StateUserQuery::ReplaceQuery(query);
    query = query->GetNextPtr();
  }
}

std::future<int> run_hash_event_init_thread(StateTable *&state_table, std::vector<StateTableInfo> *&table_info_list)
{
  return StateThreadPool::Instance().EnqueueJob([&]() -> int {
    mysql_state_hash_check_init(StateThreadPool::Instance().GetMySql());

    if (hash_query_time.sec == 0)
    {
      warning("[hash_init] no hash time");
      return -1;
    }

    // HASH 이벤트 등록
    std::string dbname;
    std::string tablename;
    std::vector<std::string> table_list;
    for (auto &i : state_table->GetUserTableList())
    {
      if (state_table->IsWriteTable(i) == false)
      {
        continue;
      }

      if (StateUserQuery::SplitDBNameAndTableName(i, dbname, tablename) == false)
      {
        continue;
      }

      table_list.emplace_back(dbname + "." + tablename);
    }

    //특정 시간의 쿼리가 (user query 또는 group 쿼리의 마지막 시간) 실행되었을때
    //타겟 테이블들(write 테이블들)의 해시값이 현재의 해시값과 같다면
    //상태전환이 필요하지 않음 -> 프로그램 즉시 종료

    st_state_hash_check check_data;
    st_state_hash_create_table create_data;
    st_state_hash_write_table write_data;

    check_data.target_query_sec = hash_query_time.sec;
    check_data.target_query_sec_part = hash_query_time.sec_part;
    check_data.create_table_count = table_info_list->size();
    check_data.write_table_count = table_list.size();

    std::vector<char> buffer;
    add_buffer(buffer, check_data.target_query_sec);
    add_buffer(buffer, check_data.target_query_sec_part);

    add_buffer(buffer, check_data.create_table_count);
    for (auto &i : *table_info_list)
    {
      create_data.from = i.from;
      create_data.from_sec = i.from_sec;
      create_data.from_sec_part = i.from_sec_part;
      create_data.length = i.name.size();

      add_buffer(buffer, create_data.from);
      add_buffer(buffer, create_data.from_sec);
      add_buffer(buffer, create_data.from_sec_part);
      add_buffer(buffer, create_data.length);
      add_buffer(buffer, i.name.c_str(), i.name.size());
    }

    add_buffer(buffer, check_data.write_table_count);
    for (auto &i : table_list)
    {
      write_data.length = i.size();

      add_buffer(buffer, write_data.length);
      add_buffer(buffer, i.c_str(), i.size());
    }

    mysql_state_hash_check(StateThreadPool::Instance().GetMySql(), buffer.data(), buffer.size());

    int msqid = -1;
    if (-1 == (msqid = msgget((key_t)STATE_HASH_EVENT_KEY, IPC_CREAT | 0666)))
    {
      warning("[hash_init] failed to create hash event queue");
      return -1;
    }

    return msqid;
  });
}

bool g_hash_event_loop = true;
std::future<void> run_hash_event_run_thread(int msqid)
{
  g_hash_event_loop = true;

  return StateThreadPool::Instance().EnqueueJob([&](int msqid) -> void {
    if (msqid == -1)
      return;

    struct state_hash_event msg;
    while (g_hash_event_loop)
    {
      memset(&msg, 0, sizeof(struct state_hash_event));
      if (msgrcv(msqid, (void *)&msg, STATE_HASH_EVENT_MAX_SIZE, STATE_HASH_EVENT_MSG_TYPE, IPC_NOWAIT) == -1)
      {
        if (errno == EAGAIN || errno == ENOMSG)
        {
          usleep(1000 * 10);
          continue;
        }
        else
        {
          warning("[run_hash_event_run_thread] event queue recv error ((%d)%s)", errno, strerror(errno));
          msgctl(msqid, IPC_RMID, NULL);
          return;
        }
      }

      b_hash_matched = true;
      debug("[run_hash_event_run_thread] hash matched");
      break;
    }

    msgctl(msqid, IPC_RMID, NULL);

    return;
  }, msqid);
}

size_t run_query_single(const StateTable::QueryList &list, StateTable *state_table, std::vector<StateTableInfo> *table_info_list)
{
  //해시 초기화 쓰레드 실행
  auto hash_init_future = run_hash_event_init_thread(state_table, table_info_list);

  //쿼리 교체 쓰레드 실행
  debug("[run_query_single] replace query");
  std::vector<std::future<void>> replace_futures;
  for (auto &i : list)
  {
    replace_futures.emplace_back(StateThreadPool::Instance().EnqueueJob(replace_function, i.get()));
  }

  StateThreadPool::Instance().Resize(2); // +1 : HASH 대기 쓰레드

  //쿼리 교체 쓰레드 대기
  for (auto &f : replace_futures)
  {
    f.get();
  }

  //해시 초기화 쓰레드 대기
  int msqid = hash_init_future.get();

  //해시 이벤트 쓰레드 실행
  auto hash_event_future = run_hash_event_run_thread(msqid);

  auto mysql = StateThreadPool::Instance().PopMySql();

  static const char* sp_id = "SP_SINGLE";

  //redo query 실행
  size_t acc_failed_count = 0;
  size_t failed_count = 0;
  for (auto &q : list)
  {
    StateUserQuery::CreateSavePoint(mysql, sp_id);
    StateUserQuery::SetQueryStateXid(mysql, q->xid);

    failed_count = 0;
    for (auto &i : q->transactions)
    {
      if (is_hash_matched() == true)
      {
        debug("[run_query_single] hash matched");
        acc_failed_count += failed_count;
        goto end;
      }

      if (StateUserQuery::SetQueryTime(mysql, i.time) == false)
      {
        ++failed_count;
        break;
      }

      if (mysql_query(mysql, (std::string(STATE_QUERY_COMMENT_STRING) + " " + i.query).c_str()) != 0)
      {
        // error("[run_query_single] query error [%s] [%s]", i.query.c_str(), mysql_error(mysql));
        // q->is_failed = 1;
        ++failed_count;

        StateUserQuery::ClearResult(mysql);

        break;
      }
      // debug("[run_query_single] query [%s]", i.query.c_str());

      StateUserQuery::ClearResult(mysql);
    }

    if (failed_count == 0)
    {
      StateUserQuery::ReleaseSavePoint(mysql, sp_id);
    }
    else
    {
      StateUserQuery::RollbackToSavePoint(mysql, sp_id);
    }

    acc_failed_count += failed_count;
  }

end:
  StateThreadPool::Instance().PushMySql(mysql, true);

  //해시 이벤트 쓰레드 대기
  g_hash_event_loop = false;
  hash_event_future.get();

  return acc_failed_count;
}

size_t run_query_multi(const std::vector<StateQuery *> &list, StateTable *state_table, std::vector<StateTableInfo> *table_info_list)
{
  //해시 초기화 쓰레드 실행
  auto hash_init_future = run_hash_event_init_thread(state_table, table_info_list);

  //쿼리 교체 쓰레드 실행
  debug("[run_query_multi] replace query");
  std::vector<std::future<void>> replace_futures;
  for (auto &i : list)
  {
    replace_futures.emplace_back(StateThreadPool::Instance().EnqueueJob(replace_function, i));
  }

  debug("[run_query_multi] job count :%u", list.size());
  StateThreadPool::Instance().Resize(std::min(list.size() + 1, (size_t)0x3FFF)); // +1 : HASH 대기 쓰레드

  //쿼리 교체 쓰레드 대기
  for (auto &f : replace_futures)
  {
    f.get();
  }

  //해시 초기화 쓰레드 대기
  int msqid = hash_init_future.get();

  //해시 이벤트 쓰레드 실행
  auto hash_event_future = run_hash_event_run_thread(msqid);

  //redo query 쓰레드 실행
  std::vector<std::future<size_t>> redo_futures;
  for (auto &i : list)
  {
    redo_futures.emplace_back(StateThreadPool::Instance().EnqueueJob(StateUserQuery::RunBulkQuery, i));
  }

  //redo query 쓰레드 대기 및 진행 상태 표시
  size_t done_count = 0;
  do
  {
    usleep(1000 * 1000);
    done_count = 0;

    for (auto &f : redo_futures)
    {
      done_count += (f.wait_for(std::chrono::seconds(0)) == std::future_status::ready ? 1 : 0);
    }

    debug("[run_query_multi] job count :%u/%u", done_count, list.size());

  } while (done_count != redo_futures.size());

  size_t total_failed_count = 0;
  for (auto &f : redo_futures)
  {
    total_failed_count += f.get();
  }

  //해시 이벤트 쓰레드 대기
  g_hash_event_loop = false;
  hash_event_future.get();

  return total_failed_count;
}

void clear_db()
{
  auto mysql = StateThreadPool::Instance().GetMySql();

  StateUserQuery::SetForeignKeyChecks(mysql, false);
  StateTable::CreateUndoDB(mysql);
  StateUserQuery::SetForeignKeyChecks(mysql, true);

  mysql_commit(mysql);
}

std::vector<std::string> get_table_list(MYSQL *mysql, const std::string &db)
{
  std::stringstream ss;
  MYSQL_RES *query_result = NULL;
  MYSQL_ROW query_row;
  std::vector<std::string> table_list;

  ss.str(std::string());
  ss << "SELECT TABLE_NAME from information_schema.TABLES WHERE TABLE_SCHEMA = '"
     << db << "'";

  if (mysql_query(mysql, ss.str().c_str()) != 0 ||
      (query_result = mysql_store_result(mysql)) == NULL)
  {
    error("[get_table_list] get table list error [%s] [%s]",
          ss.str().c_str(), mysql_error(mysql));
    return table_list;
  }

  while ((query_row = mysql_fetch_row(query_result)))
  {
    table_list.push_back(db + ".`" + query_row[0] + '`');
  }
  mysql_free_result(query_result);

  return table_list;
}

bool lock_and_sync_table(MYSQL *mysql)
{
  debug("[lock_and_sync_table] start");

  std::stringstream ss;
  for (auto &i : state_table->GetUserTableList())
  {
    if (state_table->IsWriteTable(i) == false)
      continue;

    if (StateUserQuery::ResetQueryTime(mysql) == false)
    {
      error("[lock_and_sync_table] reset query time failed");
      return false;
    }

    ss.str(std::string());
    ss << STATE_QUERY_COMMENT_STRING " ALTER TABLE " << StateUserQuery::MakeFullTableName(i) << " COMMENT='" STATE_ALTER_COMMENT_STRING "';";

    if (mysql_query(mysql, ss.str().c_str()) != 0)
    {
      error("[lock_and_sync_table] failed to lock [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
      return false;
    }
    debug("[lock_and_sync_table] %s", ss.str().c_str());
  }

  state_binary_log->UpdateList();

  end_time.sec_part += 1;
  auto binary_log_list = state_binary_log->GetList(end_time);

  if (binary_log_list.size() == 0)
  {
    debug("[lock_and_sync_table] empty sync query");
    return true;
  }

  start_time = end_time;
  end_time = binary_log_list.back().end;

  sync_state_table = new StateTable(*state_table);
  sync_state_table->SetBinaryLog(state_binary_log);
  sync_state_table->SetTime(start_time, end_time);

  // 새로 추가된 쿼리 로그 읽기
  if (sync_state_table->ReadLog(state_log_filepath) == false)
  {
    fprintf(stderr, "[lock_and_sync_table] StateTable::ReadLog error\n");
    return false;
  }
  if (sync_state_table->ValidLog() == false)
  {
    fprintf(stderr, "[lock_and_sync_table] StateTable::ValidLog error\n");
    return false;
  }

  // Load 된 로그 분석
  sync_state_table->AnalyzeLog();

  // 새로 추가된 쿼리 실행
  size_t ret = sync_state_table->RunQueries();
  if (ret > 0)
  {
    error("[lock_and_sync_table] query failed count %d", ret);
    return false;
  }

  // 동기화 로그 까지 그래프에 추가
  sync_state_table->AddQueries(state_graph);

  debug("[lock_and_sync_table] done");
  return true;
}

// 상태전환 테이블 교체시 외래키 정보를 바로잡기 위한 함수
// case 1 외래키 참조되고 있는 테이블이 교체되면 다른 테이블들의 참조 위치가 BACKUP DB로 변경됨
// case 2 외래키가 있는 테이블이 교체되면 해당 테이블의 참조 위치가 임시 DB로 변경됨
void restore_foreign_key(MYSQL *mysql)
{
  std::stringstream ss;
  MYSQL_RES *query_result = NULL;

  struct st_table
  {
    std::string db;
    std::string table;
    bool operator<(const struct st_table &t) const
    {
      return db < t.db || (db == t.db && table < t.table);
    }
  };

  std::map<struct st_table, std::vector<std::string>> foreign_map;

  auto make_foreign_map = [&foreign_map](MYSQL_RES *query_result) {
    foreign_map.clear();

    MYSQL_ROW query_row;
    while ((query_row = mysql_fetch_row(query_result)))
    {
      struct st_table t = {query_row[1], query_row[2]};
      if (t.db == STATE_BACKUP_DATABASE || t.db == STATE_CHANGE_DATABASE)
        continue;

      std::string k = query_row[0];
      auto iter = foreign_map.find(t);
      if (iter != foreign_map.end())
      {
        iter->second.emplace_back(k);
        StateUtil::unique_vector(iter->second);
      }
      else
      {
        std::vector<std::string> v;
        v.emplace_back(k);
        foreign_map.insert(std::make_pair(t, v));
      }
    }
    mysql_free_result(query_result);
  };

  auto run_alter_query = [&foreign_map](MYSQL *mysql, std::function<std::string(std::string)> func) {
    std::stringstream ss;
    MYSQL_RES *query_result = NULL;
    MYSQL_ROW query_row;

    for (auto &i : foreign_map)
    {
      ss.str(std::string());
      ss << "SHOW CREATE TABLE `" << i.first.db << "`.`" << i.first.table << "`";
      if (mysql_query(mysql, ss.str().c_str()) != 0 ||
          (query_result = mysql_store_result(mysql)) == NULL)
      {
        error("[restore_foreign_key] filaed to show create table query [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
        return;
      }
      // debug("[restore_foreign_key] %s", ss.str().c_str());

      std::string create_table_query;
      while ((query_row = mysql_fetch_row(query_result)))
      {
        create_table_query = query_row[1];
      }
      mysql_free_result(query_result);

      if (create_table_query.size() == 0)
      {
        error("[restore_foreign_key] filaed to get create table query [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
        return;
      }

      auto foreign_list = StateUtil::split("\n", create_table_query);
      for (auto iter = foreign_list.begin(); iter != foreign_list.end();)
      {
        if (iter->find("CONSTRAINT ") == std::string::npos)
        {
          iter = foreign_list.erase(iter);
        }
        else
        {
          bool is_find = false;
          for (auto &k : i.second)
          {
            if (iter->find(k) != std::string::npos)
            {
              is_find = true;
              break;
            }
          }

          if (is_find)
          {
            if (iter->back() == ',')
              *iter = iter->substr(0, iter->size() - 1);
            ++iter;
          }
          else
          {
            iter = foreign_list.erase(iter);
          }
        }
      }

      if (foreign_list.size() == 0)
      {
        error("[restore_foreign_key] filaed to get foreign info [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
        return;
      }

      // ALTER DROP 쿼리
      ss.str(std::string());
      ss << STATE_QUERY_COMMENT_STRING << " ALTER TABLE `" << i.first.db << "`.`" << i.first.table << "` ";
      for (auto &k : i.second)
      {
        ss << "DROP FOREIGN KEY `" << k << "`";
        if (k != i.second.back())
        {
          ss << ", ";
        }
      }
      if (StateUserQuery::ResetQueryTime(mysql) == false)
      {
        error("[restore_foreign_key] reset query time failed");
      }
      if (mysql_query(mysql, ss.str().c_str()) != 0)
      {
        error("[restore_foreign_key] failed to drop foreign key [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
      }
      debug("[restore_foreign_key] %s", ss.str().c_str());

      // ALTER ADD 쿼리
      ss.str(std::string());
      ss << STATE_QUERY_COMMENT_STRING << " ALTER TABLE `" << i.first.db << "`.`" << i.first.table << "` ";
      for (auto &f : foreign_list)
      {
        ss << "ADD " << f;
        if (f != foreign_list.back())
        {
          ss << ", ";
        }
      }

      std::string new_query = ss.str();
      while (true)
      {
        auto tmp_query = func(new_query);
        if (tmp_query.size() > 0)
        {
          new_query = tmp_query;
        }
        else
        {
          break;
        }
      }

      if (new_query.size() > 0)
      {
        if (StateUserQuery::ResetQueryTime(mysql) == false)
        {
          error("[restore_foreign_key] reset query time failed");
        }
        if (mysql_query(mysql, new_query.c_str()) != 0)
        {
          error("[restore_foreign_key] failed to add foreign key [%s] [%s]", new_query.c_str(), mysql_error(mysql));
        }
        debug("[restore_foreign_key] %s", new_query.c_str());
      }
    }
  };

  //case 1
  ss.str(std::string());
  ss << "SELECT CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME "
        "FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE REFERENCED_TABLE_SCHEMA = '" STATE_BACKUP_DATABASE "'";
  if (mysql_query(mysql, ss.str().c_str()) != 0 ||
      (query_result = mysql_store_result(mysql)) == NULL)
  {
    error("[restore_foreign_key] filaed to select foreign info [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
    return;
  }
  // debug("[restore_foreign_key] %s", ss.str().c_str());

  make_foreign_map(query_result);
  run_alter_query(mysql, [](std::string query) -> std::string {
    static const std::string target = "REFERENCES `" STATE_BACKUP_DATABASE "`.";
    auto ref_pos = query.find(target);
    if (ref_pos == std::string::npos)
    {
      // error("[restore_foreign_key] failed to find references db [%s]", query.c_str());
      return std::string();
    }

    auto table_st_pos = ref_pos + target.size() + 1;
    auto table_ed_pos = query.find('`', table_st_pos);
    if (table_ed_pos == std::string::npos)
    {
      error("[restore_foreign_key] failed to find references table [%s]", query.c_str());
      return std::string();
    }

    std::string db, table;
    {
      std::string table_part = query.substr(table_st_pos, table_ed_pos - table_st_pos);

      auto db_pos = table_part.find('.');
      if (db_pos == std::string::npos)
      {
        error("[restore_foreign_key] failed to find original db [%s] [%s]", query.c_str(), table_part.c_str());
        return std::string();
      }
      db = table_part.substr(0, db_pos);

      auto time_pos = table_part.find_last_not_of("0123456789.");
      if (time_pos == std::string::npos || table_part[time_pos] != '_')
      {
        error("[restore_foreign_key] failed to find original table [%s] [%s]", query.c_str(), table_part.c_str());
        return std::string();
      }
      table = table_part.substr(db_pos + 1, time_pos - db_pos - 1);
    }

    return query.replace(ref_pos, table_ed_pos + 1 - ref_pos, "REFERENCES `" + db + "`.`" + table + "`");
  });

  //case 2
  ss.str(std::string());
  ss << "SELECT CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME "
        "FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE REFERENCED_TABLE_SCHEMA = '" STATE_CHANGE_DATABASE "'";
  if (mysql_query(mysql, ss.str().c_str()) != 0 ||
      (query_result = mysql_store_result(mysql)) == NULL)
  {
    error("[restore_foreign_key] filaed to select foreign info [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
    return;
  }
  // debug("[restore_foreign_key] %s", ss.str().c_str());

  make_foreign_map(query_result);
  run_alter_query(mysql, [](std::string query) -> std::string {
    static const std::string target = "REFERENCES `" STATE_CHANGE_DATABASE "`.";
    auto ref_pos = query.find(target);
    if (ref_pos == std::string::npos)
    {
      // error("[restore_foreign_key] failed to find references db [%s]", query.c_str());
      return std::string();
    }

    auto table_st_pos = ref_pos + target.size() + 1;
    auto table_ed_pos = query.find('`', table_st_pos);
    if (table_ed_pos == std::string::npos)
    {
      error("[restore_foreign_key] failed to find references table [%s]", query.c_str());
      return std::string();
    }

    std::string db, table;
    {
      std::string table_part = query.substr(table_st_pos, table_ed_pos - table_st_pos);

      auto db_pos = table_part.find('.');
      if (db_pos == std::string::npos)
      {
        error("[restore_foreign_key] failed to find original db [%s] [%s]", query.c_str(), table_part.c_str());
        return std::string();
      }
      db = table_part.substr(0, db_pos);
      table = table_part.substr(db_pos + 1);
    }

    return query.replace(ref_pos, table_ed_pos + 1 - ref_pos, "REFERENCES `" + db + "`.`" + table + "`");
  });
}

bool swap_and_unlock_table(MYSQL *mysql)
{
  debug("[swap_and_unlock_table] start");
  std::stringstream ss;

  MYSQL_RES *query_result = NULL;
  MYSQL_ROW query_row;

  // DROP TRIGGER
  ss.str(std::string());
  ss << "SELECT TRIGGER_NAME from information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '"
     << STATE_CHANGE_DATABASE << "'";

  if (mysql_query(mysql, ss.str().c_str()) != 0 ||
      (query_result = mysql_store_result(mysql)) == NULL)
  {
    error("[swap_and_unlock_table] get trigger list error [%s] [%s]",
          ss.str().c_str(), mysql_error(mysql));
    return false;
  }
  // debug("[swap_and_unlock_table] %s", ss.str().c_str());

  while ((query_row = mysql_fetch_row(query_result)))
  {
    if (StateUserQuery::ResetQueryTime(mysql) == false)
    {
      error("[swap_and_unlock_table] reset query time failed");
      return false;
    }

    ss.str(std::string());
    ss << STATE_QUERY_COMMENT_STRING " DROP TRIGGER " << STATE_CHANGE_DATABASE << ".`" << query_row[0] << "`";

    if (mysql_query(mysql, ss.str().c_str()) != 0)
    {
      error("[swap_and_unlock_table] drop trigger error [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
      return false;
    }
    debug("[swap_and_unlock_table] %s", ss.str().c_str());
  }
  mysql_free_result(query_result);

  // DROP VIEW
  ss.str(std::string());
  ss << "SELECT TABLE_NAME from information_schema.VIEWS WHERE TABLE_SCHEMA = '"
     << STATE_CHANGE_DATABASE << "'";

  if (mysql_query(mysql, ss.str().c_str()) != 0 ||
      (query_result = mysql_store_result(mysql)) == NULL)
  {
    error("[swap_and_unlock_table] get view list error [%s] [%s]",
          ss.str().c_str(), mysql_error(mysql));
    return false;
  }
  // debug("[swap_and_unlock_table] %s", ss.str().c_str());

  while ((query_row = mysql_fetch_row(query_result)))
  {
    if (StateUserQuery::ResetQueryTime(mysql) == false)
    {
      error("[swap_and_unlock_table] reset query time failed");
      return false;
    }

    ss.str(std::string());
    ss << STATE_QUERY_COMMENT_STRING " DROP VIEW " << STATE_CHANGE_DATABASE << ".`" << query_row[0] << "`";

    if (mysql_query(mysql, ss.str().c_str()) != 0)
    {
      error("[swap_and_unlock_table] drop view error [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
      return false;
    }
    debug("[swap_and_unlock_table] %s", ss.str().c_str());
  }
  mysql_free_result(query_result);

  //make valid table list
  std::vector<std::string> valid_table_list;
  std::string dbname;
  std::string tablename;

  ss.str(std::string());
  ss << "SELECT TABLE_NAME from information_schema.TABLES WHERE TABLE_SCHEMA = '"
     << STATE_CHANGE_DATABASE << "'";

  if (mysql_query(mysql, ss.str().c_str()) != 0 ||
      (query_result = mysql_store_result(mysql)) == NULL)
  {
    error("[swap_and_unlock_table] get table list error [%s] [%s]",
          ss.str().c_str(), mysql_error(mysql));
    return false;
  }

  while ((query_row = mysql_fetch_row(query_result)))
  {
    if (StateUserQuery::SplitDBNameAndTableName(query_row[0], dbname, tablename) == false)
    {
      error("[swap_and_unlock_table] SplitDBNameAndTableName error %s", query_row[0]);
      return false;
    }

    valid_table_list.push_back(dbname + ".`" + tablename + '`');
  }
  mysql_free_result(query_result);

  //UNLOCK AND RENAME
  ss.str(std::string());

  for (auto &i : state_table->GetUserTableList())
  {
    if (std::find(valid_table_list.begin(), valid_table_list.end(), i) == valid_table_list.end())
    {
      debug("[swap_and_unlock_table] continue table not exists %s", i.c_str());
      continue;
    }

    if (state_table->IsWriteTable(i) == false)
    {
      debug("[swap_and_unlock_table] continue read table %s", i.c_str());
      continue;
    }

    if (StateUserQuery::SplitDBNameAndTableName(i, dbname, tablename) == false)
    {
      error("[swap_and_unlock_table] SplitDBNameAndTableName error %s", i.c_str());
      return false;
    }

    if (StateUserQuery::ResetQueryTime(mysql) == false)
    {
      error("[swap_and_unlock_table] reset query time failed");
      return false;
    }

    // 기존 테이블 삭제
    ss.str(std::string());
    ss << STATE_QUERY_COMMENT_STRING " DROP TABLE " << StateUserQuery::MakeFullTableName(i) << "; ";
    if (mysql_query(mysql, ss.str().c_str()) != 0)
    {
      error("[swap_and_unlock_table] failed to drop [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
      return false;
    }
    debug("[swap_and_unlock_table] %s", ss.str().c_str());

    if (StateUserQuery::ResetQueryTime(mysql) == false)
    {
      error("[swap_and_unlock_table] reset query time failed");
      return false;
    }

    // 상태전환 테이블을 기존 테이블로 대체
    ss.str(std::string());
    ss << STATE_QUERY_COMMENT_STRING " RENAME TABLE "
       << STATE_CHANGE_DATABASE
       << ".`" << dbname << "." << tablename << "`"
       << " TO "
       << StateUserQuery::MakeFullTableName(i)
       << "; ";
    if (mysql_query(mysql, ss.str().c_str()) != 0)
    {
      error("[swap_and_unlock_table] failed to rename [%s] [%s]", ss.str().c_str(), mysql_error(mysql));
      return false;
    }
    debug("[swap_and_unlock_table] %s", ss.str().c_str());
  }

  //외래키 정보 복구
  restore_foreign_key(mysql);

  if (mysql_commit(mysql) != 0)
  {
    error("[swap_and_unlock_table] failed to commit [%s]", mysql_error(mysql));
    return false;
  }

  if (mysql_autocommit(mysql, true) != 0)
  {
    error("[swap_and_unlock_table] failed to autocommit [%s]", mysql_error(mysql));
  }

  debug("[swap_and_unlock_table] commit done");

  return true;
}

bool run_only_candidate()
{
  if (start_time.sec == 0)
  {
    error("[run_only_candidate] start time is not set");
    return false;
  }

  //start_time 재설정
  if (start_time.sec_part == 0)
  {
    start_time.sec--;
    start_time.sec_part = 999999;
  }
  else
  {
    start_time.sec_part--;
  }

  if (candidate_db == NULL)
  {
    error("[run_only_candidate] candidate DB is not set");
    return false;
  }

  auto mysql = StateThreadPool::Instance().GetMySql();

  // TABLE 목록 조회
  auto undo_table_list = get_table_list(mysql, candidate_db);
  if (undo_table_list.size() == 0)
  {
    error("[run_only_candidate] undo table empty");
    return false;
  }

  state_table = new StateTable();
  state_table->UpdateWriteUserTableList(undo_table_list);
  state_table->SetBinaryLog(state_binary_log);
  state_table->SetTime(start_time, end_time);

  // VIEW, TRIGGER, TABLE 목록 관련 로그 읽기
  debug("[run_only_candidate] Read state table log");
  if (state_table->ReadTableLog(state_log_table_filepath) == false)
  {
    error("[run_only_candidate] StateTable::ReadTableLog error");
    return false;
  }

  // 전체 쿼리 로그 읽기
  debug("[run_only_candidate] Read state log");
  if (state_table->ReadLog(state_log_filepath) == false)
  {
    error("[run_only_candidate] StateTable::ReadLog error");
    return false;
  }
  if (state_table->ValidLog() == false)
  {
    error("[run_only_candidate] StateTable::ValidLog error");
    return false;
  }
  end_load_log_time_point = std::chrono::high_resolution_clock::now();

  std::string column;
  if (state_table->FindCandidateColumn(column) == false)
  {
    error("[run_only_candidate] StateTable::FindCandidateColumn error");
    return false;
  }
  fprintf(stdout, "CANDIDATE COLUMN: %s\n", column.c_str());

  auto vec = StateUserQuery::SplitDBNameAndTableName(column);
  if (vec.size() == 3)
  {
    column = vec[0] + "." + vec[1] + "." + vec[2];
    fprintf(stdout, "STRIP CANDIDATE COLUMN: %s\n", column.c_str());
  }

  fflush(stdout);

  return true;
}

bool run_only_redo()
{
  //start_time : undo datetime
  //redo_time : redo datetime
  if (start_time.sec == 0)
  {
    error("[run_only_redo] start time is not set");
    return false;
  }

  if (redo_time.sec == 0)
  {
    error("[run_only_redo] redo time is not set");
    return false;
  }

  //start_time 재설정
  if (start_time.sec_part == 0)
  {
    start_time.sec--;
    start_time.sec_part = 999999;
  }
  else
  {
    start_time.sec_part--;
  }

  if (redo_db == NULL)
  {
    error("[run_only_redo] redo DB is not set");
    return false;
  }

  auto mysql = StateThreadPool::Instance().GetMySql();

  // TABLE 목록 조회
  auto undo_table_list = get_table_list(mysql, redo_db);
  if (undo_table_list.size() == 0)
  {
    error("[run_only_redo] undo table empty");
    return false;
  }

  state_table = new StateTable();
  state_table->UpdateWriteUserTableList(undo_table_list);
  state_table->SetBinaryLog(state_binary_log);
  state_table->SetTime(start_time, end_time);

  // VIEW, TRIGGER, TABLE 목록 관련 로그 읽기
  debug("[run_only_redo] Read state table log");
  if (state_table->ReadTableLog(state_log_table_filepath) == false)
  {
    error("[run_only_redo] StateTable::ReadTableLog error");
    return false;
  }

  // 전체 쿼리 로그 읽기
  debug("[run_only_redo] Read state log");
  if (state_table->ReadLog(state_log_filepath) == false)
  {
    error("[run_only_redo] StateTable::ReadLog error");
    return false;
  }
  if (state_table->ValidLog() == false)
  {
    error("[run_only_redo] StateTable::ValidLog error");
    return false;
  }

  end_load_log_time_point = std::chrono::high_resolution_clock::now();
  end_query_analyze_time_point = end_load_log_time_point;

  // UNDO 대상 TABLE 생성
  debug("[run_only_redo] Create undo table");
  auto table_list = get_all_table_list();
  if (table_list.size() == 0)
  {
    return false;
  }
  if (state_table->CreateUndoTables(table_list) == false)
  {
    error("[run_only_redo] StateTable::CreateUndoTables error");
    return false;
  }
  end_undo_time_point = std::chrono::high_resolution_clock::now();

  if (mysql_select_db(mysql, STATE_CHANGE_DATABASE) != 0)
  {
    error("[run_only_redo] failed to change db [%s]", mysql_error(mysql));
    return false;
  }

  auto &valid_log_list = state_table->GetValidQueryList();

  //쿼리 교체 쓰레드 실행
  debug("[run_only_redo] replace query");
  std::vector<std::future<void>> replace_futures;
  for (auto &q : valid_log_list)
  {
    if (q->transactions.front().time < redo_time)
    {
      continue;
    }

    replace_futures.emplace_back(StateThreadPool::Instance().EnqueueJob(replace_function, q.get()));
  }

  //쿼리 교체 쓰레드 대기
  for (auto &f : replace_futures)
  {
    f.get();
  }

  static const char* sp_id = "SP_REDO";

  for (auto &q : valid_log_list)
  {
    if (q->transactions.front().time < redo_time)
    {
      continue;
    }

    StateUserQuery::CreateSavePoint(mysql, sp_id);
    StateUserQuery::SetQueryStateXid(mysql, q->xid);

    size_t failed_count = 0;
    for (auto &i : q->transactions)
    {
      if (StateUserQuery::SetQueryTime(mysql, i.time) == false)
      {
        ++failed_count;
        break;
      }

      if (mysql_query(mysql, (std::string(STATE_QUERY_COMMENT_STRING) + " " + i.query).c_str()) != 0)
      {
        // error("[run_only_redo] query error [%s] [%s]", i.query.c_str(), mysql_error(mysql));
        // q->is_failed = 1;
        ++failed_count;

        StateUserQuery::ClearResult(mysql);

        break;
      }
      // debug("[run_only_redo] query [%s]", i.query.c_str());

      StateUserQuery::ClearResult(mysql);
    }

    if (failed_count == 0)
    {
      StateUserQuery::ReleaseSavePoint(mysql, sp_id);
    }
    else
    {
      StateUserQuery::RollbackToSavePoint(mysql, sp_id);
    }
  }

  end_redo_time_point = std::chrono::high_resolution_clock::now();

  if (mysql_commit(mysql) != 0)
  {
    error("[run_only_redo] failed to commit [%s]", mysql_error(mysql));
  }

  {
    // SYNC
    if (lock_and_sync_table(mysql) == false)
    {
      error("[run_only_redo] lock_and_sync_table failed");
      return false;
    }
    end_sync_table_time_point = std::chrono::high_resolution_clock::now();

    if (swap_and_unlock_table(mysql) == false)
    {
      error("[run_only_redo] swap_and_unlock_table failed");
      return false;
    }
    end_swap_table_time_point = std::chrono::high_resolution_clock::now();
  }

  return true;
}

int main(int argc, char **argv)
{
  start_time_point = std::chrono::high_resolution_clock::now();

  MY_INIT(argv[0]);
  DBUG_ENTER("main");
  DBUG_PROCESS(argv[0]);

  my_init_time(); // for time functions
  tzset();        // set tzname

  char **defaults_argv;
  int ret = EXIT_SUCCESS;
  MYSQL *mysql = NULL;

  load_defaults_or_exit("my", load_groups, &argc, &argv);

  defaults_argv = argv;

  if (parse_args(&argc, (char ***)&argv) == false)
  {
    error("[main] parse_args error");
    goto err;
  }

  if (!argc || opt_version)
  {
    if (!opt_version)
    {
      usage();
    }
    goto err;
  }

  if (check_args() == false)
  {
    error("[main] check_args error");
    goto err;
  }

  mysql = StateThreadPool::Instance().GetMySql();

  if (mysql_state_query_start(mysql) != 0)
  {
    error("[main] mysql_state_query_start failed");
    goto err;
  }

  // 상태 전환 프로그램 동작 시점의 바이너리 로그 파일 목록 획득
  // 로그의 가장 마지막 시간을 end_time 으로 설정하고 추후 동기화 때 사용
  state_binary_log = new StateBinaryLog(binary_log_filepath, process_binary_log);
  {
    auto binary_log_list = state_binary_log->GetList();
    end_time = binary_log_list.back().end;
  }

  StateThreadPool::Instance().Resize(std::thread::hardware_concurrency() + 1);
  // StateThreadPool::Instance().Resize(1);

  state_user_query = new StateUserQuery();
  state_graph = new StateGraphBoost();
  // if (graph_filepath == NULL)
  //   state_graph = new StateGraphBoost();
  // else
  //   state_graph = new StateGraphGvc();

  if (redo_datetime_str != NULL)
  {
    if (run_only_redo() == false)
    {
      error("[main] run_only_redo error");
      goto err;
    }

    ret = EXIT_SUCCESS;
    goto exit;
  }

  if (candidate_db != NULL)
  {
    if (run_only_candidate() == false)
    {
      error("[main] run_only_candidate error");
      goto err;
    }

    ret = EXIT_SUCCESS;
    goto exit;
  }

  if (i_gid > 0 && query_filepath)
  {
    // 지정한 group id 를 취소하고 새로운 쿼리 추가

    // 사용자 쿼리 분석
    if (state_user_query->SetQueryFile(query_filepath) != EXIT_SUCCESS)
    {
      error("[main] StateUserQuery::SetQueryFile error");
      goto err;
    }

    state_table = new StateTable(state_user_query->GetUserTransaction());
    state_table->SetBinaryLog(state_binary_log);
    state_table->SetTime(start_time, end_time);

    // group id 에 해당하는 쿼리 시간 읽기
    if (state_table->SetGroupID(i_gid) == false)
    {
      warning("[main] StateTable::SetGroupID error", i_gid);
      goto err;
    }

    // 쿼리 시간에 해당하는 쿼리 로그 읽기
    // start_time 을 group 쿼리의 제일 첫번째로 설정
    if (state_table->ReadGroupLog(state_log_filepath) == false)
    {
      warning("[main] StateTable::ReadGroupLog error");
      goto err;
    }
  }
  else if (i_gid > 0)
  {
    // 지정한 group id 를 취소

    state_table = new StateTable();
    state_table->SetBinaryLog(state_binary_log);
    state_table->SetTime(start_time, end_time);

    // group id 에 해당하는 쿼리 시간 읽기
    if (state_table->SetGroupID(i_gid) == false)
    {
      warning("[main] StateTable::SetGroupID error", i_gid);
      goto err;
    }

    // 쿼리 시간에 해당하는 쿼리 로그 읽기
    // start_time 을 group 쿼리의 제일 첫번째로 설정
    if (state_table->ReadGroupLog(state_log_filepath) == false)
    {
      warning("[main] StateTable::ReadGroupLog error");
      goto err;
    }
  }
  else if (query_filepath)
  {
    // 새로운 쿼리 추가

    // 사용자 쿼리 분석
    if (state_user_query->SetQueryFile(query_filepath) != EXIT_SUCCESS)
    {
      error("[main] StateUserQuery::SetQueryFile error");
      goto err;
    }

    // 사용자 쿼리와 연관된 테이블을 기준으로 설정
    state_table = new StateTable(state_user_query->GetUserTransaction());
    state_table->SetBinaryLog(state_binary_log);
    state_table->SetTime(start_time, end_time);
  }
  else if (query_filepath2)
  {
    // template
    // USE DB_NAME;
    // ADD, TIMESTAMP, QUERY;
    // DEL, TIMESTAMP;
    // ex)
    // USE tpcc;
    // ADD, 2021-04-11 09:30:12.012345, UPDATE DISTRICT SET D_YTD = D_YTD + 4027.219970703125 WHERE D_ID > 1 AND D_ID < 10;
    // DEL, 2021-04-11 09:31:15.678901;
    // DEL, 2021-04-11 09:31:16;
    // ADD, 2021-04-11 09:30:17, UPDATE DISTRICT SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = 1 AND D_ID = 6;

    // 사용자 쿼리 분석
    if (state_user_query->SetQueryFile2(query_filepath2) != EXIT_SUCCESS)
    {
      error("[main] StateUserQuery::SetQueryFile2 error");
      goto err;
    }

    auto user_add_transaction = state_user_query->GetUserAddTransaction();
    auto user_del_transaction = state_user_query->GetUserDelTransaction();

    if (user_add_transaction->transactions.size() == 0 && user_del_transaction->transactions.size() == 0)
    {
      error("[main] query2 is empty");
      goto err;
    }

    if (user_add_transaction->transactions.size() > 0)
    {
      start_time = user_add_transaction->transactions.front().time;
    }
    else
    {
      // 쿼리 삭제의 경우 타겟 쿼리가 없는 시점으로 시간을 당김
      // 삭제할 쿼리를 실행하지 않음
      start_time = user_del_transaction->transactions.front().time;
      --start_time;
    }

    if (user_del_transaction->transactions.size() > 0 &&
        start_time > user_del_transaction->transactions.front().time)
    {
      start_time = user_del_transaction->transactions.front().time;
      --start_time;
    }

    state_table = new StateTable(user_add_transaction);
    state_table->SetBinaryLog(state_binary_log);
    state_table->SetTime(start_time, end_time);

    // user_del_transaction 에 해당하는 쿼리 로그 읽기
    auto res = state_table->ReadUserDelLog(state_log_filepath, user_del_transaction);
    if (std::get<0>(res) == false)
    {
      warning("[main] StateTable::ReadGroupLog error");
      goto err;
    }
    auto group_list = std::get<1>(res);

    // user 쿼리에 writeset 이 없는지 확인
    bool is_user_add_no_writeset = true;
    bool is_user_del_no_writeset = true;
    if (user_add_transaction->transactions.size() > 0 && user_add_transaction->write_set.size() > 0)
    {
      is_user_add_no_writeset = false;
    }

    if (group_list.size() > 0)
    {
      for (auto &q : group_list)
      {
        if (q->write_set.size() > 0)
        {
          is_user_del_no_writeset = false;
          break;
        }
      }
    }

    if (is_user_add_no_writeset && is_user_del_no_writeset)
    {
      error("[main] no write set");
      return EXIT_FAILURE;
    }
  }
  else
  {
    error("[main] Invalid arguments passed");
    goto err;
  }

  // VIEW, TRIGGER, TABLE 목록 관련 로그 읽기
  debug("[main] Read state table log");
  if (state_table->ReadTableLog(state_log_table_filepath) == false)
  {
    error("[main] StateTable::ReadTableLog error");
    goto err;
  }

  // 전체 쿼리 로그 읽기
  debug("[main] Read state log");
  if (state_table->ReadLog(state_log_filepath) == false)
  {
    fprintf(stderr, "[main] StateTable::ReadLog error");
    goto err;
  }
  if (state_table->ValidLog() == false)
  {
    fprintf(stderr, "[main] StateTable::ValidLog error");
    goto err;
  }

  end_load_log_time_point = std::chrono::high_resolution_clock::now();

  // Load 된 로그 분석
  if (state_table->AnalyzeLog() == false)
  {
    //재생할 쿼리가 없기 때문에 read table 을 생성할 필요 없음
    state_table->ClearReadTableList();

#if 0
    end_query_analyze_time_point = std::chrono::high_resolution_clock::now();
    end_undo_time_point = std::chrono::high_resolution_clock::now();
    end_redo_time_point = std::chrono::high_resolution_clock::now();
    end_sync_table_time_point = std::chrono::high_resolution_clock::now();
    end_swap_table_time_point = std::chrono::high_resolution_clock::now();
    ret = EXIT_SUCCESS;
    goto exit;
#endif
  }
  // state_table->ReduceLogForTest(25);

  if (true)
  {
    // 쿼리 병렬 실행 구조

    // 사용자 쿼리 그래프에 추가
    auto user_transaction = state_user_query->GetUserTransaction();
    auto user_add_transaction = state_user_query->GetUserAddTransaction();

    if (user_transaction != NULL && user_transaction->transactions.size() > 0)
    {
      debug("[main] Add User Query %d", user_transaction->transactions.size());

      auto db = state_user_query->GetDatabase();
      for (auto &t : user_transaction->transactions)
      {
        t.time = start_time;

        if (mysql_state_query_logging(mysql, start_time.sec, start_time.sec_part, db.c_str(), t.query.c_str()) != 0)
        {
          error("[main] mysql_state_query_logging failed");
          goto err;
        }
      }
    }
    else if (user_add_transaction != NULL && user_add_transaction->transactions.size() > 0)
    {
      debug("[main] Add User Query2 %d", user_add_transaction->transactions.size());

      auto db = state_user_query->GetDatabase();
      for (auto &t : user_add_transaction->transactions)
      {
        if (mysql_state_query_logging(mysql, t.time.sec, t.time.sec_part, db.c_str(), t.query.c_str()) != 0)
        {
          error("[main] mysql_state_query_logging failed");
          goto err;
        }
      }
    }

    // redo 쿼리 그래프에 추가
    debug("[main] Make query graph");
    state_table->AddQueries(state_graph);

    // 그래프에서 병렬로 실행할 쿼리 묶음 생성
    auto head_list = state_graph->GetQueries();
    if (graph_filepath != NULL)
    {
      debug("[main] Make graph image");
      state_graph->MakeOutputFilename("svg", graph_filepath);
    }

    end_query_analyze_time_point = std::chrono::high_resolution_clock::now();

    auto table_list = get_all_table_list();
    if (table_list.size() == 0)
    {
      goto err;
    }

    // UNDO 대상 TABLE 생성
    debug("[main] Create undo table");
    auto table_info_list = new std::vector<StateTableInfo>;
    if (state_table->CreateUndoTables(table_list, table_info_list) == false)
    {
      error("[main] StateTable::CreateUndoTables error");
      goto err;
    }

    end_undo_time_point = std::chrono::high_resolution_clock::now();

    // REDO
    debug("[main] Run redo query");
    ret = run_query_multi(head_list, state_table, table_info_list);
    if (ret > 0)
      error("[main] Run redo query failed count %d", ret);

    end_redo_time_point = std::chrono::high_resolution_clock::now();
  }
  else
  {
    // 쿼리 단일 실행 구조

    auto &valid_log_list = state_table->GetValidQueryList();

    // 사용자 쿼리 추가
    auto user_transaction = state_user_query->GetUserTransaction();
    auto user_add_transaction = state_user_query->GetUserAddTransaction();

    if (user_transaction != NULL)
    {
      debug("[main] Add User Query %d", user_transaction->transactions.size());

      auto db = state_user_query->GetDatabase();
      for (auto &t : user_transaction->transactions)
      {
        t.time = start_time;

        if (mysql_state_query_logging(mysql, start_time.sec, start_time.sec_part, db.c_str(), t.query.c_str()) != 0)
        {
          error("[main] mysql_state_query_logging failed");
          goto err;
        }
      }

      valid_log_list.insert(valid_log_list.begin(), user_transaction);
      StateTable::SortList(valid_log_list);
    }
    else if (user_add_transaction->transactions.size() > 0)
    {
      debug("[main] Add User Query2 %d", user_add_transaction->transactions.size());

      auto db = state_user_query->GetDatabase();
      for (auto &t : user_add_transaction->transactions)
      {
        if (mysql_state_query_logging(mysql, t.time.sec, t.time.sec_part, db.c_str(), t.query.c_str()) != 0)
        {
          error("[main] mysql_state_query_logging failed");
          goto err;
        }
      }

      valid_log_list.insert(valid_log_list.begin(), user_add_transaction);
      StateTable::SortList(valid_log_list);
    }

    auto table_list = get_all_table_list();
    if (table_list.size() == 0)
    {
      goto err;
    }

    // UNDO 대상 TABLE 생성
    debug("[main] Create undo table");
    auto table_info_list = new std::vector<StateTableInfo>;
    if (state_table->CreateUndoTables(table_list, table_info_list) == false)
    {
      error("[main] StateTable::CreateUndoTables error");
      goto err;
    }

    end_undo_time_point = end_query_analyze_time_point = std::chrono::high_resolution_clock::now();

    // REDO
    debug("[main] Run redo query");
    ret = run_query_single(valid_log_list, state_table, table_info_list);
    if (ret > 0)
      error("[main] Run redo query failed count %d", ret);

    end_redo_time_point = std::chrono::high_resolution_clock::now();
  }

  if (is_hash_matched() == true)
  {
    end_sync_table_time_point = std::chrono::high_resolution_clock::now();
    end_swap_table_time_point = std::chrono::high_resolution_clock::now();
    ret = EXIT_SUCCESS;
    goto exit;
  }
  else
  {
    // SYNC
    if (lock_and_sync_table(mysql) == false)
    {
      error("[main] lock_and_sync_table failed");
      goto err;
    }
    end_sync_table_time_point = std::chrono::high_resolution_clock::now();

    if (swap_and_unlock_table(mysql) == false)
    {
      error("[main] swap_and_unlock_table failed");
      goto err;
    }
    end_swap_table_time_point = std::chrono::high_resolution_clock::now();
  }

  if (mysql_state_query_commit(mysql) != 0)
  {
    error("[main] mysql_state_query_commit failed");
    goto err;
  }

  // state_table->ClearGroupID();
  state_graph->PrintSummary();

  ret = EXIT_SUCCESS;
  goto exit;

err:
  ret = EXIT_FAILURE;

exit:
  if (ret == EXIT_SUCCESS)
  {
    auto time_print_func = [](const char *msg, const std::chrono::time_point<std::chrono::high_resolution_clock> &tp) {
      auto usec = std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch());
      auto sec = std::chrono::duration_cast<std::chrono::seconds>(usec);
      usec -= sec;

      printf("%s : %s.%ld\n", msg, StateUtil::format_time(sec.count(), "%Y-%m-%d %H:%M:%S").c_str(), usec.count());
    };

    end_time_point = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time_point - start_time_point;
    printf("PROGRAM RUN SUMMARY ==========================\n");
    time_print_func("start time", start_time_point);
    time_print_func("load log end time", end_load_log_time_point);
    time_print_func("query analyze end time", end_query_analyze_time_point);
    time_print_func("undo end time", end_undo_time_point);
    time_print_func("redo end time", end_redo_time_point);
    time_print_func("sync table end time", end_sync_table_time_point);
    time_print_func("swap table end time", end_swap_table_time_point);
    time_print_func("end time", end_time_point);
    printf("elapsed time : %ld ms\n", std::chrono::duration_cast<std::chrono::milliseconds>(elapsed_seconds).count());
    printf("==============================================\n");

    //CLEAR DB
    clear_db();
  }
  debug("[main] Cleanup");

  StateThreadPool::Instance().Release();

  if (state_table)
    delete state_table;
  if (sync_state_table)
    delete sync_state_table;
  if (state_graph)
    delete state_graph;
  if (state_user_query)
    delete state_user_query;
  if (state_binary_log)
    delete state_binary_log;

  cleanup();
  free_defaults(defaults_argv);
  my_free_open_file_info();
  mysql_server_end();

  if (ret == EXIT_FAILURE)
  {
    debug("[main] exit failure");
    my_end(MY_CHECK_ERROR);
    return EXIT_FAILURE;
  }
  else
  {
    debug("[main] exit success");
    my_end(MY_DONT_FREE_DBUG);
    return EXIT_SUCCESS;
  }
}

/*
  We must include this here as it's compiled with different options for
  the server
*/

#include "rpl_tblmap.cc"
#undef TABLE
#include "my_decimal.h"
#include "decimal.c"
#include "my_decimal.cc"
#include "../sql-common/my_time.c"
#include "password.c"
#include "log_event.cc"
#include "log_event_old.cc"
#include "rpl_utility.cc"
#include "sql_string.cc"
#include "sql_list.cc"
#include "rpl_filter.cc"
#include "compat56.cc"
