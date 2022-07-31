
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

#include "StateThreadPool.h"
#include "StateSimpleThreadPool.h"
#include "StateTable.h"
#include "StateBinaryLog.h"
#include "StateQuery.hpp"
#include "StateUserQuery.h"
#include "state_log_hdr.h"
#include "mysqld.h"

#include "nlohmann/json.hpp"

#include <sys/ipc.h>
#include <sys/msg.h>
#include <netinet/in.h>
#include <libgen.h>
#include <vector>
#include <string>
#include <algorithm>
#include <fstream>

#define BIN_LOG_HEADER_SIZE 4
#define PROBE_HEADER_LEN (EVENT_LEN_OFFSET + 4)

static char *start_datetime_str;
static char *end_datetime_str;
static char *binary_log_filepath;
static char *state_log_table_filepath;
static char *state_log_filepath;
static char *output_filepath;
static char *output_format;
static char *database;

static std::string output_log;
static std::string output_json;
static std::string output_table_log;
static std::string output_table_json;

static bool opt_version = false;
static const char *load_groups[] =
    {"client", "client-server", "client-mariadb",
     "mysqld",
     0};

static state_log_time start_time = {0, 0};
static state_log_time end_time = {0, 0};

static StateTable::QueryList table_list;
static StateTable::QueryList valid_table_list;
static StateTable::QueryList log_list;
static StateTable::QueryList valid_log_list;

StateBinaryLog *state_binary_log = NULL;
StateSimpleThreadPool *StateSimpleThreadPool::instance_ = NULL;

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
        {"stop-datetime", OPT_STOP_DATETIME,
         "Stop reading the binlog at first event having a datetime equal or "
         "posterior to the argument; the argument must be a date and time "
         "in the local time zone, in any format accepted by the MySQL server "
         "for DATETIME and TIMESTAMP types, for example: 2004-12-25 11:25:56 "
         "(you should probably use quotes for your shell to set it properly).",
         &end_datetime_str, &end_datetime_str,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

        {"output", OPT_MAX_CLIENT_OPTION,
         "File path to write",
         &output_filepath, &output_filepath,
         0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

        {"format", OPT_MAX_CLIENT_OPTION,
         "File format to output (log/json)",
         &output_format, &output_format,
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

        // {"database", 'd', "List entries for just this database (local log only).",
        //  &database, &database, 0, GET_STR_ALLOC, REQUIRED_ARG,
        //  0, 0, 0, 0, 0, 0}
};

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
 * 사용하지 않으나 컴파일을 위함 --- START
 */

StateThreadPool *StateThreadPool::instance_ = NULL;

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

const char *get_key_column_name()
{
    return NULL;
}

en_state_log_column_data_type get_key_column_type()
{
    return en_column_data_null;
}

MYSQL *open_mysql()
{
    return NULL;
}
/**
 * 사용하지 않으나 컴파일을 위함 --- END
 */

/**
  Frees memory for global variables in this file.
*/
static void cleanup()
{
    DBUG_ENTER("cleanup");
    my_free(start_datetime_str);
    my_free(end_datetime_str);
    my_free(binary_log_filepath);
    my_free(state_log_table_filepath);
    my_free(state_log_filepath);
    my_free(output_filepath);
    my_free(output_format);
    my_free(database);

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
DB State log viewer.\n\n");
    printf("Usage: %s [start-datetime] [stop-datetime]\n", my_progname);
    print_defaults("my", load_groups);
    puts("");
    my_print_help(my_options);
    my_print_variables(my_options);
}

void convert_str_to_timestamp(const char *const str, state_log_time *time)
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
    switch (optid)
    {
    case OPT_START_DATETIME:
        convert_str_to_timestamp(start_datetime_str, &start_time);
        break;
    case OPT_STOP_DATETIME:
        convert_str_to_timestamp(end_datetime_str, &end_time);
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
        error("[check_args] Unable to stat the state_log_table_filepath (%s)", state_log_table_filepath);
        return false;
    }

    if (!my_stat(state_log_filepath, &my_file_stat, MYF(MY_WME)))
    {
        error("[check_args] Unable to stat the state_log_filepath (%s)", state_log_filepath);
        return false;
    }

    if (start_time.sec == 0 && start_time.sec_part == 0)
    {
        error("[check_args] invalid start time : %ld.%lu", start_time.sec, start_time.sec_part);
        return false;
    }
    if ((end_time.sec != 0 || end_time.sec_part != 0) && (start_time >= end_time))
    {
        error("[check_args] invalid start time : %ld.%lu", start_time.sec, start_time.sec_part);
        error("[check_args] invalid end time : %ld.%lu", end_time.sec, end_time.sec_part);
        return false;
    }

    if (output_filepath == NULL)
    {
        error("[check_args] output is not set");
        return false;
    }

    output_log = std::string(output_filepath) + ".log";
    output_json = std::string(output_filepath) + ".json";
    output_table_log = std::string(output_filepath) + ".table.log";
    output_table_json = std::string(output_filepath) + ".table.json";

    const char *dir = dirname(output_filepath);
    if (NULL == my_stat(dir, &my_file_stat, 0))
    {
        error("[check_args] failed to stat output path (%s)", dir);
        return false;
    }
    if (!S_ISDIR(my_file_stat.st_mode))
    {
        error("[check_args] output path is not directory (%s)", dir);
        return false;
    }
    if (0 != access(dir, R_OK | W_OK))
    {
        error("[check_args] have no permission to write output (%s)", dir);
        return false;
    }

    if (output_format == NULL)
    {
        output_format = my_strdup("log", MYF(MY_WME));
    }
    else
    {
        if (!strcasecmp(output_format, "log"))
        {
            output_json = "";
            output_table_json = "";

            if (my_stat(output_log.c_str(), &my_file_stat, 0) &&
                remove(output_log.c_str()) != 0)
            {
                error("[check_args] Unable to remove the output file (%s)(%s)", output_log.c_str(), strerror(errno));
                return false;
            }
            if (my_stat(output_table_log.c_str(), &my_file_stat, 0) &&
                remove(output_table_log.c_str()) != 0)
            {
                error("[check_args] Unable to remove the output file (%s)(%s)", output_table_log.c_str(), strerror(errno));
                return false;
            }

            debug("[check_args] LOG: %s, TABLE LOG: %s", output_log.c_str(), output_table_log.c_str());
        }
        else if (!strcasecmp(output_format, "json"))
        {
            output_log = "";
            output_table_log = "";

            if (my_stat(output_json.c_str(), &my_file_stat, 0) &&
                remove(output_json.c_str()) != 0)
            {
                error("[check_args] Unable to remove the output file (%s)(%s)", output_json.c_str(), strerror(errno));
                return false;
            }

            if (my_stat(output_table_json.c_str(), &my_file_stat, 0) &&
                remove(output_table_json.c_str()) != 0)
            {
                error("[check_args] Unable to remove the output file (%s)(%s)", output_table_json.c_str(), strerror(errno));
                return false;
            }

            debug("[check_args] JSON: %s, TABLE JSON: %s", output_json.c_str(), output_table_json.c_str());
        }
        else
        {
            error("[check_args] invalid file format (%s)", output_format);
            return false;
        }
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
            // if binlog wasn't closed properly ("in use" flag is set) don't complain
            // about a corruption, but treat it as EOF and move to the next binlog.

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

bool ReadLogLow(StateTable::LogType type, const std::string &path)
{
    auto func = [&](char *ptr) -> std::shared_ptr<StateQuery>
    {
        auto data = StateTable::ReadLogBlock(ptr);

        // 상태 전환에서 사용한 쿼리는 제외
        if (StateTable::IsContinueDB(type, data) == true)
        {
            return NULL;
        }

        return data;
    };

    std::vector<std::future<std::shared_ptr<StateQuery>>> futures;

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0)
    {
        error("[ReadLogLow] open error (%d, %s)", errno, path.c_str());
        return false;
    }
    debug("[ReadLogLow] open : %s", path.c_str());

    struct stat s;
    fstat(fd, &s);

    off_t pagesize = getpagesize();
    off_t size = s.st_size;
    size += pagesize - (size % pagesize);

    char *ptr = (char *)mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);

    state_log_hdr *hdr = NULL;
    off_t curr = 0;
    while (curr < s.st_size)
    {
        hdr = (state_log_hdr *)(ptr + curr);

        if (start_time > hdr->start_time)
            goto conti;
        else if (end_time < hdr->start_time)
        {
            if (hdr->is_user_query == false)
                goto conti;
        }

        futures.emplace_back(StateSimpleThreadPool::Instance().EnqueueJob(func, ptr + curr));

    conti:
        curr += (sizeof(state_log_hdr) +
                 hdr->query_length +
                 hdr->read_table_length +
                 hdr->write_table_length +
                 hdr->foreign_length +
                 hdr->data_column_item_length +
                 hdr->where_column_item_length);
    }

    if (type == StateTable::EN_STATE_LOG_BIN)
    {
        log_list.reserve(futures.size());

        for (auto &f : futures)
        {
            auto data = f.get();
            if (data == NULL)
                continue;

            log_list.emplace_back(data);
        }
    }
    else
    {
        table_list.reserve(futures.size());

        for (auto &f : futures)
        {
            auto data = f.get();
            if (data == NULL)
                continue;

            table_list.emplace_back(data);
        }
    }

    munmap(ptr, size);
    close(fd);

    return true;
}

void SortQuery(StateTable::Query &q)
{
    std::sort(q->transactions.begin(), q->transactions.end(), [](const StateQuery::st_transaction &a, const StateQuery::st_transaction &b)
              { return a.time < b.time; });
}

void SortList(StateTable::QueryList &list)
{
    std::vector<std::future<void>> futures;
    for (auto &i : list)
    {
        futures.emplace_back(StateSimpleThreadPool::Instance().EnqueueJob(SortQuery, i));
    }

    for (auto &f : futures)
    {
        f.get();
    }

    std::sort(list.begin(), list.end(), [](const std::shared_ptr<StateQuery> &a, const std::shared_ptr<StateQuery> &b)
              { return a->transactions.front().time < b->transactions.front().time; });
}

bool ReadTableLog()
{
    table_list.clear();
    valid_table_list.clear();

    if (ReadLogLow(StateTable::EN_STATE_LOG_TABLE, state_log_table_filepath) == false)
    {
        error("[ReadTableLog] failed to read table file");
        return false;
    }

    SortList(table_list);

    return true;
}

bool ReadLog()
{
    log_list.clear();
    valid_log_list.clear();

    if (ReadLogLow(StateTable::EN_STATE_LOG_BIN, state_log_filepath) == false)
    {
        error("[ReadLog] failed to read state file");
        return false;
    }

    SortList(log_list);

    return true;
}

bool ValidLog()
{
    auto valid_proc_func = [&](long sec, ulong sec_part, uint64_t xid)
    {
        StateTable::MarkList(sec, sec_part, xid, table_list);
        StateTable::MarkList(sec, sec_part, xid, log_list);
    };

    for (auto &i : state_binary_log->GetList())
    {
        if (process_binary_log(i.filepath, valid_proc_func) == false)
        {
            error("[ValidLog] process_binary_log error");
            return false;
        }
    }

    StateTable::MoveMarkList(table_list, valid_table_list);

    // 최초 로그를 읽었을때 (시간으로 정렬 하기 전에) 트랜잭션으로 전환
    // 최초 로그는 트랜잭션 id 로 정렬되어 있음
    // -> 시간으로 정렬을 하면 id 가 섞임
    StateTable::MoveMarkListAndMakeTransaction(log_list, valid_log_list);

    debug("[ValidLog] table : %lu, valid : %lu", table_list.size(), valid_table_list.size());
    debug("[ValidLog] state : %lu, valid : %lu", log_list.size(), valid_log_list.size());

    // 기존 리스트 삭제
    table_list.clear();
    log_list.clear();

    return true;
}

void OutputJson(const StateTable::QueryList &QueryList, const std::string &filepath)
{
    char buffer[1024];
    std::ofstream fout;
    fout.open(filepath);
    size_t idx = 0;
    nlohmann::json j;
    for (auto &q : QueryList)
    {
        ++idx;

        nlohmann::json obj;
        obj["index"] = idx;
        obj["xid"] = q->xid;
        obj["is_user_query"] = q->user_query == EN_USER_QUERY_NONE ? "false" : "true";
        obj["transactions"] = nlohmann::json();
        for (auto &t : q->transactions)
        {
            snprintf(buffer, sizeof(buffer) - 1, "%s.%06lu",
                     StateUtil::format_time(t.time.sec, "%Y-%m-%d %H:%M:%S").c_str(),
                     t.time.sec_part);

            nlohmann::json t_obj;
            t_obj["time"] = buffer;
            t_obj["query"] = t.query;
            t_obj["readset"] = t.read_set;
            t_obj["writeset"] = t.write_set;
            t_obj["columns"] = nlohmann::json();

            for (auto &f : t.foreign_set)
            {
                std::vector<std::string> cols;
                for (auto &c : f.columns)
                {
                    if (c == "*")
                    {
                        cols.clear();
                        cols.emplace_back(f.table + ".*");
                        continue;
                    }
                    cols.emplace_back(f.table + "." + c);
                }
                if (cols.size() > 0)
                {
                    for (auto &c : cols)
                    {
                        nlohmann::json c_obj;
                        c_obj["access_type"] = f.access_type == en_table_access_read ? "read" : "write";
                        c_obj["name"] = c;
                        t_obj["columns"].emplace_back(c_obj);
                    }
                }
            }

            obj["transactions"].emplace_back(t_obj);
        }

        j.emplace_back(obj);
    }

    fout << std::setw(4) << j << std::endl;
    fout.close();
}

void OutputLog(const StateTable::QueryList &QueryList, const std::string &filepath)
{
    std::ofstream fout;
    fout.open(filepath);
    size_t idx = 0;
    for (auto &q : QueryList)
    {
        for (auto &t : q->transactions)
        {
            ++idx;
            fout << "[INDEX: " << idx << "] ";
            fout << "[XID: " << q->xid << "] ";
            fout << "[IS_USER: " << (q->user_query == EN_USER_QUERY_NONE ? "false] " : "true] ");
            fout << "[TIME: " << StateUtil::format_time(t.time.sec, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0') << std::setw(6) << t.time.sec_part << "] ";
            fout << "[QUERY: " << t.query << "]" << std::endl;
        }
    }

    fout.close();
}

int main(int argc, char **argv)
{
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

    StateSimpleThreadPool::Instance().Resize(std::thread::hardware_concurrency() + 1);
    // StateSimpleThreadPool::Instance().Resize(1);

    // 상태 전환 프로그램 동작 시점의 바이너리 로그 파일 목록 획득
    // 로그의 가장 마지막 시간을 end_time 으로 설정하고 추후 동기화 때 사용
    state_binary_log = new StateBinaryLog(binary_log_filepath, process_binary_log);
    if (end_time.sec == 0 && end_time.sec_part == 0)
    {
        auto binary_log_list = state_binary_log->GetList();
        end_time = binary_log_list.back().end;
    }

    // VIEW, TRIGGER, TABLE 목록 관련 로그 읽기
    debug("[main] Read state table log");
    if (ReadTableLog() == false)
    {
        error("[main] ReadTableLog error");
        goto err;
    }

    // 전체 쿼리 로그 읽기
    debug("[main] Read state log");
    if (ReadLog() == false)
    {
        fprintf(stderr, "[main] ReadLog error");
        goto err;
    }
    if (ValidLog() == false)
    {
        fprintf(stderr, "[main] ValidLog error");
        goto err;
    }

    // 쿼리 정보 출력
    if (output_log.size() > 0)
    {
        OutputLog(valid_log_list, output_log);
        OutputLog(valid_table_list, output_table_log);
    }
    else if (output_json.size() > 0)
    {
        OutputJson(valid_log_list, output_json);
        OutputJson(valid_table_list, output_table_json);
    }

    ret = EXIT_SUCCESS;
    goto exit;

err:
    ret = EXIT_FAILURE;

exit:
    debug("[main] Cleanup");

    StateSimpleThreadPool::Instance().Release();

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
