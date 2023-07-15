#include <mysql/mysql.h>
#include <mysql/mysqld_error.h>
/*
#include "my_global.h"
#include "sql_alloc.h"
#include "sql_cmd.h"
 */

#include "StateTable.h"
#include "StateQuery.hpp"
#include "StateGraph.h"
#include "StateUtil.h"
#include "StateBinaryLog.h"
#include "StateThreadPool.h"
// #include "state_log.h"


#include "utils/log.hpp"

#include "SQLParser.h"
#include "bison_parser.h"


#include <cmath>
#include <csignal>

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <sstream>
#include <algorithm>
#include <map>
#include <tuple>

namespace ultraverse::state {
    StateTable::StateTable(DBHandlePool<mariadb::DBHandle> &dbHandlePool, Query _user_transaction)
        : StateTable(dbHandlePool)
    {
        user_transaction = _user_transaction;
        UpdateReadUserTableList(user_transaction->read_set);
        UpdateWriteUserTableList(user_transaction->write_set);
        
        is_user_query2 = false;
    }
    
    StateTable::StateTable(DBHandlePool<mariadb::DBHandle> &dbHandlePool):
        group_id(-1), start_time({0, 0}), end_time({0, 0}), binary_log(NULL),
        
        _dbHandlePool(dbHandlePool),
        _logger(createLogger("StateTable"))
    {
        is_user_query2 = false;
    }
    
    StateTable::StateTable(const StateTable &table):
        _dbHandlePool(table._dbHandlePool)
    {
        this->start_time = table.start_time;
        this->end_time = table.end_time;
        
        this->binary_log = table.binary_log;
        
        this->user_table_list = table.user_table_list;
        this->read_user_table_list = table.read_user_table_list;
        this->write_user_table_list = table.write_user_table_list;
        
        this->valid_view_list = table.valid_view_list;
        this->valid_trigger_list = table.valid_trigger_list;
        this->valid_table_list = table.valid_table_list;
        this->valid_log_list = table.valid_log_list;
        
        this->rename_history = table.rename_history;
        this->related_table_map = table.related_table_map;
        
        this->is_user_query2 = table.is_user_query2;
    }
    
    StateTable::~StateTable(void) {
    }
    
    
    void StateTable::updateDefinitions() {
        clearDefinitions();
        _logger->info("updating data definitions...");
    
        // TODO: parallel process
        /*
        auto handle = _dbHandlePool.take();
        mariadb::BinaryLog binaryLog(handle.get());
        
        // read from beginning
        binaryLog.setStartPosition(4);
        binaryLog.open();
        
        while (binaryLog.next()) {
            auto event = binaryLog.currentEvent();
            auto queryEvent = std::dynamic_pointer_cast<mariadb::QueryEvent>(event);
            
            if (queryEvent == nullptr) {
                continue;
            }
            
            if (!queryEvent->tokenize()) {
                _logger->warn("failed to parse SQL statement");
                continue;
            } else if (!queryEvent->isDDL()) {
                continue;
            }
            
            auto command = queryEvent->tokens()[0];
            auto what = queryEvent->tokens()[1];
            
            auto query = std::make_shared<StateQuery>();
            query->user_query = EN_USER_QUERY_NONE;
            query->transactions.emplace_back(StateQuery::st_transaction(
                state_log_time(0, 0), // FIXME
                SQLCOM_DROP_DB, // FIXME
                queryEvent->statement()
            ));
            
            if (command == SQL_DROP && what == SQL_DATABASE) {
                _logger->debug("view_list.emplace_back({})", queryEvent->statement());
                _logger->debug("trigger_list.emplace_back({})", queryEvent->statement());
                _logger->debug("table_list.emplace_back({})", queryEvent->statement());
                view_list.emplace_back(query);
                trigger_list.emplace_back(query);
                table_list.emplace_back(query);
            } else if (
                (command == SQL_CREATE && what == SQL_VIEW) ||
                (command == SQL_DROP   && what == SQL_VIEW)
            ) {
                _logger->debug("view_list.emplace_back({})", queryEvent->statement());
                view_list.emplace_back(query);
            } else if (
                (command == SQL_CREATE && what == SQL_TRIGGER) ||
                (command == SQL_DROP   && what == SQL_TRIGGER)
            ) {
                _logger->debug("trigger_list.emplace_back({})", queryEvent->statement());
                trigger_list.emplace_back(query);
            } else if (
                (command == SQL_CREATE && what == SQL_PROCEDURE) ||
                (command == SQL_DROP   && what == SQL_PROCEDURE) ||
                (command == SQL_ALTER  && what == SQL_PROCEDURE)
            ) {
                _logger->info("TODO: procedure_list.append()");
            } else if (
                (command == SQL_CREATE  && what == SQL_TABLE) ||
                (command == SQL_DROP    && what == SQL_TABLE) ||
                (command == SQL_ALTER   && what == SQL_TABLE) ||
                (command == SQL_RENAME  && what == SQL_TABLE) ||
                (command == SQL_TRUNCATE) ||
                (command == SQL_CREATE  && what == SQL_INDEX) ||
                (command == SQL_DROP    && what == SQL_INDEX)
            ) {
                _logger->debug("table_list.emplace_back({})", queryEvent->statement());
                table_list.emplace_back(query);
            }
        }
    
        binaryLog.close();
        */
    }
    
    void StateTable::clearDefinitions() {
        _logger->debug("clearing data definitions...");
        
        procedure_list.clear();
        valid_procedure_list.clear();
    
        view_list.clear();
        valid_view_list.clear();
    
        trigger_list.clear();
        valid_trigger_list.clear();
    
        table_list.clear();
        valid_table_list.clear();
    
        before_undo_procedure_list.clear();
        before_undo_view_list.clear();
        before_undo_trigger_list.clear();
        before_undo_table_list.clear();
    }
    
    void StateTable::SetTime(const state_log_time &st_time, const state_log_time &ed_time) {
        start_time = st_time;
        end_time = ed_time;
    }
    
    void StateTable::SetBinaryLog(StateBinaryLog *log) {
        binary_log = log;
    }
    
    bool StateTable::SetGroupID(const int id) {
        group_id = id;
        
        unsigned char *data = NULL;
        size_t data_len = 0;
        
        /*
        if (0 != mysql_state_group_list(StateThreadPool::Instance().GetMySql()->handle().get(), group_id, &data, &data_len)) {
            error("[StateTable::SetGroupID] mysql_state_group_list error");
            return false;
        }
         */
        
        if (data == NULL || data_len < sizeof(st_state_group) || data_len % sizeof(st_state_group) != 0) {
            free(data);
            error("[StateTable::SetGroupID] invalid group data");
            return false;
        }
        
        st_state_group *grp = (st_state_group *) data;
        size_t count = data_len / sizeof(st_state_group);
        
        group_transaction = std::make_shared<StateQuery>();
        for (size_t i = 0; i < count; ++i) {
            group_transaction->xid = grp->trx_id;
            group_transaction->transactions.emplace_back(
                state_log_time(grp->time_sec, grp->time_usec),
                0,
                std::string()
            );
            ++grp;
        }
        free(data);
        
        SortQuery(group_transaction);
        
        return true;
    }
    
    void StateTable::ClearGroupID() {
        //TODO
        //상태전환 반복을 위해 취소된 GROUP ID 는 컬럼을 추가하여 따로 별도의 표시를 함
        //표시가 된 GROUP ID 와 입력된 GROUP ID 는 실행시 제외
        //GROUP ID 의 초기화는 주기적으로 적당히 오래된 GROUP ID 삭제
        
        auto mysql = StateThreadPool::Instance().GetMySql()->handle().get();
        
        std::stringstream query;
        
        query.str(std::string());
        query << STATE_QUERY_COMMENT_STRING " DELETE FROM " STATE_GROUP_DATABASE ".`" STATE_GROUP_TABLE
                 "` WHERE `group_id`="
              << group_id;
        
        if (mysql_query(mysql, query.str().c_str()) != 0) {
            if (ER_NO_SUCH_TABLE != mysql_errno(mysql)) {
                error("[StateTable::ClearGroupID] query error [%s] [%s]", query.str().c_str(), mysql_error(mysql));
            }
        }
        debug("[StateTable::ClearGroupID] %s", query.str().c_str());
        
        if (mysql_commit(mysql) != 0) {
            error("[StateTable::ClearGroupID] failed to commit [%s]", mysql_error(mysql));
        }
    }
    
    std::vector<StateQuery::st_transaction>::iterator BinaryFind(
        std::vector<StateQuery::st_transaction> &list,
        const state_log_time &t) {
        auto iter = std::lower_bound(list.begin(), list.end(), t,
                                     [](const StateQuery::st_transaction &s, const state_log_time &t) {
                                         return s.time < t;
                                     });
        return (iter != list.end() && iter->time == t) ? iter : list.end();
    }
    
    StateTable::QueryList::iterator BinaryFind(
        StateTable::QueryList::iterator begin,
        StateTable::QueryList::iterator end,
        const state_log_time &t) {
        auto iter = std::lower_bound(begin, end, t, [](const std::shared_ptr<StateQuery> &s, const state_log_time &t) {
            return s->transactions.front().time < t;
        });
        return (iter != end && (*iter)->transactions.front().time == t) ? iter : end;
    }
    
    void StateTable::RemoveGroupQuery(QueryList &list) {
        if (group_transaction == NULL) {
            return;
        }
        
        // group 쿼리들은 하나의 트랜잭션이기 때문에 트랜잭션 중 하나만 지워지는 경우는 없음
        // 트랜잭션중 하나라도 제거되면 모두 제거
        
        std::function<bool(StateQuery *)> is_continue_func;
        if (is_user_query2) {
            // user query2 는 여러 트랜잭션을 사용함
            is_continue_func = [&](StateQuery *query) -> bool {
                for (const auto &trx: group_list) {
                    if (trx->xid == query->xid) {
                        for (auto &i: query->transactions) {
                            auto iter = BinaryFind(trx->transactions, i.time);
                            
                            // 쿼리(트랜잭션)에 group 쿼리가 없음
                            if (iter == trx->transactions.end()) {
                                continue;
                            }
                                // 쿼리(트랜잭션)에 group 쿼리가 있음
                            else {
                                return false;
                            }
                        }
                    }
                }
                
                return true;
            };
        } else {
            is_continue_func = [&](StateQuery *query) -> bool {
                if (group_transaction->xid != query->xid)
                    return true;
                
                for (auto &i: query->transactions) {
                    auto iter = BinaryFind(group_transaction->transactions, i.time);
                    
                    // 쿼리(트랜잭션)에 group 쿼리가 없음
                    if (iter == group_transaction->transactions.end()) {
                        continue;
                    }
                        // 쿼리(트랜잭션)에 group 쿼리가 있음
                    else {
                        return false;
                    }
                }
                
                return true;
            };
        }
        
        // 삭제된 그룹 쿼리는 redo 에서 제외
        std::vector<std::future<bool>> futures;
        for (auto &i: list) {
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob(is_continue_func, i.get()));
        }
        
        size_t idx = 0;
        for (auto iter = list.begin(); iter != list.end();) {
            if (futures[idx++].get() == false) {
                iter = list.erase(iter);
            } else {
                ++iter;
            }
        }
    }
    
    bool StateTable::ReadTableLog(const std::string &log_table_filepath) {
        procedure_list.clear();
        valid_procedure_list.clear();
        
        view_list.clear();
        valid_view_list.clear();
        
        trigger_list.clear();
        valid_trigger_list.clear();
        
        table_list.clear();
        valid_table_list.clear();
        
        before_undo_procedure_list.clear();
        before_undo_view_list.clear();
        before_undo_trigger_list.clear();
        before_undo_table_list.clear();
        
        if (start_time.sec == 0) {
            error("[StateTable::ReadTableLog] invalid time : %ld.%lu", start_time.sec, start_time.sec_part);
            return false;
        }
        
        if (ReadLogLow(EN_STATE_LOG_TABLE, log_table_filepath) == false) {
            error("[StateTable::ReadTableLog] failed to read table file");
            return false;
        }
        
        SortList(procedure_list);
        SortList(view_list);
        SortList(trigger_list);
        SortList(table_list);
        
        return true;
    }
    
    bool StateTable::ReadGroupLogLow(const std::string &path) {
        auto func = [&](char *ptr) -> Query {
            auto hdr = (state_log_hdr *) ptr;
            
            if (group_transaction->xid != hdr->xid)
                return NULL;
            
            auto iter = BinaryFind(group_transaction->transactions, hdr->start_time);
            if (iter == group_transaction->transactions.end())
                return NULL;
            
            auto data = ReadLogBlock(ptr);
            
            auto &trans = data->transactions.front();
            if (trans.query.find(STATE_GROUP_COMMENT_STRING) != std::string::npos)
                return NULL;
            
            if (trans.query.find(STATE_QUERY_COMMENT_STRING) != std::string::npos)
                return NULL;
            
            if (trans.command == SQLCOM_SET_OPTION && trans.query.find("TIMESTAMP") != std::string::npos)
                return NULL;
            
            return data;
        };
        
        std::vector<std::future<Query>> futures;
        
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            error("[StateTable::ReadGroupLogLow] open error (%d, %s)", errno, path.c_str());
            return false;
        }
        debug("[StateTable::ReadGroupLogLow] open : %s", path.c_str());
        
        struct stat s;
        fstat(fd, &s);
        
        off_t pagesize = getpagesize();
        off_t size = s.st_size;
        size += pagesize - (size % pagesize);
        
        char *ptr = (char *) mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
        
        auto tmp_start_time = group_transaction->transactions.front().time;
        auto tmp_end_time = group_transaction->transactions.back().time;
        
        state_log_hdr *hdr = NULL;
        off_t curr = 0;
        while (curr < s.st_size) {
            hdr = (state_log_hdr *) (ptr + curr);
            
            // start_time 부터 파일 end_time 까지
            if (tmp_start_time > hdr->start_time)
                goto conti;
            else if (tmp_end_time < hdr->start_time) {
                goto conti;
            }
            
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob(func, ptr + curr));
            
            conti:
            curr += (sizeof(state_log_hdr) +
                     hdr->query_length +
                     hdr->read_table_length +
                     hdr->write_table_length +
                     hdr->foreign_length +
                     hdr->data_column_item_length +
                     hdr->where_column_item_length);
        }
        
        for (auto &f: futures) {
            auto q = f.get();
            if (q != NULL)
                group_list.emplace_back(q);
        }
        
        munmap(ptr, size);
        close(fd);
        
        return true;
    }
    
    std::tuple<bool, StateTable::QueryList>
    StateTable::ReadUserDelLog(const std::string &log_bin_filepath, std::shared_ptr<StateQuery> user_del_transaction) {
        is_user_query2 = true;
        
        group_list.clear();
        valid_group_list.clear();
        
        auto res = std::make_tuple(false, group_list);
        
        if (user_del_transaction->transactions.size() == 0) {
            std::get<0>(res) = true;
            return res;
        }
        
        auto func = [&](char *ptr) -> Query {
            auto hdr = (state_log_hdr *) ptr;
            
            auto iter = BinaryFind(user_del_transaction->transactions, hdr->start_time);
            if (iter == user_del_transaction->transactions.end())
                return NULL;
            
            auto data = ReadLogBlock(ptr);
            
            auto &trans = data->transactions.front();
            if (trans.query.find(STATE_GROUP_COMMENT_STRING) != std::string::npos)
                return NULL;
            
            if (trans.query.find(STATE_QUERY_COMMENT_STRING) != std::string::npos)
                return NULL;
            
            if (trans.command == SQLCOM_SET_OPTION && trans.query.find("TIMESTAMP") != std::string::npos)
                return NULL;
            
            return data;
        };
        
        std::vector<std::future<Query>> futures;
        
        int fd = open(log_bin_filepath.c_str(), O_RDONLY);
        if (fd < 0) {
            error("[StateTable::ReadUserDelLog] open error (%d, %s)", errno, log_bin_filepath.c_str());
            std::get<0>(res) = false;
            return res;
        }
        debug("[StateTable::ReadUserDelLog] open : %s", log_bin_filepath.c_str());
        
        struct stat s;
        fstat(fd, &s);
        
        off_t pagesize = getpagesize();
        off_t size = s.st_size;
        size += pagesize - (size % pagesize);
        
        char *ptr = (char *) mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
        
        auto tmp_start_time = user_del_transaction->transactions.front().time;
        auto tmp_end_time = user_del_transaction->transactions.back().time;
        
        state_log_hdr *hdr = NULL;
        off_t curr = 0;
        while (curr < s.st_size) {
            hdr = (state_log_hdr *) (ptr + curr);
            
            // start_time 부터 파일 end_time 까지
            if (tmp_start_time > hdr->start_time)
                goto conti;
            else if (tmp_end_time < hdr->start_time) {
                goto conti;
            }
            
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob(func, ptr + curr));
            
            conti:
            curr += (sizeof(state_log_hdr) +
                     hdr->query_length +
                     hdr->read_table_length +
                     hdr->write_table_length +
                     hdr->foreign_length +
                     hdr->data_column_item_length +
                     hdr->where_column_item_length);
        }
        
        for (auto &f: futures) {
            auto q = f.get();
            if (q != NULL) {
                StateUserQuery::MergeQuery(q);
                group_list.emplace_back(q);
            }
        }
        
        munmap(ptr, size);
        close(fd);
        
        if (group_list.size() == 0) {
            error("[StateTable::ReadUserDelLog] query not found");
            std::get<0>(res) = false;
            return res;
        }
        
        if (user_del_transaction->transactions.size() > group_list.size()) {
            error("[StateTable::ReadUserDelLog] transaction count miss match (%d, %d)",
                  user_del_transaction->transactions.size(),
                  group_list.size());
            std::get<0>(res) = false;
            return res;
        }
        
        group_transaction = user_del_transaction;
        
        std::get<0>(res) = true;
        std::get<1>(res) = group_list;
        return res;
    }
    
    bool StateTable::ReadGroupLog(const std::string &log_bin_filepath) {
        group_list.clear();
        valid_group_list.clear();
        
        // STATE LOG 에서 group id 에 해당하는 시간의 쿼리 로그 읽기
        if (ReadGroupLogLow(log_bin_filepath) == false) {
            error("[StateTable::ReadGroupLog] ReadGroupLogLow error");
            return false;
        }
        
        if (group_list.size() == 0) {
            error("[StateTable::ReadGroupLog] No query about group id (%d)", group_id);
            return false;
        }
        
        if (group_transaction->transactions.size() != group_list.size()) {
            error("[StateTable::ReadGroupLog] group transaction count miss match (%d, %d)",
                  group_transaction->transactions.size(),
                  group_list.size());
            return false;
        }
        
        SortList(group_list);
        
        //start_time 재설정
        start_time = group_list.front()->transactions.front().time;
        --start_time;
        
        throw std::runtime_error("fatal: reimplement get_start_time()");
        /*
        _context.get_start_time().sec = start_time.sec;
        _context.get_start_time().sec_part = start_time.sec_part;
         */
        
        if (user_transaction != NULL) {
            // user 쿼리는 사용자가 지정한 시간으로 동작하지만
            // 그룹 쿼리와 user 쿼리가 동시에 있는 경우
            // 그룹 쿼리의 시간으로 설정
            for (auto &t: user_transaction->transactions) {
                t.time = start_time;
            }
        }
        
        return true;
    }
    
    bool StateTable::ReadLog(const std::string &log_bin_filepath) {
        log_list.clear();
        valid_log_list.clear();
        
        if (ReadLogLow(EN_STATE_LOG_BIN, log_bin_filepath) == false) {
            error("[StateTable::ReadLog] failed to read state file");
            return false;
        }
        
        SortList(log_list);
        
        return true;
    }
    
    bool StateTable::IsContinueDB(LogType type, const std::shared_ptr<StateQuery> &data) {
        struct find_db {
            find_db() {}
            
            bool operator()(const std::string &s) const {
                std::string dbname;
                std::string tablename;
                
                if (false == StateUserQuery::SplitDBNameAndTableName(s, dbname, tablename))
                    return false;
                
                if (StateUtil::iequals(dbname, STATE_BACKUP_DATABASE))
                    return true;
                else if (StateUtil::iequals(dbname, STATE_CHANGE_DATABASE))
                    return true;
                else
                    return false;
            }
        };
        
        if (std::find_if(data->read_set.begin(), data->read_set.end(), find_db()) != data->read_set.end())
            return true;
        
        if (std::find_if(data->write_set.begin(), data->write_set.end(), find_db()) != data->write_set.end())
            return true;
        
        if (std::string::npos != data->transactions.front().query.find(STATE_QUERY_COMMENT_STRING))
            return true;
        
        return false;
    }
    
    /**
     * @deprecated
     */
    std::vector<StateTable::QueryList *> StateTable::GetMatchList(const LogType &type, const uint16_t command) {
        std::vector<QueryList *> vec;
        
        switch (command) {
            case SQLCOM_DROP_DB:
                vec.push_back(&view_list);
                vec.push_back(&trigger_list);
                vec.push_back(&table_list);
                return vec;
            
            case SQLCOM_CREATE_VIEW:
            case SQLCOM_DROP_VIEW:
                vec.push_back(&view_list);
                return vec;
            
            case SQLCOM_CREATE_TRIGGER:
            case SQLCOM_DROP_TRIGGER:
                vec.push_back(&trigger_list);
                return vec;
            
            case SQLCOM_CREATE_PROCEDURE:
            case SQLCOM_DROP_PROCEDURE:
            case SQLCOM_ALTER_PROCEDURE:
                vec.push_back(&procedure_list);
                return vec;
            
            case SQLCOM_ALTER_TABLE:
            case SQLCOM_CREATE_TABLE:
            case SQLCOM_DROP_TABLE:
            case SQLCOM_TRUNCATE:
            case SQLCOM_RENAME_TABLE:
            case SQLCOM_CREATE_INDEX:
            case SQLCOM_DROP_INDEX:
                vec.push_back(&table_list);
                return vec;
            
            default:
                return vec;
        }
    }
    
    void StateTable::FillFromLogBlock(const char *ptr, off_t curr, size_t data_size, std::vector<std::string> &set) {
        std::string buff;
        size_t len = 0;
        
        while (len < data_size) {
            if (isspace(ptr[curr])) {
                if (buff.size() > 0) {
                    set.push_back(buff);
                    buff.clear();
                }
            } else {
                buff.push_back(ptr[curr]);
            }
            
            ++curr;
            ++len;
        }
        
        if (buff.size() > 0) {
            set.push_back(buff);
        }
    }
    
    int read_column_item_field(StateTaskContext &_context, const char *ptr, size_t &curr, StateItem &col, size_t arg_count);
    
    int read_column_item_condition(StateTaskContext &_context, const char *ptr, size_t &curr, StateItem &col);
    
    int read_column_item_function(StateTaskContext &_context, const char *ptr, size_t &curr, StateItem &col);
    
    int read_column_item(StateTaskContext &_context, const char *ptr, size_t &curr, StateItem &col, uint8_t &data_count);
    
    inline bool is_column_item_arg_ok(const StateItem *arg) {
        return (arg->arg_list.size() == 0) ? false : true;
    }
    
    inline bool is_column_item_data_ok(const StateItem *arg) {
        return (arg->data_list.size() == 0 || arg->name.size() == 0) ? false : true;
    }
    
    int read_column_item_field(StateTaskContext &_context, const char *ptr, size_t &curr, StateItem &col, size_t arg_count) {
        int total_count = 0;
        
        state_log_column_item *item = (state_log_column_item *) (ptr + curr);
        curr += (state_log_column_item_length + item->data_length);
        total_count += 1;
        
        assert(item->item_type == en_column_item_field);
        
        col.name.assign((char *) &item->data, item->data_length);
        // debug("[read_column_item_field] col_name : %s", col.name.c_str());
        
        std::string col_name = col.name;
        {
            auto pos = col_name.find_last_of('.');
            col_name = col_name.substr(pos + 1);
        }
        std::string key_name = "";
        if (_context.get_key_column_name() != NULL) {
            key_name = _context.get_key_column_name();
            auto pos = key_name.find_last_of('.');
            key_name = key_name.substr(pos + 1);
        }
        
        if (_context.is_using_candidate() == false && col_name != key_name) {
            // 설정한 키 이름과 다르면 검사 대상에 해당하지 않음
            col.name.clear();
        }
        
        // 위의 필드 데이터가 arg_count 1 에 해당됨
        for (size_t i = 1; i < arg_count; ++i) {
            state_log_column_item *sub_item = (state_log_column_item *) (ptr + curr);
            
            switch (item->item_type) {
                
                case en_column_item_cond: {
                    col.arg_list.emplace_back();
                    auto *arg = &col.arg_list.back();
                    
                    int ret = read_column_item_condition(_context, ptr, curr, *arg);
                    if (ret < 0) {
                        return -1;
                    }
                    total_count += ret;
                    
                    if (!is_column_item_arg_ok(arg)) {
                        col.arg_list.pop_back();
                    }
                    
                    break;
                }
                
                case en_column_item_func: {
                    col.arg_list.emplace_back();
                    auto *arg = &col.arg_list.back();
                    
                    int ret = read_column_item_function(_context, ptr, curr, *arg);
                    if (ret < 0) {
                        return -1;
                    }
                    total_count += ret;
                    
                    if (!is_column_item_arg_ok(arg)) {
                        col.arg_list.pop_back();
                    }
                    
                    break;
                }
                
                case en_column_item_field:
                case en_column_item_int:
                case en_column_item_date:
                case en_column_item_real:
                case en_column_item_deci:
                case en_column_item_str:
                case en_column_item_nul: {
                    col.data_list.emplace_back();
                    auto *data = &col.data_list.back();
                    
                    curr += (state_log_column_item_length + sub_item->data_length);
                    total_count += 1;
                    
                    if (false == data->SetData(sub_item->sub_type.data, &sub_item->data, sub_item->data_length)) {
                        col.data_list.pop_back();
                    }
                        
                        //키에 맞게 데이터 형변환
                    else if (false == _context.is_using_candidate() && false == data->ConvertData(_context.get_key_column_type())) {
                        col.data_list.pop_back();
                    }
                    
                    if (data->IsSubSelect()) {
                        col.sub_query_list.emplace_back();
                        auto *sub_col = &col.sub_query_list.back();
                        
                        uint8_t tmp = 0;
                        int ret = read_column_item(_context, ptr, curr, *sub_col, tmp);
                        total_count += tmp;
                        if (ret < 0) {
                            col.sub_query_list.pop_back();
                        }
                    }
                    
                    break;
                }
                
                default:
                    break;
            }
        }
        
        return total_count;
    }
    
    int read_column_item_condition(StateTaskContext &_context, const char *ptr, size_t &curr, StateItem &col) {
        int total_count = 0;
        
        state_log_column_item *item = (state_log_column_item *) (ptr + curr);
        curr += state_log_column_item_length + item->data_length;
        total_count += 1;
        
        assert(item->item_type == en_column_item_cond);
        
        if (item->sub_type.cond == en_column_cond_and) {
            // debug("[read_column_item_condition] EN_CONDITION_AND");
            col.condition_type = EN_CONDITION_AND;
        } else if (item->sub_type.cond == en_column_cond_or) {
            // debug("[read_column_item_condition] EN_CONDITION_OR");
            col.condition_type = EN_CONDITION_OR;
        } else {
            error("[read_column_item_condition] func_type parsing error");
            return -1;
        }
        
        for (size_t i = 0; i < item->arg_count; ++i) {
            col.arg_list.emplace_back();
            auto *arg = &col.arg_list.back();
            
            state_log_column_item *sub_item = (state_log_column_item *) (ptr + curr);
            
            if (sub_item->item_type == en_column_item_cond) {
                int ret = read_column_item_condition(_context, ptr, curr, *arg);
                if (ret < 0) {
                    return -1;
                }
                total_count += ret;
            } else if (sub_item->item_type == en_column_item_func) {
                int ret = read_column_item_function(_context, ptr, curr, *arg);
                if (ret < 0) {
                    return -1;
                }
                total_count += ret;
            } else {
                error("[read_column_item_condition] item_type parsing error");
                return -1;
            }
            
            if (!is_column_item_arg_ok(arg)) {
                col.arg_list.pop_back();
            }
        }
        
        return total_count;
    }
    
    int read_column_item_function(StateTaskContext &_context, const char *ptr, size_t &curr, StateItem &col) {
        int total_count = 0;
        
        state_log_column_item *item = (state_log_column_item *) (ptr + curr);
        curr += state_log_column_item_length + item->data_length;
        total_count += 1;
        
        assert(item->item_type == en_column_item_func);
        
        switch (item->sub_type.func) {
            case en_column_func_eq:
            case en_column_func_equal:
                col.function_type = FUNCTION_EQ;
                // debug("[read_column_item_function] FUNCTION_EQ");
                break;
            case en_column_func_ne:
                // debug("[read_column_item_function] FUNCTION_NE");
                col.function_type = FUNCTION_NE;
                break;
            case en_column_func_lt:
                // debug("[read_column_item_function] FUNCTION_LT");
                col.function_type = FUNCTION_LT;
                break;
            case en_column_func_le:
                // debug("[read_column_item_function] FUNCTION_LE");
                col.function_type = FUNCTION_LE;
                break;
            case en_column_func_gt:
                // debug("[read_column_item_function] FUNCTION_GT");
                col.function_type = FUNCTION_GT;
                break;
            case en_column_func_ge:
                // debug("[read_column_item_function] FUNCTION_GE");
                col.function_type = FUNCTION_GE;
                break;
            case en_column_func_between:
                // debug("[read_column_item_function] FUNCTION_BETWEEN");
                col.function_type = FUNCTION_BETWEEN;
                break;
            default:
                error("[read_column_item_function] func_type parsing error");
                return -1;
        }
        
        col.arg_list.emplace_back();
        auto *arg = &col.arg_list.back();
        
        state_log_column_item *sub_item = (state_log_column_item *) (ptr + curr);
        
        if (sub_item->item_type == en_column_item_field) {
            int ret = read_column_item_field(_context, ptr, curr, *arg, item->arg_count);
            if (ret < 0) {
                return -1;
            }
            total_count += ret;
            
            if (arg->sub_query_list.size() > 0) {
                col.sub_query_list.insert(
                    col.sub_query_list.end(),
                    arg->sub_query_list.begin(),
                    arg->sub_query_list.end());
                
                arg->sub_query_list.clear();
            }
            
            if (is_column_item_data_ok(arg)) {
                // update_column_item_arg(col, arg->name);
            } else if (is_column_item_arg_ok(arg) && arg->name.size() > 0) {
                // update_column_item_arg(col, arg->name);
            } else {
                // data type 이 en_column_data_null 인 경우
                // 필드 아이템이 참고할 데이터가 없으면 삭제
                col.arg_list.pop_back();
            }
        }
            // else if (sub_item->item_type == en_column_item_cond)
            // {
            //   int ret = read_column_item_condition(ptr, curr, *arg);
            //   if (ret < 0)
            //   {
            //     return -1;
            //   }
            //   total_count += ret;
            
            //   if (!is_column_item_arg_ok(arg))
            //   {
            //   }
            // }
        else {
            error("[read_column_item_function] item_type parsing error");
            return -1;
        }
        
        return total_count;
    }
    
    int read_column_item(StateTaskContext &_context, const char *ptr, size_t &curr, StateItem &col, uint8_t &data_count) {
        state_log_column_item *item = (state_log_column_item *) (ptr + curr);
        
        switch (item->item_type) {
            
            case en_column_item_cond: {
                int ret = read_column_item_condition(_context, ptr, curr, col);
                if (ret < 0) {
                    return -1;
                }
                data_count += ret;
                
                if (is_column_item_arg_ok(&col) == false) {
                    //아이템이 arg 정보가 없다면 사용할 수 없음
                    return -1;
                }
                
                return 0;
            }
            
            case en_column_item_func: {
                int ret = read_column_item_function(_context, ptr, curr, col);
                if (ret < 0) {
                    return -1;
                }
                data_count += ret;
                
                if (is_column_item_arg_ok(&col) == false) {
                    //아이템이 arg 정보가 없다면 사용할 수 없음
                    return -1;
                }
                
                return 0;
            }
            
            case en_column_item_field: {
                int ret = read_column_item_field(_context, ptr, curr, col, item->arg_count + 1);
                if (ret < 0) {
                    return -1;
                }
                data_count += ret;
                
                if (is_column_item_data_ok(&col)) {
                    // update_column_item_arg(col, col.name);
                } else if (is_column_item_arg_ok(&col) && col.name.size() > 0) {
                    // update_column_item_arg(col, col.name);
                } else {
                    //Field 아이템이 data 정보가 없다면 사용할 수 없음
                    return -1;
                }
                
                return 0;
            }
            
            default: {
                data_count += 1;
                
                error("[read_column_item] item_type parsing error");
                return -1;
            }
        }
    }
    
    StateTable::Query StateTable::ReadLogBlock(const char *ptr) {
        state_log_hdr *hdr = (state_log_hdr *) ptr;
        size_t curr = sizeof(state_log_hdr);
        
        auto data = std::make_shared<StateQuery>();
        
        data->user_query = hdr->is_user_query > 0 ? EN_USER_QUERY_LOG : EN_USER_QUERY_NONE;
        data->xid = hdr->xid;
        
        data->transactions.emplace_back(
            StateQuery::st_transaction(hdr->start_time,
                                       hdr->sql_command,
                                       std::string(ptr + curr, hdr->query_length)));
        
        curr += hdr->query_length;
        
        FillFromLogBlock(ptr, curr, hdr->read_table_length, data->read_set);
        StateUtil::unique_vector(data->read_set);
        curr += hdr->read_table_length;
        
        FillFromLogBlock(ptr, curr, hdr->write_table_length, data->write_set);
        StateUtil::unique_vector(data->write_set);
        curr += hdr->write_table_length;
        
        for (uint8_t i = 0; i < hdr->foreign_count; ++i) {
            state_log_hdr_foreign *foreign_hdr = (state_log_hdr_foreign *) (ptr + curr);
            
            data->foreign_set.emplace_back(
                foreign_hdr->table_type,
                foreign_hdr->update_type,
                foreign_hdr->delete_type,
                foreign_hdr->access_type,
                
                ptr + curr + sizeof(state_log_hdr_foreign),
                foreign_hdr->foreign_name_length,
                
                ptr + curr + sizeof(state_log_hdr_foreign) + foreign_hdr->foreign_name_length,
                foreign_hdr->foreign_table_length,
                
                ptr + curr + sizeof(state_log_hdr_foreign) + foreign_hdr->foreign_name_length +
                foreign_hdr->foreign_table_length,
                foreign_hdr->columns_length,
                
                ptr + curr + sizeof(state_log_hdr_foreign) + foreign_hdr->foreign_name_length +
                foreign_hdr->foreign_table_length + foreign_hdr->columns_length,
                foreign_hdr->foreign_columns_length);
            
            curr += (sizeof(state_log_hdr_foreign) + foreign_hdr->foreign_name_length +
                     foreign_hdr->foreign_table_length + foreign_hdr->columns_length +
                     foreign_hdr->foreign_columns_length);
        }
        
        // debug("[StateTable::ReadLogBlock] %s", data->query.c_str());
        
        // candidate 탐색 모드이거나 설정한 키가 있을때만 사용
        throw std::runtime_error("fixme: repimplement ReadLogBlock()");
        /*
        if (_context.is_using_candidate() || _context.get_key_column_name() != NULL) {
            std::vector<StateItem> item_set;
            std::vector<StateItem> where_set;
            
            for (uint8_t i = 0; i < hdr->data_column_count;) {
                item_set.emplace_back();
                auto *col = &item_set.back();
                
                // debug("[StateTable::ReadLogBlock] DATA COLUMN...");
                int ret = read_column_item(ptr, curr, *col, i);
                if (ret < 0) {
                    item_set.pop_back();
                }
            }
            
            for (uint8_t i = 0; i < hdr->where_column_count;) {
                where_set.emplace_back();
                auto *col = &where_set.back();
                
                // debug("[StateTable::ReadLogBlock] WHERE COLUMN...");
                int ret = read_column_item(ptr, curr, *col, i);
                if (ret < 0) {
                    where_set.pop_back();
                }
                
                for (auto &i: col->sub_query_list) {
                    where_set.emplace_back(i);
                }
                col->sub_query_list.clear();
            }
            
            switch (hdr->sql_command) {
                case SQLCOM_SELECT:
                case SQLCOM_DELETE:
                case SQLCOM_DELETE_MULTI:
                    data->transactions.back().where_set = where_set;
                    
                    for (auto &i: where_set) {
                        data->range.emplace_back(i.MakeRange());
                    }
                    data->range = StateRange::OR_ARRANGE(data->range);
                    break;
                
                case SQLCOM_INSERT:
                case SQLCOM_INSERT_SELECT:
                case SQLCOM_UPDATE:
                case SQLCOM_UPDATE_MULTI:
                    data->transactions.back().item_set = item_set;
                    data->transactions.back().where_set = where_set;
                    
                    for (auto &i: item_set) {
                        data->range.emplace_back(i.MakeRange());
                    }
                    for (auto &i: where_set) {
                        data->range.emplace_back(i.MakeRange());
                    }
                    data->range = StateRange::OR_ARRANGE(data->range);
                    break;
                
                default:
                    break;
            }
        }
         */
        
        StateUserQuery::MergeTableSet(data->read_set, data->write_set);
        StateForeign::MergeList(data->foreign_set);
        
        data->transactions.back().read_set = data->read_set;
        data->transactions.back().write_set = data->write_set;
        data->transactions.back().foreign_set = data->foreign_set;
        data->transactions.back().range = data->range;
        
        data->read_set.clear();
        data->write_set.clear();
        data->foreign_set.clear();
        data->range.clear();
        
        return data;
    }
    
    bool StateTable::ReadLogLow(LogType type, const std::string &path) {
#if 0
        // FOR DEBUG
  auto thread_count = StateThreadPool::Instance().Size();
  StateThreadPool::Instance().Release();
  StateThreadPool::Instance().Resize(1);
#endif
        
        auto func = [&](char *ptr) -> std::shared_ptr<StateQuery> {
            auto data = ReadLogBlock(ptr);
            
            // 상태 전환에서 사용한 쿼리는 제외
            if (IsContinueDB(type, data) == true) {
                return NULL;
            }
            
            return data;
        };
        
        std::vector<std::future<std::shared_ptr<StateQuery>>> futures;
        
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            error("[StateTable::ReadLogLow] open error (%d, %s)", errno, path.c_str());
            return false;
        }
        debug("[StateTable::ReadLogLow] open : %s", path.c_str());
        
        struct stat s;
        fstat(fd, &s);
        
        off_t pagesize = getpagesize();
        off_t size = s.st_size;
        size += pagesize - (size % pagesize);
        
        char *ptr = (char *) mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
        
        state_log_hdr *hdr = NULL;
        off_t curr = 0;
        while (curr < s.st_size) {
            hdr = (state_log_hdr *) (ptr + curr);
            
            // LOG 는 undo 부터 파일 end_time 까지
            if (type == EN_STATE_LOG_BIN) {
                if (start_time > hdr->start_time)
                    goto conti;
                else if (end_time < hdr->start_time) {
                    if (hdr->is_user_query == false)
                        goto conti;
                }
            }
            
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob(func, ptr + curr));
            
            conti:
            curr += (sizeof(state_log_hdr) +
                     hdr->query_length +
                     hdr->read_table_length +
                     hdr->write_table_length +
                     hdr->foreign_length +
                     hdr->data_column_item_length +
                     hdr->where_column_item_length);
        }
        
        if (type == EN_STATE_LOG_BIN) {
            log_list.reserve(futures.size());
            
            for (auto &f: futures) {
                auto data = f.get();
                if (data == NULL)
                    continue;
                
                log_list.emplace_back(data);
            }
        } else {
            for (auto &f: futures) {
                auto data = f.get();
                if (data == NULL)
                    continue;
                
                for (auto &list: GetMatchList(type, data->transactions.front().command)) {
                    list->emplace_back(data);
                }
            }
        }
        
        munmap(ptr, size);
        close(fd);

#if 0
        // FOR DEBUG
  StateThreadPool::Instance().Resize(thread_count);
#endif
        
        return true;
    }

#if 0
    StateTable::QueryList::iterator StateTable::BinaryFindList(QueryList::iterator begin, QueryList::iterator end, const state_log_time &t)
{
  auto find_func = [](const std::shared_ptr<StateQuery> &s, const state_log_time &t) {
    return s->time < t;
  };

  auto iter = std::lower_bound(begin, end, t, find_func);
  return (iter != end && (*iter)->time == t) ? iter : end;
}

StateTable::QueryList::iterator StateTable::BinaryFindList(QueryList &list, const state_log_time &t)
{
  return BinaryFindList(list.begin(), list.end(), t);
}
#endif
    
    void StateTable::SortQuery(Query &q) {
        std::sort(q->transactions.begin(), q->transactions.end(),
                  [](const StateQuery::st_transaction &a, const StateQuery::st_transaction &b) {
                      return a.time < b.time;
                  });
    }
    
    void StateTable::SortList(QueryList &list) {
        std::vector<std::future<void>> futures;
        for (auto &i: list) {
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob(SortQuery, i));
        }
        
        for (auto &f: futures) {
            f.get();
        }
        
        std::sort(list.begin(), list.end(),
                  [](const std::shared_ptr<StateQuery> &a, const std::shared_ptr<StateQuery> &b) {
                      return a->transactions.front().time < b->transactions.front().time;
                  });
    }
    
    void StateTable::MarkList(long sec, ulong sec_part, uint64_t xid, QueryList &list) {
        state_log_time t = {sec, sec_part};
        
        auto iter = BinaryFind(list.begin(), list.end(), t);
        while (iter != list.end()) {
            if (xid == (*iter)->xid) {
                (*iter)->is_valid_query = 1;
            }
            iter = BinaryFind(iter + 1, list.end(), t);
        }
    }
    
    void StateTable::MoveMarkList(QueryList &from_list, QueryList &to_list) {
        to_list.clear();
        to_list.reserve(from_list.size());
        for (auto &i: from_list) {
            if (i->is_valid_query == 1) {
                i->is_valid_query = 0;
                to_list.emplace_back(i);
            } else if (i->transactions[0].command == SQLCOM_SET_OPTION) {
                to_list.emplace_back(i);
            }
        }
    }
    
    void StateTable::MoveMarkListAndMakeTransaction(QueryList &from_list, QueryList &to_list) {
        // 최초에 Binary Log 에서 읽은 쿼리를 트랜잭션 단위로 합침
        std::shared_ptr<StateQuery> curr_query = NULL;
        auto merge = [&curr_query](std::shared_ptr<StateQuery> iter) {
            if (curr_query != NULL) {
                if (iter->xid == curr_query->xid) {
                    // 트랜잭션 id 가 같으면 쿼리를 합치고 삭제
                    StateUserQuery::AddQuery(curr_query, iter);
                    // iter = list.erase(iter);
                    return true;
                } else {
                    // 트랜잭션 id 가 다르면 다음 트랜잭션임
                    StateUserQuery::MergeQuery(curr_query);
                    curr_query = iter;
                    // ++iter;
                }
            } else {
                curr_query = iter;
            }
            
            return false;
        };
        
        to_list.clear();
        to_list.reserve(from_list.size());
        for (auto &i: from_list) {
            if (i->is_valid_query == 1) {
                i->is_valid_query = 0;
                if (merge(i) == false) {
                    to_list.emplace_back(i);
                }
            } else if (i->transactions[0].command == SQLCOM_SET_OPTION) {
                if (merge(i) == false) {
                    to_list.emplace_back(i);
                }
            }
        }
    }
    
    size_t StateTable::GetQueries(std::function<size_t(StateQuery *)> func) {
        
        size_t ret = 0;
        for (auto &i: valid_log_list) {
            ret += func(i.get());
        }
        
        return ret;
    }
    
    StateTable::QueryList &StateTable::GetValidQueryList() {
        return valid_log_list;
    }
    
    size_t StateTable::RunQueries() {
        return GetQueries(StateUserQuery::RunQuery);
    }
    
    void StateTable::AddQueries(StateGraph *graph) {
        if (user_transaction != NULL) {
            debug("[StateTable::AddQueries] Add User Query");
            if (is_user_query2) {
                // query2 는 추가된 쿼리의 시간이 서로 다름 -> 시간이 다르면 서로 다른 트랜잭션으로 취급해야 함
                auto new_user_transaction = StateUserQuery::SplitQuery(user_transaction);
                valid_log_list.insert(valid_log_list.begin(), new_user_transaction.begin(), new_user_transaction.end());
            } else {
                valid_log_list.insert(valid_log_list.begin(), user_transaction);
            }
            
            SortList(valid_log_list);
        }
        
        debug("[StateTable::AddQueries] Add Redo Query %d", valid_log_list.size());
        // graph->AddQueries(valid_log_list);
    }
    
    std::string StateTable::GetDbNameFromDropDbQuery(const std::string &query) {
        std::vector<int16_t> tokens;
        std::vector<size_t> token_pos;
        if (hsql::SQLParser::tokenize(query, &tokens, &token_pos) == false) {
            error("[StateTable::GetDbNameFromDropDbQuery] tokenize failed [%s]",
                  query.c_str());
            return std::string();
        }
        
        if (tokens.size() < 3) {
            error("[StateTable::GetDbNameFromDropDbQuery] unknown query failed [%s]",
                  query.c_str());
            return std::string();
        }
        
        static const std::vector<int16_t> query_valid_drop_token = {SQL_DROP, SQL_DATABASE};
        auto it = std::search(tokens.begin(), tokens.end(),
                              query_valid_drop_token.begin(),
                              query_valid_drop_token.end());
        
        if (it != tokens.begin()) {
            error("[StateTable::GetDbNameFromDropDbQuery] unknown query failed [%s]",
                  query.c_str());
            return std::string();
        }
        
        std::string db = query.substr(token_pos.back());
        if (db.front() == '`' && db.back() == '`') {
            db = db.substr(1, db.size() - 2);
        }
        
        return db;
    }
    
    void StateTable::RemoveList(QueryList &list, int create_command, int drop_command) {
        auto forward_for = [&list, &drop_command]() {
            for (auto iter = list.begin(); iter != list.end(); ++iter) {
                for (auto trans_iter = (*iter)->transactions.begin();
                     trans_iter != (*iter)->transactions.end(); ++trans_iter) {
                    if (trans_iter->write_set.size() == 0) continue;
                    if (trans_iter->command == drop_command) {
                        auto table = trans_iter->write_set.front();
                        trans_iter = (*iter)->transactions.erase(trans_iter);
                        if ((*iter)->transactions.size() == 0)
                            return std::make_tuple(table, std::make_reverse_iterator(++iter),
                                                   (*iter)->transactions.rend());
                        else
                            return std::make_tuple(table, std::make_reverse_iterator(++iter),
                                                   std::make_reverse_iterator(++trans_iter));
                    }
                }
            }
            
            std::vector<StateQuery::st_transaction> empty;
            return std::make_tuple(std::string(), list.rend(), empty.rbegin());
        };
        
        auto reverse_for = [&list, &create_command](std::string table_name, QueryList::reverse_iterator rbegin,
                                                    std::vector<StateQuery::st_transaction>::reverse_iterator trans_rbegin) {
            for (auto iter = rbegin; iter != list.rend();) {
                for (auto trans_iter = trans_rbegin; trans_iter != (*iter)->transactions.rend();) {
                    if (trans_iter->command == create_command &&
                        trans_iter->write_set.size() != 0 && trans_iter->write_set.front() == table_name) {
                        auto it = (*iter)->transactions.erase(--trans_iter.base());
                        trans_iter = std::make_reverse_iterator(it);
                        continue;
                    }
                    
                    ++trans_iter;
                }
                
                if ((*iter)->transactions.size() == 0) {
                    auto it = list.erase(--iter.base());
                    iter = std::make_reverse_iterator(it);
                } else {
                    ++iter;
                }
                
                if (iter != list.rend()) {
                    trans_rbegin = (*iter)->transactions.rbegin();
                }
            }
        };
        
        // 목록에서 drop_command 를 검색, 검색되면 drop_command 이전의 create_command 를 찾아서 삭제
        while (true) {
            auto tuple_iter = forward_for();
            if (std::get<1>(tuple_iter) == list.rend()) {
                break;
            }
            
            reverse_for(std::get<0>(tuple_iter), std::get<1>(tuple_iter), std::get<2>(tuple_iter));
        }
        
        std::string target_db_name;
        auto db_forward_for = [&list, &target_db_name]() {
            for (auto iter = list.begin(); iter != list.end(); ++iter) {
                for (auto trans_iter = (*iter)->transactions.begin();
                     trans_iter != (*iter)->transactions.end(); ++trans_iter) {
                    if (trans_iter->command == SQLCOM_DROP_DB) {
                        target_db_name = GetDbNameFromDropDbQuery(trans_iter->query);
                        
                        trans_iter = (*iter)->transactions.erase(trans_iter);
                        if ((*iter)->transactions.size() == 0)
                            return std::make_tuple(std::make_reverse_iterator(++iter), (*iter)->transactions.rend());
                        else
                            return std::make_tuple(std::make_reverse_iterator(++iter),
                                                   std::make_reverse_iterator(++trans_iter));
                    }
                }
            }
            
            std::vector<StateQuery::st_transaction> empty;
            return std::make_tuple(list.rend(), empty.rbegin());
        };
        
        auto db_reverse_for = [&list, &target_db_name](QueryList::reverse_iterator rbegin,
                                                       std::vector<StateQuery::st_transaction>::reverse_iterator trans_rbegin) {
            std::string db_name, table_name;
            for (auto iter = rbegin; iter != list.rend();) {
                for (auto trans_iter = trans_rbegin; trans_iter != (*iter)->transactions.rend();) {
                    bool is_search = false;
                    for (auto &t: trans_iter->write_set) {
                        if (StateUserQuery::SplitDBNameAndTableName(t, db_name, table_name) == false) {
                            error("[StateTable::RemoveList] invalid name [%s]", t.c_str());
                            return false;
                        }
                        
                        if (target_db_name == db_name) {
                            is_search = true;
                            break;
                        }
                    }
                    
                    if (is_search) {
                        auto it = (*iter)->transactions.erase(--trans_iter.base());
                        trans_iter = std::make_reverse_iterator(it);
                        
                        continue;
                    }
                    
                    ++trans_iter;
                }
                
                if ((*iter)->transactions.size() == 0) {
                    auto it = list.erase(--iter.base());
                    iter = std::make_reverse_iterator(it);
                } else {
                    ++iter;
                }
            }
            
            return true;
        };
        
        // 목록에서 SQLCOM_DROP_DB 를 검색, 검색되면 SQLCOM_DROP_DB 이전의 DB 사용 쿼리를 삭제
        while (true) {
            auto tuple_iter = db_forward_for();
            if (std::get<0>(tuple_iter) == list.rend()) {
                break;
            }
            
            db_reverse_for(std::get<0>(tuple_iter), std::get<1>(tuple_iter));
        }
        
        for (auto &i: list) {
            StateUserQuery::MergeQuery(i);
        }
    }
    
    std::string StateTable::GetBeginTableName(const std::string &fullname, const state_log_time &query_time) {
        //입력한 테이블의 최초 생성 이름을 리턴
        for (auto &i: rename_history) {
            std::string name = i.first;
            
            for (auto &j: i.second) {
                if (query_time > j.time)
                    name = j.name;
                else
                    break;
            }
            
            if (name == fullname)
                return i.first;
        }
        
        return fullname;
    }
    
    std::string StateTable::GetEndTableName(const std::string &fullname, const state_log_time &query_time) {
        //입력한 테이블의 현재(제일 마지막) 이름을 리턴
        for (auto &i: rename_history) {
            std::string name = i.first;
            
            for (auto &j: i.second) {
                if (query_time > j.time)
                    name = j.name;
                else
                    break;
            }
            
            if (name == fullname)
                return i.second.back().name;
        }
        
        return fullname;
    }
    
    std::vector<std::string> StateTable::GetAllTableName(const std::string &fullname, const state_log_time &query_time,
                                                         const state_log_time &undo_time) {
        //입력한 테이블의 undo ~ 현재 이름 전체 목록을 리턴
        std::vector<std::string> range;
        
        struct find_start {
            const state_log_time undo_time;
            
            find_start(const state_log_time &time) : undo_time(time) {}
            
            bool operator()(const rename_info &s) const {
                return undo_time < s.time;
            }
        };
        
        for (auto &i: rename_history) {
            std::string name = i.first;
            
            for (auto &j: i.second) {
                if (query_time > j.time)
                    name = j.name;
                else
                    break;
            }
            
            if (name == fullname) {
                auto iter = std::find_if(i.second.begin(), i.second.end(), find_start(undo_time));
                if (iter == i.second.end()) {
                    range.push_back(i.second.back().name);
                    return range;
                } else if (iter == i.second.begin()) {
                    range.push_back(i.first);
                    for (; iter != i.second.end(); ++iter)
                        range.push_back(iter->name);
                    return range;
                } else {
                    range.push_back(std::prev(iter, 1)->name);
                    for (; iter != i.second.end(); ++iter)
                        range.push_back(iter->name);
                    return range;
                }
            }
        }
        
        range.push_back(fullname);
        return range;
    }
    
    void StateTable::MakeRenameHistory() {
        rename_history.clear();
        
        static const std::vector<int16_t> query_valid_rename_token = {SQL_RENAME, SQL_TABLE, SQL_IDENTIFIER};
        for (auto &i: valid_table_list)
            // for (auto iter = valid_table_list.begin(); iter != valid_table_list.end(); ++iter)
        {
            for (auto &t: i->transactions) {
                if (t.command != SQLCOM_RENAME_TABLE)
                    continue;
                
                // 쿼리를 찢는 token 과 쿼리에서 token 의 offset 벡터
                std::vector<int16_t> tokens;
                std::vector<size_t> token_pos;
                if (hsql::SQLParser::tokenize(t.query, &tokens, &token_pos) == false) {
                    error("[StateTable::MakeRenameHistory] tokenize failed [%s]", t.query.c_str());
                    break;
                }
                
                if (tokens.size() < 5) {
                    error("[StateTable::MakeRenameHistory] unknown query failed [%s]", t.query.c_str());
                    break;
                }
                
                auto it = std::search(tokens.begin(), tokens.end(),
                                      query_valid_rename_token.begin(),
                                      query_valid_rename_token.end());
                
                if (it != tokens.begin()) {
                    error("[StateTable::MakeRenameHistory] unknown query failed [%s]", t.query.c_str());
                    break;
                }
                
                for (size_t i = 2; i < tokens.size(); ++i) {
                    std::string old_table;
                    while (tokens[i] != SQL_TO) {
                        if (i + 1 >= tokens.size()) {
                            old_table += t.query.substr(token_pos[i]);
                            break;
                        } else {
                            old_table += t.query.substr(token_pos[i], token_pos[i + 1] - token_pos[i]);
                            ++i;
                        }
                    }
                    
                    std::string new_table;
                    while (tokens[i] != ',') {
                        if (i + 1 >= tokens.size()) {
                            new_table += t.query.substr(token_pos[i]);
                            break;
                        } else {
                            new_table += t.query.substr(token_pos[i], token_pos[i + 1] - token_pos[i]);
                            ++i;
                        }
                    }
                    
                    if (old_table.size() == 0 || new_table.size() == 0) {
                        error("[StateTable::MakeRenameHistory] unknown query failed [%s]",
                              t.query.c_str());
                        break;
                    }
                    
                    bool is_find = false;
                    for (auto &i: rename_history) {
                        if (i.second.back().name == old_table) {
                            i.second.emplace_back(t.time, new_table);
                            is_find = true;
                            break;
                        }
                    }
                    if (is_find == false) {
                        rename_history.emplace(old_table, std::list<rename_info>());
                        rename_history[old_table].emplace_back(t.time, new_table);
                    }
                }
            }
        }
        
        for (auto &i: rename_history) {
            i.second.sort(
                [](const rename_info &a, const rename_info &b) {
                    return a.time < b.time;
                });
        }
    }
    
    bool StateTable::UpdateReadUserTableList(const std::vector<std::string> &read_set) {
        return UpdateUserTableList(read_set, std::vector<std::string>());
    }
    
    bool StateTable::UpdateWriteUserTableList(const std::vector<std::string> &write_set) {
        return UpdateUserTableList(std::vector<std::string>(), write_set);
    }
    
    bool StateTable::UpdateUserTableList(const std::vector<std::string> &read_set,
                                         const std::vector<std::string> &write_set) {
        bool is_insert = false;
        
        if (read_set.size() > 0) {
            auto tmp = read_user_table_list;
            tmp.insert(tmp.end(), read_set.begin(), read_set.end());
            StateUtil::unique_vector(tmp);
            
            if (read_user_table_list.size() != tmp.size()) {
                is_insert = true;
                read_user_table_list.assign(tmp.begin(), tmp.end());
                user_table_list.insert(user_table_list.end(), read_user_table_list.begin(), read_user_table_list.end());
            }
        }
        
        if (write_set.size() > 0) {
            auto tmp = write_user_table_list;
            tmp.insert(tmp.end(), write_set.begin(), write_set.end());
            StateUtil::unique_vector(tmp);
            
            if (write_user_table_list.size() != tmp.size()) {
                is_insert = true;
                write_user_table_list.assign(tmp.begin(), tmp.end());
                user_table_list.insert(user_table_list.end(), write_user_table_list.begin(),
                                       write_user_table_list.end());
            }
        }
        
        if (is_insert == true) {
            StateUtil::unique_vector(user_table_list);
        }
        
        return is_insert;
    }
    
    bool update_range(std::vector<StateRange> &current_range_set, const std::vector<StateRange> &range_set) {
        // 교차한 범위를 current_range_set 을 합집합으로 업데이트
        auto new_range_set = current_range_set;
        /*
        new_range_set.insert(new_range_set.end(), range_set.begin(), range_set.end());
        new_range_set = StateRange::OR_ARRANGE(new_range_set);
         */
        
        bool is_range_changed = true;
        if (current_range_set.size() == new_range_set.size()) {
            size_t same_count = 0;
            for (size_t idx = 0; idx < current_range_set.size(); ++idx) {
                if (current_range_set[idx] == new_range_set[idx]) {
                    ++same_count;
                }
            }
            
            if (same_count == current_range_set.size()) {
                // 범위가 변경되지 않음
                is_range_changed = false;
            }
        }
        
        if (is_range_changed) {
            current_range_set = new_range_set;
            return true;
        } else {
            return false;
        }
    }
    
    void UpdateForeignMapFunc(std::map<std::string, StateForeign> &fmap, const StateForeign &f) {
        auto iter = fmap.find(f.table);
        if (iter == fmap.end()) {
            fmap.emplace(f.table, f);
        } else {
            iter->second.columns.insert(iter->second.columns.end(), f.columns.begin(), f.columns.end());
            StateUtil::unique_vector(iter->second.columns);
        }
    }
    
    void StateTable::DoneAnalyze(QueryList &list) {
        std::vector<std::future<void>> futures;
        futures.reserve(list.size());
        
        for (auto &i: list) {
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob([](Query q) {
                q->is_valid_query = 0;
            }, i));
        }
        
        for (auto &f: futures) {
            f.get();
        }
    }
    
    void StateTable::PrepareAnalyze(QueryList &list) {
        std::vector<std::future<void>> futures;
        futures.reserve(list.size());
        
        if (current_range_set.size() > 0) {
            for (auto &i: list) {
                futures.emplace_back(StateThreadPool::Instance().EnqueueJob([this](Query q) {
                    q->is_valid_query = 0;
                    
                    if (q->range.size() > 0) {
                        size_t write_table_count = 0;
                        size_t key_column_table_count = 0;
                        for (auto &i: q->foreign_set) {
                            if (i.access_type == en_table_access_read)
                                continue;
                            
                            ++write_table_count;
                            
                            if (i.table == _context.get_key_column_table_name()) {
                                ++key_column_table_count;
                            }
                        }
                        
                        // warehouse 쿼리가 하나의 테이블만 변경하는 경우
                        //    범위 검사 수행
                        if (write_table_count == 1 && key_column_table_count == 1) {
                            q->is_range_valid = true;
                        }
                            
                            // warehouse 쿼리가 여러 테이블을 변경하는 경우
                            //    해당 쿼리는 항상 실행 되어야 함 - 다른 테이블을 변경하는 쿼리가 있기 때문
                            //    해당 쿼리의 range로 업데이트 필요 - 실행되기 때문에
                        else if (write_table_count > 1 && key_column_table_count > 0) {
                            q->is_range_valid = true;
                            q->is_range_update = true;
                        }
                            
                            // warehouse 검사 필요하지 않음
                        else {
                            q->is_range_valid = false;
                        }
                    }
                }, i));
            }
        } else {
            for (auto &i: list) {
                futures.emplace_back(StateThreadPool::Instance().EnqueueJob([](Query q) {
                    q->is_valid_query = 0;
                }, i));
            }
        }
        
        for (auto &f: futures) {
            f.get();
        }
    }
    
    void StateTable::UpdateUserTableListFromLog_2(
        std::map<std::string, StateForeign> &read_foreign_map,
        std::map<std::string, StateForeign> &write_foreign_map,
        std::vector<StateForeign> &foreign_set) {
        auto move_log = [&](QueryList::iterator &iter) {
            valid_log_list.push_back(*iter);
            (*iter)->is_valid_query = 1;
            return ++iter;
        };
        
        auto next_log = [&](QueryList::iterator &iter) {
            ++iter;
            return iter;
        };
        
        // 모든 is_valid_query 리셋
        PrepareAnalyze(valid_log_list);
        
        // 모든 로그를 log_list 에 이동
        // 로그를 검사하면서 실행해야 할 것만 valid_log_list 에 옮김
        log_list = std::move(valid_log_list);
        
        warehouse_changed:
        bool is_use_range = true;
        bool is_changed = true;
        while (is_changed) {
            is_changed = false;
            
            for (auto iter = log_list.begin(); iter != log_list.end();) {
                if ((*iter)->is_valid_query == 1) {
                    ++iter;
                    continue;
                }
                
                auto i = (*iter);
                
                //테이블 관련 쿼리도 재생 하도록 함
#if 0
                switch (i->command)
      {
      case SQLCOM_DROP_DB:

      case SQLCOM_ALTER_TABLE:
      case SQLCOM_CREATE_TABLE:
      case SQLCOM_DROP_TABLE:
      case SQLCOM_TRUNCATE:
      case SQLCOM_CREATE_INDEX:
      case SQLCOM_DROP_INDEX:
        continue;

      default:
        break;
      }
#endif
                
                if (i->foreign_set.size() > 0) {
                    if (is_use_range && i->is_range_valid) {
                        // warehouse 검사 시작
                        if (i->is_range_update) {
                            if (update_range(current_range_set, i->range) == true) {
                                // warehouse 범위 업데이트 -> 재검사 필요
                                goto warehouse_changed;
                            }
                        }
                        
                        size_t empty_size = 0;
                        for (auto &query_range: i->range) {
                            for (auto &base_range: current_range_set) {
                                auto and_range = StateRange::AND(query_range, base_range);
                                if (and_range->GetRange()->size() == 0) {
                                    // 공집합
                                    ++empty_size;
                                } else {
                                    // 교집합
                                    // 해당 쿼리는 의존성이 있으므로 연관성 검사 수행 필요
                                    // goto is_intersection;
                                }
                            }
                        }
                        
                        // warehouse 에 서브쿼리 없으면 그대로 진행
                        for (auto &query_range: i->range) {
                            for (auto &sub_range: *query_range.GetRange()) {
                                if (sub_range.begin.IsSubSelect() || sub_range.end.IsSubSelect()) {
                                    // warehouse 에 서브쿼리 발견시 warehouse 사용하지 않음 -> 재검사 필요
                                    current_range_set.clear();
                                    
                                    // log_list, valid_log_list 원복
                                    is_use_range = false;
                                    log_list.insert(log_list.end(), valid_log_list.begin(), valid_log_list.end());
                                    valid_log_list.clear();
                                    
                                    goto warehouse_changed;
                                }
                            }
                        }
                        
                        if (empty_size == i->range.size() * current_range_set.size()) {
                            // 공집합
                            // 해당 쿼리는 의존성이 없음
                            iter = next_log(iter);
                            goto next;
                        }
                        
                        // 교차한 범위를 current_range_set 을 합집합으로 업데이트
                        if (update_range(current_range_set, i->range) == true) {
                            // warehouse 범위 업데이트 -> 재검사 필요
                            goto warehouse_changed;
                        }
#if 0
                        if (false == StateItem::IsIntersection(current_range_set, i->range))
        {
          continue;
        }
#endif
                    }
                    
                    // 쿼리의 컬럼 정보
                    for (auto &f: i->foreign_set) {
                        // 유효하게 변경된 컬럼 정보
                        // 연결 관계에 대한 우선순위가 있기 때문에 아래와 같이 구분함
                        // WRITE-WRITE > READ-WRITE > WRITE-READ
                        if (f.access_type == en_table_access_write) {
                            auto wf_iter = write_foreign_map.find(f.table);
                            if (wf_iter != write_foreign_map.end()) {
                                // WRITE 테이블은 무조건 되감아야 함 -> 종료시 교체되어야 하기 때문
                                foreign_set.clear();
                                
                                for (auto &t: i->transactions) {
                                    FindSubRelatedTable(t.command, f, t.read_set, t.write_set, foreign_set);
                                }
                                
                                for (auto &sub_f: foreign_set) {
                                    if (sub_f.access_type == en_table_access_write) {
                                        i->write_set.push_back(sub_f.table);
                                        UpdateForeignMapFunc(write_foreign_map, sub_f);
                                    } else {
                                        i->read_set.push_back(sub_f.table);
                                        UpdateForeignMapFunc(read_foreign_map, sub_f);
                                    }
                                }
                                
                                for (auto &sub_f: i->foreign_set) {
                                    if (sub_f.access_type == en_table_access_write)
                                        UpdateForeignMapFunc(write_foreign_map, sub_f);
                                    else
                                        UpdateForeignMapFunc(read_foreign_map, sub_f);
                                }
                                
                                UpdateReadUserTableList(i->read_set);
                                UpdateWriteUserTableList(i->write_set);
                                
                                is_changed = true;
                                iter = move_log(iter);
                                goto next;
                            }
                            
                            auto rf_iter = read_foreign_map.find(f.table);
                            if (rf_iter != read_foreign_map.end()) {
                                if (std::find(f.columns.begin(), f.columns.end(), "*") != f.columns.end() ||
                                    StateUtil::find_sub_range(f.columns, rf_iter->second.columns)) {
                                    foreign_set.clear();
                                    for (auto &t: i->transactions) {
                                        FindSubRelatedTable(t.command, f, t.read_set, t.write_set, foreign_set);
                                    }
                                    
                                    for (auto &sub_f: foreign_set) {
                                        if (sub_f.access_type == en_table_access_write) {
                                            i->write_set.push_back(sub_f.table);
                                            UpdateForeignMapFunc(read_foreign_map, sub_f);
                                        } else {
                                            i->read_set.push_back(sub_f.table);
                                            UpdateForeignMapFunc(read_foreign_map, sub_f);
                                        }
                                    }
                                    
                                    for (auto &sub_f: i->foreign_set) {
                                        if (sub_f.access_type == en_table_access_write)
                                            UpdateForeignMapFunc(read_foreign_map, sub_f);
                                        else
                                            UpdateForeignMapFunc(read_foreign_map, sub_f);
                                    }
                                    
                                    UpdateReadUserTableList(i->read_set);
                                    UpdateReadUserTableList(i->write_set);
                                    
                                    is_changed = true;
                                    iter = move_log(iter);
                                    goto next;
                                }
                            }
                        } else // if (f.access_type == en_table_access_read)
                        {
                            auto f_iter = write_foreign_map.find(f.table);
                            if (f_iter != write_foreign_map.end()) {
                                if (std::find(f.columns.begin(), f.columns.end(), "*") != f.columns.end() ||
                                    StateUtil::find_sub_range(f.columns, f_iter->second.columns)) {
                                    foreign_set.clear();
                                    for (auto &t: i->transactions) {
                                        FindSubRelatedTable(t.command, f, t.read_set, t.write_set, foreign_set);
                                    }
                                    
                                    for (auto &sub_f: foreign_set) {
                                        if (sub_f.access_type == en_table_access_write) {
                                            i->write_set.push_back(sub_f.table);
                                            UpdateForeignMapFunc(write_foreign_map, sub_f);
                                        } else {
                                            i->read_set.push_back(sub_f.table);
                                            UpdateForeignMapFunc(read_foreign_map, sub_f);
                                        }
                                    }
                                    
                                    for (auto &sub_f: i->foreign_set) {
                                        if (sub_f.access_type == en_table_access_write)
                                            UpdateForeignMapFunc(write_foreign_map, sub_f);
                                        else
                                            UpdateForeignMapFunc(read_foreign_map, sub_f);
                                    }
                                    
                                    UpdateReadUserTableList(i->read_set);
                                    UpdateWriteUserTableList(i->write_set);
                                    
                                    is_changed = true;
                                    iter = move_log(iter);
                                    goto next;
                                }
                            }
                        }
                    }
                } else {
                    if (StateUtil::find_sub_range(i->write_set, write_user_table_list)) {
                        for (auto &t: i->write_set) {
                            FindSubTable(t, i->read_set, i->write_set);
                        }
                        
                        UpdateReadUserTableList(i->read_set);
                        UpdateWriteUserTableList(i->write_set);
                        
                        is_changed = true;
                        iter = move_log(iter);
                        goto next;
                    }
                    
                    if (StateUtil::find_sub_range(i->read_set, write_user_table_list)) {
                        for (auto &t: i->read_set) {
                            FindSubTable(t, i->read_set, i->write_set);
                        }
                        
                        UpdateReadUserTableList(i->read_set);
                        UpdateWriteUserTableList(i->write_set);
                        
                        is_changed = true;
                        iter = move_log(iter);
                        goto next;
                    }
                    
                    if (StateUtil::find_sub_range(i->write_set, read_user_table_list)) {
                        for (auto &t: i->write_set) {
                            FindSubTable(t, i->read_set, i->write_set);
                        }
                        
                        UpdateReadUserTableList(i->read_set);
                        UpdateReadUserTableList(i->write_set);
                        
                        is_changed = true;
                        iter = move_log(iter);
                        goto next;
                    }
                }
                
                iter = next_log(iter);
                
                next:
                continue;
            }
        }
        
        DoneAnalyze(valid_log_list);
    }
    
    StateTable::Query StateTable::GetCurrTransaction() {
        if (group_transaction == NULL) {
            return user_transaction;
        } else {
            return group_transaction;
        }
    }
    
    void StateTable::UpdateUserTableListFromLog_1() {
        /**
   * valid log 에서 실제 실행할 쿼리 분리
   * TABLE 과 연결된 쿼리들 마크
   * 쿼리들의 read/write set 업데이트 -> 추후 그래프를 위함
   */
        
        std::vector<std::string> read_set;
        std::vector<std::string> write_set;
        std::vector<StateForeign> foreign_set;
        
        std::map<std::string, StateForeign> read_foreign_map;
        std::map<std::string, StateForeign> write_foreign_map;
        
        current_range_set.clear();
        
        if (user_transaction != NULL) {
            read_set.insert(read_set.end(), user_transaction->read_set.begin(), user_transaction->read_set.end());
            write_set.insert(write_set.end(), user_transaction->write_set.begin(), user_transaction->write_set.end());
            current_range_set.insert(current_range_set.end(), user_transaction->range.begin(),
                                     user_transaction->range.end());
        }
        
        if (group_transaction != NULL) {
            read_set.insert(read_set.end(), group_transaction->read_set.begin(), group_transaction->read_set.end());
            write_set.insert(write_set.end(), group_transaction->write_set.begin(), group_transaction->write_set.end());
            current_range_set.insert(current_range_set.end(), group_transaction->range.begin(),
                                     group_transaction->range.end());
        }
        
        /*
    사용자 쿼리 / Group 쿼리에서 실제 변경한 warehouse 정보를 가져옴
    group list 에서 item_set - where_set 을 구함, 이 목록안에 해당되면 되감기 대상임
    redo list 에서 foreign_set 체크할때 같이 수행
  */
        // current_range_set = StateRange::OR_ARRANGE(current_range_set);
        
        // group list 쿼리와 연결된 TABLE 목록 획득
        auto func = [&](Query &i, bool is_grp) {
            if (i == NULL)
                return;
            
            for (auto &t: i->transactions) {
                uint16_t command = t.command;
                
                // 그룹 쿼리는 쿼리를 취소하기 때문에 반대로 따져야 함
                if (is_grp) {
                    switch (command) {
                        case SQLCOM_DELETE:
                        case SQLCOM_DELETE_MULTI: {
                            command = SQLCOM_INSERT;
                            break;
                        }
                        
                        case SQLCOM_INSERT:
                        case SQLCOM_INSERT_SELECT: {
                            command = SQLCOM_DELETE;
                            break;
                        }
                        
                        default: {
                            break;
                        }
                    }
                }
                
                // 쿼리에 컬럼 정보가 있음
                if (t.foreign_set.size() > 0) {
                    for (auto &f: t.foreign_set) {
                        foreign_set.emplace_back(f);
                        
                        if (f.access_type == en_table_access_write) {
                            FindSubRelatedTable(command, f, read_set, write_set, foreign_set);
                        }
                    }
                } else {
                    for (auto &s: t.write_set) {
                        StateForeign new_foreign(s);
                        new_foreign.columns.push_back("*");
                        FindSubRelatedTable(command, new_foreign, read_set, write_set, foreign_set);
                    }
                    
                    for (auto &s: t.read_set) {
                        StateForeign new_foreign(s);
                        new_foreign.columns.push_back("*");
                        FindSubRelatedTable(command, new_foreign, read_set, write_set, foreign_set);
                    }
                }
            }
        };
        
        func(user_transaction, false);
        func(group_transaction, true);
        
        if (_context.is_using_candidate() == false && _context.get_key_column_name() == NULL) {
            // candidate 탐색 모드이거나 설정한 키가 있을때만 사용
            current_range_set.clear();
        }
        
        StateForeign::MergeList(foreign_set);
        for (auto &i: foreign_set) {
            if (i.access_type == en_table_access_read) {
                read_set.push_back(i.table);
                UpdateForeignMapFunc(read_foreign_map, i);
            } else {
                write_set.push_back(i.table);
                UpdateForeignMapFunc(write_foreign_map, i);
            }
        }
        
        user_table_list.clear();
        read_user_table_list.clear();
        write_user_table_list.clear();
        UpdateUserTableList(read_set, write_set);
        
        UpdateUserTableListFromLog_2(read_foreign_map, write_foreign_map, foreign_set);
    }
    
    void StateTable::UpdateUserTableListFromTableLog() {
        auto process_add_user_table = [this](const std::vector<std::string> &read_set,
                                             const std::vector<std::string> &write_set) {
            for (auto &t: read_set) {
                if (write_user_table_list.end() !=
                    std::find(write_user_table_list.begin(), write_user_table_list.end(), t)) {
                    if (true == UpdateWriteUserTableList(write_set))
                        return true;
                }
            }
            
            for (auto &t: write_set) {
                if (write_user_table_list.end() !=
                    std::find(write_user_table_list.begin(), write_user_table_list.end(), t)) {
                    if (true == UpdateWriteUserTableList(write_set))
                        return true;
                }
            }
            
            return false;
        };
        
        // 연관 테이블이 더 없을때 까지 반복
        bool is_add = true;
        while (is_add == true) {
            is_add = false;
            
            for (auto &i: valid_table_list) {
                is_add |= process_add_user_table(i->read_set, i->write_set);
            }
            
            for (auto &i: valid_view_list) {
                is_add |= process_add_user_table(i->read_set, i->write_set);
            }
            
            for (auto &i: valid_trigger_list) {
                std::vector<std::string> write_set;
                
                for (auto iter = i->write_set.begin(); iter != i->write_set.end(); ++iter) {
                    if ((*iter).find(STATE_TRIGGER_DATABASE) == 0)
                        continue;
                    else
                        write_set.push_back(*iter);
                }
                
                is_add |= process_add_user_table(i->read_set, write_set);
            }
        }
    }
    
    void StateTable::MakeRelatedTableMap(const QueryList &list) {
        auto add_func = [&](const std::string &t,
                            const std::vector<std::string> *read_set) {
            auto iter = related_table_map.emplace(t, related_set(read_set, NULL));
            if (iter.second == true) {
                iter.first->second.add_write_set(t);
                return iter.first;
            } else {
                if (read_set)
                    iter.first->second.add_read_set(*read_set);
                iter.first->second.add_write_set(t);
                return iter.first;
            }
        };
        
        for (auto &i: list) {
            for (auto &t: i->transactions) {
                switch (t.command) {
                    case SQLCOM_CREATE_TRIGGER:
                    case SQLCOM_DROP_TRIGGER: {
                        for (auto &s: t.write_set) {
                            if (s.find(STATE_TRIGGER_DATABASE) == 0)
                                continue;
                            
                            add_func(s, &t.read_set);
                        }
                        break;
                    }
                    
                    case SQLCOM_CREATE_INDEX:
                    case SQLCOM_DROP_INDEX:
                    case SQLCOM_CREATE_TABLE:
                    case SQLCOM_ALTER_TABLE: {
                        if (t.foreign_set.size() > 0) {
                            for (auto &s: t.write_set) {
                                auto iter = add_func(s, NULL);
                                for (auto &f: t.foreign_set) {
                                    if (f.table_type == en_foreign_key_drop) {
                                        iter->second.remove_reference_set(f);
                                    } else if (f.table_type == en_foreign_key_add) {
                                        iter->second.add_reference_set(f);
                                    }
                                }
                            }
                        } else {
                            for (auto &s: t.write_set) {
                                add_func(s, NULL);
                            }
                        }
                        break;
                    }
                    
                    default: {
                        for (auto &s: t.write_set) {
                            add_func(s, &t.read_set);
                        }
                        break;
                    }
                }
            }
        }
    }
    
    void StateTable::FindSubTable(const std::string &table, std::vector<std::string> &read_set,
                                  std::vector<std::string> &write_set) {
        auto iter = related_table_map.find(table);
        if (iter == related_table_map.end())
            return;
        
        for (auto &t: *iter->second.get_read_set()) {
            if (std::find(read_set.begin(), read_set.end(), t) == read_set.end()) {
                read_set.push_back(t);
                FindSubTable(t, read_set, write_set);
            }
        }
        
        for (auto &t: *iter->second.get_write_set()) {
            if (std::find(write_set.begin(), write_set.end(), t) == write_set.end()) {
                write_set.push_back(t);
                FindSubTable(t, read_set, write_set);
            }
        }
    }
    
    void StateTable::FindSubRelatedTable(uint16_t command, const StateForeign curr_foreign,
                                         std::vector<std::string> &read_set, std::vector<std::string> &write_set,
                                         std::vector<StateForeign> &foreign_set) {
        /*
    A 테이블 (
      A_ID
    )

    B 테이블 (
      B_ID
      A_ID
      FOREIGN KEY (A_ID) REFERENCES A 테이블 (A_ID) ON UPDATE CASCADE
    )

    get_referenced_set
    A 테이블에 DELETE 쿼리 : ON DELETE 옵션과 상관 없이 B 테이블에 write 연결
                            A 테이블의 row가 삭제 되면 B 테이블의 외래키 연결된 row는 존재 할 수 없음 (외래키 위반)
    A 테이블에 INSERT 쿼리 : 연결 없음
    A 테이블에 UPDATE 쿼리 : ON UPDATE 옵션에 따라 B 테이블에 write 연결, (+ 컬럼 기반 분석에 따라)

    get_reference_set
    B 테이블에 DELETE 쿼리 : A 테이블에 read 연결 (B 테이블을 되감을려면 A 필요)
    B 테이블에 INSERT 쿼리 : A 테이블에 read 연결 (B 테이블을 되감을려면 A 필요)
    B 테이블에 UPDATE 쿼리 : A 테이블에 read 연결, (+ 컬럼 기반 분석에 따라)
  */
        
        if (curr_foreign.access_type != en_table_access_write)
            return;
        
        auto iter = related_table_map.find(curr_foreign.table);
        if (iter == related_table_map.end())
            return;
        
        std::vector<std::string> new_columns;
        std::vector<size_t> new_columns_idx;
        switch (command) {
            //UPDATE 쿼리일때 참조관계 있는 테이블에 연결 필요한지 판단
            case SQLCOM_UPDATE:
            case SQLCOM_UPDATE_MULTI: {
                // 참조 되고 있는 외래키 목록
                for (auto &sub_ref: *iter->second.get_referenced_set()) {
                    // 해당 연결관계가 체크할 필요가 있는가?
                    if (sub_ref.update_type == en_foreign_key_opt_check) {
                        // 쿼리의 컬럼이 외래키에 속하는가?
                        // referenced 에서 sub_ref.columns 변수는 sub_ref.table 의 컬럼명임
                        // 따라서 외래키 테이블의 컬럼명인 sub_ref.foreign_columns 을 사용
                        // reference 에서는 sub_ref.columns 을 사용해야함
                        
                        // 쿼리의 컬럼이 전체를 의미할 수 있음 (*)
                        if (std::find(curr_foreign.columns.begin(), curr_foreign.columns.end(), "*") !=
                            curr_foreign.columns.end())
                            new_columns = sub_ref.columns;
                        else {
                            new_columns.clear();
                            
                            new_columns_idx = StateUtil::return_sub_range_idx(curr_foreign.columns,
                                                                              sub_ref.foreign_columns);
                            for (auto &i: new_columns_idx) {
                                new_columns.emplace_back(sub_ref.columns[i]);
                            }
                        }
                        
                        if (new_columns.size() > 0) {
                            StateForeign new_foreign(sub_ref);
                            new_foreign.access_type = en_table_access_write;
                            new_foreign.columns = new_columns;
                            new_foreign.foreign_columns.clear();
                            foreign_set.push_back(new_foreign);
                            
                            FindSubRelatedTable(command, new_foreign, read_set, write_set, foreign_set);
                        }
                    }
                }
                
                for (auto &sub_ref: *iter->second.get_reference_set()) {
                    if (std::find(curr_foreign.columns.begin(), curr_foreign.columns.end(), "*") !=
                        curr_foreign.columns.end())
                        new_columns = sub_ref.foreign_columns;
                    else {
                        new_columns.clear();
                        
                        new_columns_idx = StateUtil::return_sub_range_idx(curr_foreign.columns, sub_ref.columns);
                        for (auto &i: new_columns_idx) {
                            new_columns.emplace_back(sub_ref.foreign_columns[i]);
                        }
                    }
                    
                    if (new_columns.size() > 0) {
                        StateForeign new_foreign(sub_ref);
                        new_foreign.access_type = en_table_access_read;
                        new_foreign.columns = new_columns;
                        new_foreign.foreign_columns.clear();
                        foreign_set.push_back(new_foreign);
                        
                        FindSubRelatedTable(command, new_foreign, read_set, write_set, foreign_set);
                    }
                }
                
                break;
            }
                
                //DELETE 쿼리일때 참조관계 있는 테이블에 연결 필요한지 판단
            case SQLCOM_DELETE:
            case SQLCOM_DELETE_MULTI: {
                for (auto &sub_ref: *iter->second.get_referenced_set()) {
                    StateForeign new_foreign(sub_ref);
                    new_foreign.access_type = en_table_access_write;
                    new_foreign.columns.emplace_back("*");
                    new_foreign.foreign_columns.clear();
                    foreign_set.push_back(new_foreign);
                    
                    FindSubRelatedTable(command, new_foreign, read_set, write_set, foreign_set);
                }
                
                for (auto &sub_ref: *iter->second.get_reference_set()) {
                    StateForeign new_foreign(sub_ref);
                    new_foreign.access_type = en_table_access_read;
                    new_foreign.columns.emplace_back("*");
                    new_foreign.foreign_columns.clear();
                    foreign_set.push_back(new_foreign);
                    
                    FindSubRelatedTable(command, new_foreign, read_set, write_set, foreign_set);
                }
                break;
            }
                
                //INSERT 쿼리일때 참조관계 있는 테이블에 연결 필요한지 판단
            case SQLCOM_INSERT:
            case SQLCOM_INSERT_SELECT: {
                for (auto &sub_ref: *iter->second.get_reference_set()) {
                    StateForeign new_foreign(sub_ref);
                    new_foreign.access_type = en_table_access_read;
                    new_foreign.columns.emplace_back("*");
                    new_foreign.foreign_columns.clear();
                    foreign_set.push_back(new_foreign);
                    
                    FindSubRelatedTable(command, new_foreign, read_set, write_set, foreign_set);
                }
                break;
            }
            
            default: {
                FindSubTable(curr_foreign.table, read_set, write_set);
                break;
            }
        }
    }
    
    void StateTable::MakeForeignMap() {
        while (true) {
            // 현재 테이블의
            for (auto &curr_table: related_table_map) {
                // 외래키 정보 목록
                for (auto &ref_table_info: *curr_table.second.get_reference_set()) {
                    // 외래키 참조한 테이블을
                    auto iter = related_table_map.find(ref_table_info.table);
                    if (iter == related_table_map.end()) {
                        // 새로 생성
                        auto v = related_table_map.emplace(ref_table_info.table, related_set(NULL, NULL));
                        // 현재 테이블의 외래키 정보 입력
                        StateForeign new_ref(ref_table_info);
                        new_ref.table = curr_table.first;
                        v.first->second.add_referenced_set(new_ref);
                        
                        // 목록이 변경되었기 때문에 다시 시작
                        goto restart;
                    } else {
                        // 현재 테이블의 외래키 정보 입력
                        StateForeign new_ref(ref_table_info);
                        new_ref.table = curr_table.first;
                        iter->second.add_referenced_set(new_ref);
                    }
                }
            }
            break;
            
            restart:
            continue;
        }
    }
    
    bool StateTable::ProcessTableLog() {
        // rename 히스토리 생성
        MakeRenameHistory();
        
        // 연관 테이블 생성
        auto process_drop = [start_time = start_time](QueryList &origin_list,
                                                      QueryList &tmp_list,
                                                      int drop_command,
                                                      int create_command) {
            // start_time 이전은 tmp_list 이후는 origin_list 에 구분
            std::vector<StateQuery::st_transaction> old_transactions;
            
            for (auto iter = origin_list.begin(); iter != origin_list.end();) {
                old_transactions.clear();
                
                for (auto t_iter = (*iter)->transactions.begin(); t_iter != (*iter)->transactions.end();) {
                    if (start_time > t_iter->time) {
                        old_transactions.emplace_back(*t_iter);
                        t_iter = (*iter)->transactions.erase(t_iter);
                    } else {
                        ++t_iter;
                    }
                }
                
                if (old_transactions.size() > 0) {
                    // 새로운 쿼리를 만들어서 삭제된 트랜잭션을 모음
                    // tmp_list 에 누적함
                    auto curr_query = std::make_shared<StateQuery>();
                    (*curr_query) = *(*iter);
                    curr_query->transactions = old_transactions;
                    StateUserQuery::MergeQuery(curr_query);
                    tmp_list.emplace_back(curr_query);
                    
                    // 트랜잭션이 일부 빠지면서 범위를 새로 만듦
                    StateUserQuery::MergeQuery(*iter);
                }
                
                if ((*iter)->transactions.size() == 0) {
                    iter = origin_list.erase(iter);
                } else {
                    ++iter;
                }
            }
            
            // undo 이전 범위 중에 drop 된 것은 제외
            RemoveList(tmp_list, create_command, drop_command);
            origin_list.insert(origin_list.begin(), tmp_list.begin(), tmp_list.end());
        };
        
        process_drop(valid_view_list, before_undo_view_list, SQLCOM_DROP_VIEW, SQLCOM_CREATE_VIEW);
        process_drop(valid_trigger_list, before_undo_trigger_list, SQLCOM_DROP_TRIGGER, SQLCOM_CREATE_TRIGGER);
        process_drop(valid_procedure_list, before_undo_procedure_list, SQLCOM_DROP_PROCEDURE, SQLCOM_CREATE_PROCEDURE);
        // table 목록중 read set 이 있는 경우(외래키)를 위해 검사
        process_drop(valid_table_list, before_undo_table_list, SQLCOM_DROP_TABLE, SQLCOM_CREATE_TABLE);
        
        // 테이블 연관 목록 생성
        MakeRelatedTableMap(before_undo_table_list);
        MakeRelatedTableMap(valid_table_list);
        MakeRelatedTableMap(before_undo_trigger_list);
        MakeRelatedTableMap(valid_trigger_list);
        MakeRelatedTableMap(before_undo_view_list);
        MakeRelatedTableMap(valid_view_list);
        MakeRelatedTableMap(before_undo_procedure_list);
        MakeRelatedTableMap(valid_procedure_list);
        
        MakeForeignMap();
        
        return true;
    }
    
    std::vector<std::string> StateTable::GetUserTableList() {
        return user_table_list;
    }
    
    void StateTable::ReduceLogForTest(int ratio) {
        for (auto &i: valid_log_list) {
            if (rand() % 100 < ratio) {
                i->is_valid_query = 1;
            }
        }
        
        log_list.assign(valid_log_list.begin(), valid_log_list.end());
        MoveMarkList(log_list, valid_log_list);
        log_list.clear();
    }
    
    bool StateTable::ValidTableLogLow() {
        RemoveGroupQuery(valid_procedure_list);
        RemoveGroupQuery(valid_view_list);
        RemoveGroupQuery(valid_trigger_list);
        RemoveGroupQuery(valid_table_list);
        
        debug("[StateTable::ValidTableLogLow] procedure : %lu, valid : %lu", procedure_list.size(),
              valid_procedure_list.size());
        debug("[StateTable::ValidTableLogLow] view : %lu, valid : %lu", view_list.size(), valid_view_list.size());
        debug("[StateTable::ValidTableLogLow] trigger : %lu, valid : %lu", trigger_list.size(),
              valid_trigger_list.size());
        debug("[StateTable::ValidTableLogLow] table : %lu, valid : %lu", table_list.size(), valid_table_list.size());
        
        // 기존 리스트 삭제
        procedure_list.clear();
        view_list.clear();
        trigger_list.clear();
        table_list.clear();
        
        return ProcessTableLog();
    }
    
    bool StateTable::ValidGroupLogLow() {
        debug("[StateTable::ValidGroupLogLow] group : %lu, valid : %lu", group_list.size(), valid_group_list.size());
        
        if (valid_group_list.size() == 0) {
            error("[StateTable::ValidGroupLogLow] No query about group id (%d)", group_id);
            return false;
        }
        
        group_transaction = StateUserQuery::MakeTransaction(valid_group_list);
        
        // 검색된 로그를 기준으로 연관 테이블 생성
        StateUserQuery::MergeQuery(group_transaction);
        for (auto &i: group_transaction->transactions) {
            UpdateReadUserTableList(i.read_set);
            UpdateWriteUserTableList(i.write_set);
        }
        
        return true;
    }
    
    bool StateTable::ValidLogLow() {
        RemoveGroupQuery(valid_log_list);
        
        debug("[StateTable::ValidLogLow] state : %lu, valid : %lu", log_list.size(), valid_log_list.size());
        
        // 기존 리스트 삭제
        log_list.clear();
        
        return true;
    }
    
    bool StateTable::ValidLog() {
        auto valid_proc_func = [&](long sec, ulong sec_part, uint64_t xid) {
            MarkList(sec, sec_part, xid, group_list);
            
            MarkList(sec, sec_part, xid, procedure_list);
            MarkList(sec, sec_part, xid, view_list);
            MarkList(sec, sec_part, xid, trigger_list);
            MarkList(sec, sec_part, xid, table_list);
            
            MarkList(sec, sec_part, xid, log_list);
        };
        
        /*
        for (auto &i: binary_log->GetList()) {
            if (process_binary_log(i.filepath, valid_proc_func) == false) {
                error("[StateTable::ReadGroupLogLow] process_binary_log error");
                return false;
            }
        }
         */
        
        MoveMarkList(group_list, valid_group_list);
        
        MoveMarkList(table_list, valid_table_list);
        
        // 트랜잭션 전환
        // 테이블 목록은 트랜잭션 전환 할 필요 없음
        //  - 테이블 관련 쿼리는 항상 commit 됨
        // 테이블 목록은 트랜잭션 전환 할 수 없음
        //  - 외래키 정보를 병합할 수 없기 때문
        MoveMarkListAndMakeTransaction(procedure_list, valid_procedure_list);
        MoveMarkListAndMakeTransaction(view_list, valid_view_list);
        MoveMarkListAndMakeTransaction(trigger_list, valid_trigger_list);
        
        // 최초 로그를 읽었을때 (시간으로 정렬 하기 전에) 트랜잭션으로 전환
        // 최초 로그는 트랜잭션 id 로 정렬되어 있음
        // -> 시간으로 정렬을 하면 id 가 섞임
        MoveMarkListAndMakeTransaction(log_list, valid_log_list);
        
        if (group_transaction != NULL) {
            if (false == ValidGroupLogLow())
                return false;
        }
        
        if (user_table_list.size() == 0) {
            error("[StateTable::ValidLog] empty user table");
            return false;
        }
        
        if (false == ValidTableLogLow())
            return false;
        
        if (false == ValidLogLow())
            return false;
        
        return true;
    }
    
    struct CandidateColumn {
        std::string name;
        StateRange range;
        
        CandidateColumn(const std::string &name, const StateRange &range) : name(name), range(range) {}
    };
    
    std::vector<CandidateColumn> MakeCandidateColumnList(StateTable::Query q) {
        // insert, update, delete 쿼리를 대상으로
        // 쿼리에 컬럼 정보가 있는지 확인하고, 컬럼정보가 있을때만 분석함
        // item_set 은 외부 쿼리에 사용한 컬럼 정보가 있고,
        // where_set 은 내부 쿼리에 사용한 컬럼 정보가 있음
        // 외부 쿼리와 내부 쿼리에 동일한 컬럼명이 있으면 후보 컬럼임
        
        std::vector<std::tuple<std::string, std::string, std::string, StateItem>> cols;
        std::vector<CandidateColumn> result_cols;
        
        q->is_valid_query = 0;
        for (const auto &t: q->transactions) {
            cols.clear();
            
            for (auto &i: t.item_set) {
                if (i.condition_type == EN_CONDITION_NONE &&
                    i.function_type == FUNCTION_NONE) {
                    const auto vec = StateUserQuery::SplitDBNameAndTableName(i.name);
                    if (vec.size() != 3) {
                        continue;
                    }
                    cols.emplace_back(std::make_tuple(i.name, vec[1], vec[2], i));
                }
            }
            
            if (cols.size() > 1) {
                // 컬럼이 있는 경우만 컬럼 분석을 수행
                // 그 외 쿼리는 필요없음
                q->is_valid_query = 1;
            }
            
            for (auto &w: t.where_set) {
                if (w.condition_type != EN_CONDITION_NONE &&
                    w.function_type == FUNCTION_NONE) {
                    // 실제 where 절 컬럼 목록
                    for (auto &a: w.arg_list) {
                        // 각 컬럼의 조건 (=, <= 등)
                        for (auto &sub_a: a.arg_list) {
                            const auto &vec = StateUserQuery::SplitDBNameAndTableName(sub_a.name);
                            if (vec.size() != 3) {
                                continue;
                            }
                            for (auto &c: cols) {
                                // 테이블명은 다르고 column명이 동일할때 후보임
                                if (std::get<1>(c) != vec[1] && std::get<2>(c) == vec[2]) {
                                    // result_cols.emplace_back(std::get<0>(c), std::get<3>(c).MakeRange());
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return result_cols;
    }
    
    bool StateTable::ApplyCandidateColumn(QueryList &query_list, const std::string &column_name) {
        std::vector<std::future<bool>> futures;
        
        for (auto &i: query_list) {
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob(
                [&column_name](Query &i) -> bool {
                    bool is_valid = true;
                    bool ret = true;
                    
                    i->range.clear();
                    
                    for (auto &t: i->transactions) {
                        auto copy_item_set = t.item_set;
                        for (auto &item: copy_item_set) {
                            if (item.name == column_name)
                                // i->range.emplace_back(item.MakeRange(column_name, is_valid));
                            if (!is_valid) ret = false;
                        }
                        
                        auto copy_where_set = t.where_set;
                        for (auto &where: copy_where_set) {
                            // i->range.emplace_back(where.MakeRange(column_name, is_valid));
                            if (!is_valid) ret = false;
                        }
                    }
                    
                    // i->range = StateRange::OR_ARRANGE(i->range);
                    
                    return ret;
                },
                i));
        }
        
        bool is_valid = true;
        for (auto &f: futures) {
            if (false == f.get()) {
                is_valid = false;
            }
        }
        
        return is_valid;
    }
    
    bool StateTable::FindCandidateColumn(std::string &column) {
        // 모든 is_valid_query 리셋
        DoneAnalyze(valid_log_list);
        
        // 후보 컬럼 수집
        std::map<std::string, std::vector<StateRange>> candidate_maps;
        std::vector<std::future<std::vector<CandidateColumn>>> futures;
        futures.reserve(valid_log_list.size());
        
        for (auto &i: valid_log_list) {
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob(MakeCandidateColumnList, i));
        }
        
        for (auto &f: futures) {
            for (auto &c: f.get()) {
                candidate_maps[c.name].push_back(c.range);
            }
        }
        
        double lowest_sigma = 0.0f;
        std::string lowest_name = "";
        for (const auto &c: candidate_maps) {
            // sigma 를 계산하여 가장 작은것이 후보임
            double sigma = 0.0f;
            for (auto &r: c.second) {
                // 동일한 범위의 쿼리의 개수
                double count = 0;
                for (auto &i: c.second) {
                    auto range = StateRange::AND(i, r);
                    if (range->GetRange()->size() > 0) {
                        count += 1.0f;
                    }
                }
                
                sigma += std::pow(count, 2.0f);
            }
            
            if (lowest_name.size() == 0) {
                lowest_sigma = sigma;
                lowest_name = c.first;
            } else if (sigma < lowest_sigma) {
                lowest_sigma = sigma;
                lowest_name = c.first;
            }
        }
        
        column = lowest_name;
        if (column.size() > 0) {
            return true;
        } else {
            return false;
        }
    }
    
    bool StateTable::AnalyzeLog() {
        UpdateUserTableListFromLog_1();
        
        SortList(valid_log_list);
        log_list.clear();
        
        debug("[StateTable::AnalyzeLog] last valid : %lu", valid_log_list.size());
        
        if (valid_log_list.size() == 0) {
            return false;
        }
        
        // 쿼리의 중복 vector 정리
        std::vector<std::future<void>> futures;
        for (auto &i: valid_log_list) {
            futures.emplace_back(StateThreadPool::Instance().EnqueueJob(
                [](Query &i) {
                    StateUserQuery::MergeTableSet(i->read_set, i->write_set);
                    i->MakeUpData();
                },
                i));
        }
        
        for (auto &f: futures) {
            f.get();
        }
        
        // HASH 매칭 시점 계산
        if (group_transaction != NULL) {
            // group ID 기능을 통한 상태 전환
            auto &hash_query_time = _context.get_hash_query_time();
            hash_query_time.sec = group_transaction->transactions.front().time.sec;
            hash_query_time.sec_part = group_transaction->transactions.front().time.sec_part;
        } else {
            // user 쿼리 기능을 통한 상태 전환
            throw std::runtime_error("fix: reimplement analyzelog");
            /*
            auto &hash_query_time = _context.get_key_column_name();
            hash_query_time.sec = valid_log_list.front()->transactions.front().time.sec;
            hash_query_time.sec_part = valid_log_list.front()->transactions.front().time.sec_part;
             */
        }
        
        return true;
    }
    
    bool StateTable::IsFindRangeVector(const std::vector<std::string> &data, const std::vector<std::string> &range) {
        return StateUtil::find_sub_range(data, range);
    }
    
    bool StateTable::UndoVersionTable(MYSQL *mysql, const std::string &fullname, const state_log_time &query_time,
                                      const state_log_time &undo_time) {
        std::string curr_fullname = GetEndTableName(fullname, query_time);
        std::string undo_fullname = GetBeginTableName(fullname, query_time);
        
        std::string undo_dbname, undo_tablename;
        if (StateUserQuery::SplitDBNameAndTableName(fullname, undo_dbname, undo_tablename) == false) {
            error("[StateTable::UndoVersionTable] invalid name [%s]", fullname.c_str());
            return false;
        }
        
        if (false == StateUserQuery::IsExistsTable(mysql, STATE_CHANGE_DATABASE, undo_dbname + "." + undo_tablename)) {
            debug("[StateTable::UndoVersionTable] table is not created yet %s", fullname.c_str());
            return true;
        }
        
        if (false == StateUserQuery::SetQueryTime(mysql, undo_time)) {
            error("[StateTable::UndoVersionTable] set query time failed %s", fullname.c_str());
            return false;
        }
        
        if (current_range_set.size() > 0 && fullname == _context.get_key_column_table_name()) {
            std::stringstream query;
            
            for (auto &i: current_range_set) {
                /*
        a) 상태전환쿼리의 warehouse 번호를 가진 row들은 상태전환시점으로 되감고,
        b) 나머지 warehouse 번호를 가진 row들은 현재 상태를 그대로 유지
        a + b를 한 테이블을 생성 => 상태변환을 수행할 TEMP.TABLE_A 생성

        a) 단계
            select * from TABLE_A where start_time="과거시점" and w_id=x 하여 특정 warehouse 번호를 가진 row 를 조회하여 TEMP.TABLE_A 에 추가
        b) 단계
            select * from TABLE_A where start_time="현재시점" and w_id !=x 하여 특정 warehouse 번호를 제외한 현재의 row를 조회하여 TEMP.TABLE_A 에 추가
      */
                std::string where = i.MakeWhereQuery();
                
                query.str("");
                query << STATE_QUERY_COMMENT_STRING " INSERT INTO " << STATE_CHANGE_DATABASE
                      << ".`" << undo_dbname << "." << undo_tablename
                      << "` ( SELECT * FROM " << StateUserQuery::MakeFullTableName(curr_fullname)
                      << " FOR SYSTEM_TIME AS OF TIMESTAMP '"
                      << StateUtil::format_time(undo_time.sec, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0')
                      << std::setw(6) << undo_time.sec_part << "'"
                      << " WHERE (" << where << "));";
                if (mysql_query(mysql, query.str().c_str()) != 0) {
                    error("[StateTable::UndoVersionTable] query error [%s] [%s]", query.str().c_str(),
                          mysql_error(mysql));
                    return false;
                }
                debug("[StateTable::UndoVersionTable] %s", query.str().c_str());
                
                query.str("");
                query << STATE_QUERY_COMMENT_STRING " INSERT INTO " << STATE_CHANGE_DATABASE
                      << ".`" << undo_dbname << "." << undo_tablename
                      << "` ( SELECT * FROM " << StateUserQuery::MakeFullTableName(curr_fullname)
                      << " FOR SYSTEM_TIME AS OF TIMESTAMP '"
                      << StateUtil::format_time(_context.get_end_time().sec, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0')
                      << std::setw(6) << _context.get_end_time().sec_part << "'"
                      << " WHERE !(" << where << "));";
                if (mysql_query(mysql, query.str().c_str()) != 0) {
                    error("[StateTable::UndoVersionTable] query error [%s] [%s]", query.str().c_str(),
                          mysql_error(mysql));
                    return false;
                }
                debug("[StateTable::UndoVersionTable] %s", query.str().c_str());
            }
        } else {
            // warehouse 테이블이 아닐경우
            
            std::stringstream query;
            query << STATE_QUERY_COMMENT_STRING " INSERT INTO " << STATE_CHANGE_DATABASE
                  << ".`" << undo_dbname << "." << undo_tablename
                  << "` ( SELECT * FROM " << StateUserQuery::MakeFullTableName(curr_fullname)
                  << " FOR SYSTEM_TIME AS OF TIMESTAMP '" << StateUtil::format_time(undo_time.sec, "%Y-%m-%d %H:%M:%S")
                  << '.' << std::setfill('0') << std::setw(6) << undo_time.sec_part << "');";
            
            if (mysql_query(mysql, query.str().c_str()) != 0) {
                error("[StateTable::UndoVersionTable] query error [%s] [%s]", query.str().c_str(), mysql_error(mysql));
                return false;
            }
            debug("[StateTable::UndoVersionTable] %s", query.str().c_str());
        }
        
        return true;
    }
    
    bool StateTable::UndoBackupTable(MYSQL *mysql, const std::string &fullname, const state_log_time &query_time,
                                     const state_log_time &undo_time) {
        std::string curr_fullname = GetEndTableName(fullname, query_time);
        std::string undo_fullname = GetBeginTableName(fullname, query_time);
        
        std::string undo_dbname, undo_tablename;
        if (StateUserQuery::SplitDBNameAndTableName(undo_fullname, undo_dbname, undo_tablename) == false) {
            error("[StateTable::UndoBackupTable] invalid name [%s]", undo_fullname.c_str());
            return false;
        }
        
        std::string backup_dbname, backup_tablename;
        if (StateUserQuery::SplitDBNameAndTableName(fullname, backup_dbname, backup_tablename) == false) {
            error("[StateTable::UndoBackupTable] invalid name [%s]", fullname.c_str());
            return false;
        }
        
        if (false == StateUserQuery::IsExistsTable(mysql, STATE_CHANGE_DATABASE, undo_dbname + "." + undo_tablename)) {
            debug("[StateTable::UndoBackupTable] table is not created yet");
            return true;
        }
        
        if (false == StateUserQuery::SetQueryTime(mysql, undo_time)) {
            error("[StateTable::UndoBackupTable] set query time failed");
            return false;
        }
        
        std::string backup_fullname = StateUserQuery::GetBackupTableName(mysql, backup_dbname, backup_tablename,
                                                                         undo_time);
        if (backup_fullname.size() == 0) {
            error("[StateTable::UndoBackupTable] get backup table name failed");
            return false;
        }
        
        if (current_range_set.size() > 0 && fullname == _context.get_key_column_table_name()) {
            std::stringstream query;
            for (auto &i: current_range_set) {
                /*
        a) 상태전환쿼리의 warehouse 번호를 가진 row들은 상태전환시점으로 되감고,
        b) 나머지 warehouse 번호를 가진 row들은 현재 상태를 그대로 유지
        a + b를 한 테이블을 생성 => 상태변환을 수행할 TEMP.TABLE_A 생성

        a) 단계
            select * from TABLE_A where start_time="과거시점" and w_id=x 하여 특정 warehouse 번호를 가진 row 를 조회하여 TEMP.TABLE_A 에 추가
        b) 단계
            select * from TABLE_A where start_time="현재시점" and w_id !=x 하여 특정 warehouse 번호를 제외한 현재의 row를 조회하여 TEMP.TABLE_A 에 추가
      */
                
                std::string where = i.MakeWhereQuery();
                
                query.str("");
                query << STATE_QUERY_COMMENT_STRING " INSERT INTO " << STATE_CHANGE_DATABASE
                      << ".`" << undo_dbname << "." << undo_tablename
                      << "` ( SELECT * FROM " << StateUserQuery::MakeFullTableName(backup_fullname)
                      << " FOR SYSTEM_TIME AS OF TIMESTAMP '"
                      << StateUtil::format_time(undo_time.sec, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0')
                      << std::setw(6) << undo_time.sec_part << "'"
                      << " WHERE (" << where << "));";
                if (mysql_query(mysql, query.str().c_str()) != 0) {
                    error("[StateTable::UndoBackupTable] query error [%s] [%s]", query.str().c_str(),
                          mysql_error(mysql));
                    return false;
                }
                debug("[StateTable::UndoBackupTable] %s", query.str().c_str());
                
                query.str("");
                query << STATE_QUERY_COMMENT_STRING " INSERT INTO " << STATE_CHANGE_DATABASE
                      << ".`" << undo_dbname << "." << undo_tablename
                      << "` ( SELECT * FROM " << StateUserQuery::MakeFullTableName(curr_fullname)
                      << " FOR SYSTEM_TIME AS OF TIMESTAMP '"
                      << StateUtil::format_time(_context.get_end_time().sec, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0')
                      << std::setw(6) << _context.get_end_time().sec_part << "'"
                      << " WHERE !(" << where << "));";
                if (mysql_query(mysql, query.str().c_str()) != 0) {
                    error("[StateTable::UndoBackupTable] query error [%s] [%s]", query.str().c_str(),
                          mysql_error(mysql));
                    return false;
                }
                debug("[StateTable::UndoBackupTable] %s", query.str().c_str());
            }
        } else {
            // warehouse 테이블이 아닐경우
            
            std::stringstream query;
            query << STATE_QUERY_COMMENT_STRING " INSERT INTO " << STATE_CHANGE_DATABASE
                  << ".`" << undo_dbname << "." << undo_tablename
                  << "` ( SELECT * FROM " << StateUserQuery::MakeFullTableName(backup_fullname)
                  << " FOR SYSTEM_TIME AS OF TIMESTAMP '" << StateUtil::format_time(undo_time.sec, "%Y-%m-%d %H:%M:%S")
                  << '.' << std::setfill('0') << std::setw(6) << undo_time.sec_part << "');";
            
            if (mysql_query(mysql, query.str().c_str()) != 0) {
                error("[StateTable::UndoBackupTable] query error [%s] [%s]", query.str().c_str(), mysql_error(mysql));
                return false;
            }
            debug("[StateTable::UndoBackupTable] %s", query.str().c_str());
        }
        
        return true;
    }
    
    bool StateTable::IsWriteTable(const std::string &fullname) {
        auto iter = std::find(write_user_table_list.begin(), write_user_table_list.end(), fullname);
        if (iter == write_user_table_list.end())
            return false;
        return true;
    }
    
    bool StateTable::CreateUndoTablesLow(int command, MYSQL *mysql, const std::string &fullname,
                                         const state_log_time &query_time, const state_log_time &undo_time,
                                         StateTableInfo *info) {
        if (StateUserQuery::SetForeignKeyChecks(mysql, false) == false) {
            error("[StateTable::CreateUndoTablesLow] SetForeignKeyChecks error");
            return false;
        }
        
        if (command == SQLCOM_DROP_TABLE ||
            command == SQLCOM_TRUNCATE) {
            if (UndoBackupTable(mysql, fullname, query_time, undo_time) == false) {
                error("[StateTable::CreateUndoTablesLow] undo with table failed %s:%d.%lu",
                      fullname.c_str(),
                      query_time.sec,
                      query_time.sec_part);
                return false;
            }
            if (info != NULL) {
                std::string db, table;
                StateUserQuery::SplitDBNameAndTableName(fullname, db, table);
                info->from = en_from_backup_table;
                info->from_sec = undo_time.sec;
                info->from_sec_part = undo_time.sec_part;
                info->name = db + "." + table;
            }
        } else if (command == SQLCOM_CREATE_TABLE) {
            if (UndoVersionTable(mysql, fullname, query_time, undo_time) == false) {
                error("[StateTable::CreateUndoTablesLow] undo with version table failed %s:%d.%lu",
                      fullname.c_str(),
                      undo_time.sec,
                      undo_time.sec_part);
                return false;
            }
            if (info != NULL) {
                std::string db, table;
                StateUserQuery::SplitDBNameAndTableName(fullname, db, table);
                info->from = en_from_origin_table;
                info->from_sec = undo_time.sec;
                info->from_sec_part = undo_time.sec_part;
                info->name = db + "." + table;
            }
        }
        
        if (StateUserQuery::SetForeignKeyChecks(mysql, true) == false) {
            error("[StateTable::CreateUndoTablesLow] SetForeignKeyChecks error");
            return false;
        }
        
        return true;
    }
    
    void StateTable::ClearReadTableList() {
        read_user_table_list.clear();
        user_table_list = write_user_table_list;
    }
    
    bool StateTable::CreateUndoTables(const std::vector<std::tuple<std::string, std::string>> &table_list,
                                      std::vector<StateTableInfo> *info_list) {
        //cleanup
        for (auto &i: write_user_table_list) {
            auto iter = std::find(read_user_table_list.begin(), read_user_table_list.end(), i);
            if (iter != read_user_table_list.end()) {
                read_user_table_list.erase(iter);
            }
        }
        
        //for debug
        for (auto &i: read_user_table_list) {
            debug("[StateTable::CreateUndoTables] read user table : %s", i.c_str());
        }
        for (auto &i: write_user_table_list) {
            debug("[StateTable::CreateUndoTables] write user table : %s", i.c_str());
        }
        
        auto mysql = StateThreadPool::Instance().GetMySql()->handle().get();
        
        if (StateTable::CreateUndoDB(mysql) == false) {
            return false;
        }
        
        if (mysql_commit(mysql) != 0) {
            error("[StateTable::CreateUndoTables] failed to commit [%s]", mysql_error(mysql));
            return false;
        }
        
        SortList(before_undo_table_list);
        if (RunCreateTable(before_undo_table_list) == false) {
            error("[StateTable::CreateUndoTables] CreateTable SQLCOM_CREATE_TABLE error");
            return false;
        }
        
        struct st_last_command {
            int command;
            state_log_time time;
            bool is_done;
        };
        std::map<std::string, struct st_last_command> table_map;
        
        // UNDO 시간 이후 테이블이 삭제된 경우 BACKUP DB 에서 시스템 버저닝을 통해 UNDO 테이블 생성
        // rename 된 테이블은 UNDO 시점의 이름을 갖도록 함
        // UNDO 하면서 rename 쿼리를 통해 현재 시점의 이름이 됨
        for (auto &i: valid_table_list) {
            auto &trans = i->transactions.front();
            
            switch (trans.command) {
                case SQLCOM_DROP_TABLE:
                case SQLCOM_TRUNCATE:
                case SQLCOM_CREATE_TABLE:
                    break;
                default:
                    continue;
            }
            
            if (std::find(user_table_list.begin(), user_table_list.end(), trans.write_set[0]) == user_table_list.end())
                continue;
            
            for (auto &target_table: trans.write_set) {
                auto range = GetAllTableName(target_table, trans.time, start_time);
                if (false == IsFindRangeVector(user_table_list, range))
                    continue;
                
                if (trans.time < start_time) {
                    table_map[target_table] = {trans.command, trans.time, false};
                } else {
                    auto iter = table_map.find(target_table);
                    if (iter == table_map.end() || iter->second.is_done == false) {
                        table_map[target_table] = {trans.command, trans.time, true};
                    }
                }
            }
        }
        
        auto func = [&](int command, const std::string &fullname, const state_log_time &query_time,
                        const state_log_time &undo_time, StateTableInfo *info) {
            auto handle = StateThreadPool::Instance().PopMySql();
            auto mysql = handle->handle().get();
            
            auto ret = CreateUndoTablesLow(command, mysql, fullname, query_time, start_time, info);
            
            StateThreadPool::Instance().PushMySql(handle, true);
            
            return ret;
        };
        
        std::vector<std::future<bool>> futures;
        if (info_list != NULL) {
            for (size_t i = 0; i < table_map.size(); ++i) {
                info_list->emplace_back();
            }
            
            size_t idx = 0;
            for (auto &i: table_map) {
                futures.emplace_back(
                    StateThreadPool::Instance().EnqueueJob(func, i.second.command, i.first, i.second.time, start_time,
                                                           &info_list->at(idx++)));
            }
        } else {
            for (auto &i: table_map) {
                futures.emplace_back(
                    StateThreadPool::Instance().EnqueueJob(func, i.second.command, i.first, i.second.time, start_time,
                                                           nullptr));
            }
        }
        
        for (auto &f: futures) {
            if (f.get() == false)
                return false;
        }
        
        if (RunCreateCommand(table_list, before_undo_view_list, SQLCOM_CREATE_VIEW, true) == false) {
            error("[StateTable::CreateUndoTables] SQLCOM_CREATE_VIEW error");
            return false;
        }
        
        if (RunCreateCommand(table_list, before_undo_trigger_list, SQLCOM_CREATE_TRIGGER, false) == false) {
            error("[StateTable::CreateUndoTables] SQLCOM_CREATE_TRIGGER error");
            return false;
        }
        
        if (RunCreateCommand(table_list, before_undo_procedure_list, SQLCOM_CREATE_PROCEDURE, false) == false) {
            error("[StateTable::CreateUndoTables] SQLCOM_CREATE_PROCEDURE error");
            return false;
        }
        
        if (RunCreateCommand(table_list, before_undo_procedure_list, SQLCOM_ALTER_PROCEDURE, false) == false) {
            error("[StateTable::CreateUndoTables] SQLCOM_ALTER_PROCEDURE error");
            return false;
        }
        
        return true;
    }
    
    bool StateTable::RunCreateTable(QueryList &list) {
        for (auto &i: list) {
            auto &trans = i->transactions.front();
            
            switch (trans.command) {
                case SQLCOM_CREATE_TABLE:
                case SQLCOM_ALTER_TABLE:
                case SQLCOM_CREATE_INDEX:
                case SQLCOM_DROP_INDEX:
                    break;
                default:
                    continue;
            }
            if (0 != StateUserQuery::RunQuery(i.get())) {
                return false;
            }
        }
        
        return true;
    }
    
    bool
    StateTable::RunCreateCommand(const std::vector<std::tuple<std::string, std::string>> &table_list, QueryList &list,
                                 int command, bool is_check) {
        std::vector<std::future<size_t>> futures;
        for (auto &i: list) {
            for (auto &t: i->transactions) {
                if (t.command != command)
                    continue;
                
                if (is_check) {
                    if (t.write_set.size() == 0 ||
                        std::find(user_table_list.begin(), user_table_list.end(), t.write_set[0]) ==
                        user_table_list.end()) {
                        continue;
                    }
                    debug("[StateTable::RunCreateCommand] %s %s", StateUtil::GetCommandName(t.command),
                          t.write_set[0].c_str());
                } else {
                    StateUserQuery::ReplaceQuery(table_list, t.query);
                    debug("[StateTable::RunCreateCommand] %s %s", StateUtil::GetCommandName(t.command),
                          t.query.substr(0, t.query.find('\n')).c_str());
                }
                
                futures.emplace_back(StateThreadPool::Instance().EnqueueJob(
                    [](StateQuery::st_transaction &t) -> size_t {
                        auto handle = StateThreadPool::Instance().PopMySql();
                        auto mysql = handle->handle().get();
                        
                        if (false == StateUserQuery::SetQueryTime(mysql, t.time)) {
                            error("[StateTable::RunCreateCommand] set query time failed");
                            StateThreadPool::Instance().PushMySql(handle, false);
                            return 1;
                        }
                        
                        std::string new_query = STATE_QUERY_COMMENT_STRING " " + t.query;
                        if (mysql_query(mysql, new_query.c_str()) != 0) {
                            error("[StateTable::RunCreateCommand] query error [%s] [%s]", new_query.c_str(),
                                  mysql_error(mysql));
                            StateThreadPool::Instance().PushMySql(handle, false);
                            return 1;
                        }
                        
                        StateThreadPool::Instance().PushMySql(handle, true);
                        
                        return 0;
                    },
                    t));
            }
        }
        
        size_t failed_count = 0;
        for (auto &f: futures) {
            failed_count += f.get();
        }
        
        if (failed_count > 0)
            return false;
        
        return true;
    }
    
    bool StateTable::CreateUndoDB(MYSQL *mysql) {
        std::stringstream query;
        
        query.str(std::string());
        query << STATE_QUERY_COMMENT_STRING " DROP DATABASE IF EXISTS " << STATE_CHANGE_DATABASE << ';';
        
        if (mysql_query(mysql, query.str().c_str()) != 0) {
            error("[StateTable::CreateUndoDB] query error [%s] [%s]", query.str().c_str(), mysql_error(mysql));
            return false;
        }
        // debug("[StateTable::CreateUndoDB] %s", query.str().c_str());
        
        query.str(std::string());
        query << STATE_QUERY_COMMENT_STRING " CREATE DATABASE " << STATE_CHANGE_DATABASE << ';';
        
        if (mysql_query(mysql, query.str().c_str()) != 0) {
            error("[StateTable::CreateUndoDB] query error [%s] [%s]", query.str().c_str(), mysql_error(mysql));
            return false;
        }
        // debug("[StateTable::CreateUndoDB] %s", query.str().c_str());
        
        return true;
    }
    
}