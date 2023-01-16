#include <mysql/mysql.h>

#include "StateGraph.h"
#include "StateUserQuery.h"
#include "StateThreadPool.h"
#include "StateUtil.h"
#include "SQLParser.h"
#include "bison_parser.h"

#include "utils/log.hpp"

#include <strings.h>
#include <fstream>
#include <algorithm>

namespace ultraverse::state {
    
    inline static void
    update_token_pos(std::vector<size_t> &token_pos, size_t idx, size_t origin_string_size, size_t new_string_size) {
        if (origin_string_size > new_string_size) {
            std::for_each(token_pos.begin() + idx + 1, token_pos.end(),
                          [len = origin_string_size - new_string_size](size_t &i) { i -= len; });
        } else if (origin_string_size < new_string_size) {
            std::for_each(token_pos.begin() + idx + 1, token_pos.end(),
                          [len = new_string_size - origin_string_size](size_t &i) { i += len; });
        }
    }
    
    StateUserQuery::StateUserQuery()
        : database(""),
          delimiter(";") {
    }
    
    StateUserQuery::~StateUserQuery() {
    }
    
    static const std::vector<int16_t> db_table_field_token = {SQL_IDENTIFIER, '.', SQL_IDENTIFIER, '.', SQL_IDENTIFIER};
    static const std::vector<int16_t> db_table_token = {SQL_IDENTIFIER, '.', SQL_IDENTIFIER};
    
    static const std::vector<int16_t> from_table_token = {SQL_FROM, SQL_IDENTIFIER};     // ',' ... ignore 0
    static const std::vector<int16_t> update_table_token = {SQL_UPDATE, SQL_IDENTIFIER}; // ',' ... ignore 0
    static const std::vector<int16_t> table_opt_table_token = {SQL_TABLE, SQL_IF, SQL_NOT, SQL_EXISTS, SQL_IDENTIFIER};
    static const std::vector<int16_t> table_table_token = {SQL_TABLE, SQL_IDENTIFIER};
    static const std::vector<int16_t> into_table_token = {SQL_INTO, SQL_IDENTIFIER};
    static const std::vector<int16_t> ref_table_token = {SQL_REFERENCES, SQL_IDENTIFIER};
    static const std::vector<int16_t> on_table_token = {SQL_ON, SQL_IDENTIFIER};
    static const std::vector<int16_t> join_table_token = {SQL_JOIN, SQL_IDENTIFIER};
    static const std::vector<int16_t> straight_join_table_token = {SQL_STRAIGHT_JOIN, SQL_IDENTIFIER};
    
    static const std::string new_dbname = STATE_CHANGE_DATABASE;
    
    inline bool replace_db_table_name(const std::vector<std::tuple<std::string, std::string>> &oldname_list,
                                      std::string old_dbname, std::string old_tablename, std::string &new_name) {
        StateUtil::trim(old_dbname);
        if (old_dbname.front() == '`') {
            if (old_dbname.back() != '`') {
                error("[replace_db_table_name] invalid dbname %s", old_dbname.c_str());
                return false;
            }
            
            old_dbname = old_dbname.substr(1, old_dbname.size() - 2);
        }
        
        StateUtil::trim(old_tablename);
        if (old_tablename.front() == '`') {
            if (old_tablename.back() != '`') {
                error("[replace_db_table_name] invalid tablename %s", old_tablename.c_str());
                return false;
            }
            
            old_tablename = old_tablename.substr(1, old_tablename.size() - 2);
        }
        
        for (auto &i: oldname_list) {
            if (StateUtil::iequals(old_dbname, std::get<0>(i)) &&
                StateUtil::iequals(old_tablename, std::get<1>(i))) {
                //DB.TABLE -> NEW_DB.`DB.TABLE`
                new_name = new_dbname + ".`" + std::get<0>(i) + "." + std::get<1>(i) + "`";
                return true;
            }
        }
        
        return false;
    }
    
    inline bool replace_table_field_name(const std::vector<std::tuple<std::string, std::string>> &oldname_list,
                                         std::string old_tablename, std::string old_fieldname, std::string &new_name) {
        StateUtil::trim(old_tablename);
        if (old_tablename.front() == '`') {
            if (old_tablename.back() != '`') {
                error("[replace_table_field_name] invalid dbname %s", old_tablename.c_str());
                return false;
            }
            
            old_tablename = old_tablename.substr(1, old_tablename.size() - 2);
        }
        
        StateUtil::trim(old_fieldname);
        if (old_fieldname.front() == '`') {
            if (old_fieldname.back() != '`') {
                error("[replace_table_field_name] invalid tablename %s", old_fieldname.c_str());
                return false;
            }
            
            old_fieldname = old_fieldname.substr(1, old_fieldname.size() - 2);
        }
        
        for (auto &i: oldname_list) {
            if (StateUtil::iequals(old_tablename, std::get<1>(i))) {
                //TABLE.FIELD -> NEW_DB.`DB.TABLE`.FIELD
                new_name = new_dbname + ".`" + std::get<0>(i) + "." + std::get<1>(i) + "`." + old_fieldname;
                return true;
            }
        }
        
        return false;
    }
    
    inline bool replace_table_name(const std::vector<std::tuple<std::string, std::string>> &oldname_list,
                                   std::string old_tablename, std::string &new_name) {
        StateUtil::trim(old_tablename);
        if (old_tablename.front() == '`') {
            if (old_tablename.back() != '`') {
                error("[replace_table_name] invalid tablename %s", old_tablename.c_str());
                return false;
            }
            
            old_tablename = old_tablename.substr(1, old_tablename.size() - 2);
        }
        
        for (auto &i: oldname_list) {
            if (StateUtil::iequals(old_tablename, std::get<1>(i))) {
                new_name = new_dbname + ".`" + std::get<0>(i) + "." + std::get<1>(i) + "`";
                return true;
            }
        }
        
        return false;
    }
    
    inline bool predicate_function(int16_t list, int16_t token) {
        if (token == SQL_IDENTIFIER) {
            return true;
        } else
            return list == token;
    }
    
    inline void
    replace_single_table_name(const std::vector<std::tuple<std::string, std::string>> &oldname_list, std::string &query,
                              std::vector<int16_t> &tokens, std::vector<size_t> &token_pos,
                              const std::vector<int16_t> &table_token, bool is_more) {
        size_t idx, origin_string_size;
        std::string old_table, new_string;
        bool is_trim;
        
        auto it = tokens.begin();
        while (true) {
            it = std::search(it, tokens.end(),
                             table_token.begin(),
                             table_token.end(),
                             predicate_function);
            
            if (it != tokens.end()) {
                idx = it - tokens.begin();
                idx += (table_token.size() - 1);
                
                for (auto i = idx; i < tokens.size(); ++i) {
                    if (is_more && tokens[i] == ',')
                        continue;
                    
                    // 이미 변경된 토큰
                    if (tokens[i] == 0)
                        continue;
                    
                    if (token_pos.size() > i + 1)
                        old_table = query.substr(token_pos[i], token_pos[i + 1] - token_pos[i]);
                    else
                        old_table = query.substr(token_pos[i]);
                    
                    origin_string_size = old_table.size();
                    
                    if (old_table.back() == ' ') {
                        StateUtil::trim(old_table);
                        is_trim = true;
                    } else
                        is_trim = false;
                    
                    if (replace_table_name(oldname_list, old_table, new_string) == false) {
                        ++it;
                    } else {
                        if (is_trim)
                            new_string += " ";
                        
                        // 쿼리가 변경되면 offset 도 같이 변경
                        query.replace(token_pos[i], origin_string_size, new_string);
                        update_token_pos(token_pos, i, origin_string_size, new_string.size());
                    }
                    
                    // 변경이 완료된 토큰은 이후 stage 에서 재검색 되지 않도록 초기화
                    tokens[i] = 0;
                    
                    if (is_more)
                        continue;
                    else
                        break;
                }
                ++it;
            } else {
                break;
            }
        }
    }
    
    bool StateUserQuery::ReplaceQuery(const std::vector<std::tuple<std::string, std::string>> &oldname_list,
                                      std::string &query) {
        // 쿼리를 찢는 token 과 쿼리에서 token 의 offset 벡터
        std::vector<int16_t> tokens;
        std::vector<size_t> token_pos;
        if (hsql::SQLParser::tokenize(query, &tokens, &token_pos) == false) {
            error("[StateUserQuery::ReplaceQuery] tokenize failed [%s]",
                  query.c_str());
            return false;
        }
        
        std::vector<int16_t>::iterator it;
        size_t idx, origin_string_size;
        std::string old_db, old_table, new_string;
        bool is_trim;
    
        /* stage 1
            DB.TABLE.FIELD 의 경우
            DB.TABLE 찾아 변경
        */
        it = tokens.begin();
        while (true) {
            it = std::search(it, tokens.end(),
                             db_table_field_token.begin(),
                             db_table_field_token.end(),
                             predicate_function);
            
            if (it != tokens.end()) {
                idx = it - tokens.begin();
                
                old_db = query.substr(token_pos[idx], token_pos[idx + 1] - token_pos[idx]);
                old_table = query.substr(token_pos[idx + 2], token_pos[idx + 3] - token_pos[idx + 2]);
                origin_string_size = old_db.size() + old_table.size() + 1;
                
                if (replace_db_table_name(oldname_list, old_db, old_table, new_string) == false) {
                    ++it;
                } else {
                    // 변경이 완료된 토큰은 이후 stage 에서 재검색 되지 않도록 초기화
                    std::for_each(it, it + db_table_field_token.size(), [](int16_t &n) { n = 0; });
                    
                    // 쿼리가 변경되면 offset 도 같이 변경
                    query.replace(token_pos[idx], origin_string_size, new_string);
                    update_token_pos(token_pos, idx, origin_string_size, new_string.size());
                    
                    ++it;
                }
            } else {
                break;
            }
        }
    
        /* stage 2
            DB.TABLE 또는 TABLE.FIELD 의 경우
            DB.TABLE 찾아 변경
        */
        it = tokens.begin();
        while (true) {
            it = std::search(it, tokens.end(),
                             db_table_token.begin(),
                             db_table_token.end(),
                             predicate_function);
            
            if (it != tokens.end()) {
                idx = it - tokens.begin();
                
                old_db = query.substr(token_pos[idx], token_pos[idx + 1] - token_pos[idx]);
                if (token_pos.size() > idx + 3) {
                    old_table = query.substr(token_pos[idx + 2], token_pos[idx + 3] - token_pos[idx + 2]);
                } else {
                    old_table = query.substr(token_pos[idx + 2]);
                }
                origin_string_size = old_db.size() + old_table.size() + 1;
                
                if (old_table.back() == ' ') {
                    StateUtil::trim(old_table);
                    is_trim = true;
                } else
                    is_trim = false;
                
                // DB.TABLE 매칭 시도
                // DB.TABLE 매칭 실패시 TABLE.FIELD 매칭 시도
                if (replace_db_table_name(oldname_list, old_db, old_table, new_string) == false &&
                    replace_table_field_name(oldname_list, old_db, old_table, new_string) == false) {
                    ++it;
                } else {
                    if (is_trim)
                        new_string += " ";
                    
                    // 변경이 완료된 토큰은 이후 stage 에서 재검색 되지 않도록 초기화
                    std::for_each(it, it + db_table_token.size(), [](int16_t &n) { n = 0; });
                    
                    // 쿼리가 변경되면 offset 도 같이 변경
                    query.replace(token_pos[idx], origin_string_size, new_string);
                    update_token_pos(token_pos, idx, origin_string_size, new_string.size());
                    
                    ++it;
                }
            } else {
                break;
            }
        }
    
        /* stage 3
            기타의 경우
              from table, table, ...
              update table, table, ...
              table table
              into table
              ...
            TABLE 찾아 변경
        */
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  from_table_token, true);
        
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  update_table_token, true);
        
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  table_opt_table_token, false);
        
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  table_table_token, false);
        
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  into_table_token, false);
        
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  ref_table_token, false);
        
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  on_table_token, false);
        
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  join_table_token, false);
        
        replace_single_table_name(oldname_list, query, tokens, token_pos,
                                  straight_join_table_token, false);
        
        return true;
    }
    
    const std::shared_ptr<StateQuery> StateUserQuery::GetUserTransaction() {
        return user_transaction_query;
    }
    
    const std::shared_ptr<StateQuery> StateUserQuery::GetUserAddTransaction() {
        return user_add_transaction_query;
    }
    
    const std::shared_ptr<StateQuery> StateUserQuery::GetUserDelTransaction() {
        return user_del_transaction_query;
    }
    
    const std::vector<std::shared_ptr<StateQuery>> StateUserQuery::GetUserTransactionList() {
        return user_transaction_query_list;
    }
    
    std::string StateUserQuery::GetDatabase() {
        return database;
    }
    
    std::string StateUserQuery::MakeFullTableName(const std::string &t) {
        std::string db, table;
        if (false == SplitDBNameAndTableName(t, db, table)) {
            return "";
        }
        
        return '`' + db + "`.`" + table + '`';
    }
    
    std::vector<std::string> StateUserQuery::SplitDBNameAndTableName(const std::string &in_fullname) {
        std::vector<std::string> vec;
        
        std::string::size_type s_pos = 0;
        std::string::size_type e_pos = std::string::npos;
        while ((e_pos = in_fullname.find_first_of("`.", s_pos)) != std::string::npos) {
            if (in_fullname[s_pos] == '`') {
                ++s_pos;
                e_pos = in_fullname.find('`', s_pos);
                if (s_pos == e_pos) {
                    s_pos = e_pos + 1;
                } else if (e_pos != std::string::npos) {
                    vec.push_back(in_fullname.substr(s_pos, e_pos - s_pos));
                    s_pos = e_pos + 1;
                }
            } else if (s_pos == e_pos) {
                s_pos = e_pos + 1;
            } else {
                vec.push_back(in_fullname.substr(s_pos, e_pos - s_pos));
                s_pos = e_pos + 1;
            }
        }
        
        if (s_pos != in_fullname.size())
            vec.push_back(in_fullname.substr(s_pos));
        
        return std::move(vec);
    }
    
    bool StateUserQuery::SplitDBNameAndTableName(const std::string &in_fullname, std::string &out_dbname,
                                                 std::string &out_tablename) {
        std::vector<std::string> vec = SplitDBNameAndTableName(in_fullname);
        
        if (vec.size() == 1) {
            out_dbname = "";
            out_tablename = vec[0];
        } else if (vec.size() == 2) {
            out_dbname = vec[0];
            out_tablename = vec[1];
        } else {
            error("[StateUserQuery::SplitDBNameAndTableName] failed to parse %s", in_fullname.c_str());
            return false;
        }
        
        return true;
    }
    
    void StateUserQuery::ReplaceQuery(StateQuery *query) {
        std::vector<std::string> fullname_list;
        std::vector<std::tuple<std::string, std::string>> oldname_list;
        std::string dbname;
        std::string tablename;
        
        fullname_list.clear();
        fullname_list.insert(fullname_list.end(), query->read_set.begin(), query->read_set.end());
        fullname_list.insert(fullname_list.end(), query->write_set.begin(), query->write_set.end());
        for (auto &i: query->foreign_set) {
            fullname_list.push_back(i.table);
        }
        StateUtil::unique_vector(fullname_list);
        
        oldname_list.clear();
        for (auto &i: fullname_list) {
            if (StateUserQuery::SplitDBNameAndTableName(i, dbname, tablename) == false) {
                error("[StateUserQuery::ReplaceQuery] SplitDBNameAndTableName error [%s]", i.c_str());
            }
            
            if (dbname == STATE_TRIGGER_DATABASE)
                continue;
            
            oldname_list.emplace_back(std::make_tuple(dbname, tablename));
        }
        
        for (auto &i: query->transactions) {
            StateUserQuery::ReplaceQuery(oldname_list, i.query);
        }
    }
    
    size_t StateUserQuery::RunQueryLow(StateQuery *query, std::function<void(MYSQL *, StateQuery *)> init_func,
                                       std::function<void(MYSQL *, StateQuery *)> exit_func) {
        auto handle = StateThreadPool::Instance().PopMySql();
        auto mysql = handle->handle().get();
        
        size_t failed_count = 0;
        
        while (query) {
            init_func(mysql, query);
            
            StateUserQuery::SetQueryStateXid(mysql, query->xid);
            
            for (auto &i: query->transactions) {
                std::string new_query = STATE_QUERY_COMMENT_STRING " " + i.query;
                
                if (false == StateUserQuery::SetQueryTime(mysql, i.time)) {
                    error("[StateUserQuery::RunQueryLow] set query time failed");
                    ++failed_count;
                    query->is_failed = 1;
                }
                
                if (mysql_query(mysql, new_query.c_str()) != 0) {
                    error("[StateUserQuery::RunQueryLow] query error [%s] [%s]", new_query.c_str(), mysql_error(mysql));
                    ++failed_count;
                    query->is_failed = 1;
                }
                //debug("[StateUserQuery::RunQueryLow] %s", new_query.c_str());
            }
            
            exit_func(mysql, query);
            
            query = query->GetNextPtr();
        }
        
        StateThreadPool::Instance().PushMySql(handle, true);
        
        return failed_count;
    }
    
    size_t StateUserQuery::RunBulkQuery(StateTaskContext &context, StateQuery *query) {
        std::streampos st_pos, ed_pos;
        std::stringstream ss;
        
        struct ST_POS {
            std::streampos start;
            std::streampos end;
        };
        struct ST_QUERY {
            StateQuery *query;
            std::vector<ST_POS> pos;
        };
        std::vector<ST_QUERY> ss_pos_vec;
        
        while (query) {
            ss_pos_vec.emplace_back();
            auto &curr_pos_vec = ss_pos_vec.back();
            
            curr_pos_vec.query = query;
            for (auto &i: query->transactions) {
                st_pos = ss.tellp();
                ss << GetQueryTimeQuery(i.time);
                ed_pos = ss.tellp();
                curr_pos_vec.pos.emplace_back(ST_POS{st_pos, ed_pos - st_pos});
                
                st_pos = ed_pos;
                if (query->user_query == EN_USER_QUERY_CURRENT)
                    ss << i.query;
                else
                    ss << STATE_QUERY_COMMENT_STRING " " << i.query;
                ed_pos = ss.tellp();
                curr_pos_vec.pos.emplace_back(ST_POS{st_pos, ed_pos - st_pos});
            }
            
            query = query->GetNextPtr();
        }
        
        char sp_id[128] = {0,};
        
        std::string str_query = ss.str();
        const char *str_ptr = str_query.c_str();
        
        size_t acc_failed_count = 0;
        for (auto &curr_pos_vec: ss_pos_vec) {
            curr_pos_vec.query->ref.Wait();
            
            auto handle = StateThreadPool::Instance().PopMySql();
            auto mysql = handle->handle().get();
            
            snprintf(sp_id, sizeof(sp_id) - 1, "SP_MULTI_%p", mysql);
            
            StateUserQuery::CreateSavePoint(mysql, sp_id);
            
            size_t failed_count = 0;
            for (auto &i: curr_pos_vec.pos) {
                if (context.is_hash_matched() == true) {
                    debug("[StateUserQuery::RunBulkQuery] hash matched");
                    
                    acc_failed_count += failed_count;
                    
                    StateThreadPool::Instance().PushMySql(handle, true);
                    return acc_failed_count;
                }
                
                if (mysql_real_query(mysql, str_ptr + i.start, (uint) i.end) != 0) {
                    // error("[StateUserQuery::RunBulkQuery] query error [%p] [%s] [%s]", mysql, std::string(str_ptr + i.start, i.end).c_str(), mysql_error(mysql));
                    // if (mysql_errno(mysql) == 40001/*ER_LOCK_DEADLOCK*/)
                    // {
                    //   StateUserQuery::QueryAndPrint(mysql, "SELECT * FROM information_schema.INNODB_LOCK_WAITS");
                    //   StateUserQuery::QueryAndPrint(mysql, "SELECT * FROM information_schema.INNODB_LOCKS");
                    //   StateUserQuery::QueryAndPrint(mysql, "SELECT * FROM information_schema.INNODB_TRX");
                    //   StateUserQuery::QueryAndPrint(mysql, "SHOW ENGINE INNODB STATUS");
                    // }
                    
                    // q->is_failed = 1;
                    ++failed_count;
                    
                    ClearResult(mysql);
                    
                    break;
                }
                // debug("[StateUserQuery::RunBulkQuery] query [%s]", std::string(str_ptr + i.start, i.end).c_str());
                
                ClearResult(mysql);
            }
            
            if (failed_count == 0) {
                StateUserQuery::ReleaseSavePoint(mysql, sp_id);
            } else {
                StateUserQuery::RollbackToSavePoint(mysql, sp_id);
            }
            
            StateThreadPool::Instance().PushMySql(handle, true);
            curr_pos_vec.query->GetNextNoti();
            
            acc_failed_count += failed_count;
        }
        
        return acc_failed_count;
    }
    
    size_t StateUserQuery::RunQuery(StateQuery *query) {
        auto init_func = [](MYSQL *, StateQuery *query) {
            ReplaceQuery(query);
        };
        
        return RunQueryLow(query, init_func, [](MYSQL *, StateQuery *) {});
    }
    
    bool StateUserQuery::IsExistsTable(MYSQL *mysql, const std::string &db, const std::string &table) {
        std::stringstream ss;
        ss << "SELECT TEMPORARY from information_schema.TABLES WHERE TABLE_SCHEMA = '"
           << db << "' AND TABLE_NAME = '" << table << "'";
        
        MYSQL_RES *query_result = NULL;
        MYSQL_ROW query_row;
        
        if (mysql_query(mysql, ss.str().c_str()) != 0 ||
            (query_result = mysql_store_result(mysql)) == NULL) {
            error("[StateUserQuery::IsExistsTable] table select failed [%s] [%s]",
                  ss.str().c_str(), mysql_error(mysql));
            return false;
        }
        // debug("[StateUserQuery::IsExistsTable] %s", ss.str().c_str());
        
        bool ret = false;
        while ((query_row = mysql_fetch_row(query_result))) {
            ret = true;
        }
        mysql_free_result(query_result);
        
        return ret;
    }
    
    bool StateUserQuery::SetForeignKeyChecks(MYSQL *mysql, bool value) {
        std::string query = STATE_QUERY_COMMENT_STRING" SET FOREIGN_KEY_CHECKS=";
        if (value)
            query += std::string("1");
        else
            query += std::string("0");
        
        if (mysql_query(mysql, query.c_str()) != 0) {
            error("[StateUserQuery::SetForeignKeyChecks] query error [%s] [%s]", query.c_str(), mysql_error(mysql));
            return false;
        }
        // debug("[StateUserQuery::SetForeignKeyChecks] %s", query.c_str());
        return true;
    }
    
    std::string StateUserQuery::GetQueryTimeQuery(const state_log_time &time) {
        char query[128] = {
            0,
        };
        snprintf(query, sizeof(query) - 1, STATE_QUERY_COMMENT_STRING" SET SESSION TIMESTAMP=%ld.%06lu", time.sec,
                 time.sec_part);
        return query;
    }
    
    void StateUserQuery::ClearResult(MYSQL *mysql) {
        MYSQL_RES *query_result = NULL;
        
        do {
            if ((query_result = mysql_use_result(mysql)) == NULL) {
                return;
            }
            mysql_free_result(query_result);
            
        } while (mysql_next_result(mysql) == 0);
    }
    
    bool StateUserQuery::CreateSavePoint(MYSQL *mysql, const std::string &id) {
        std::string query = STATE_QUERY_COMMENT_STRING" SAVEPOINT " + id;
        if (mysql_real_query(mysql, query.c_str(), query.size()) != 0) {
            error("[StateUserQuery::CreateSavePoint] query error [%s]", mysql_error(mysql));
            return false;
        }
        return true;
    }
    
    bool StateUserQuery::ReleaseSavePoint(MYSQL *mysql, const std::string &id) {
        std::string query = STATE_QUERY_COMMENT_STRING" RELEASE SAVEPOINT " + id;
        if (mysql_real_query(mysql, query.c_str(), query.size()) != 0) {
            // error("[StateUserQuery::ReleaseSavePoint] query error [%s]", mysql_error(mysql));
            return false;
        }
        return true;
    }
    
    bool StateUserQuery::RollbackToSavePoint(MYSQL *mysql, const std::string &id) {
        std::string query = STATE_QUERY_COMMENT_STRING" ROLLBACK TO " + id;
        if (mysql_real_query(mysql, query.c_str(), query.size()) != 0) {
            error("[StateUserQuery::RollbackToSavePoint] query error [%s]", mysql_error(mysql));
            return false;
        }
        return true;
    }
    
    bool StateUserQuery::SetIsolationLevel(MYSQL *mysql, enum_tx_isolation level) {
        switch (level) {
            case ISO_READ_UNCOMMITTED: {
                if (mysql_real_query(mysql,
                                     STATE_QUERY_COMMENT_STRING" SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
                                     sizeof(STATE_QUERY_COMMENT_STRING" SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")) !=
                    0) {
                    error("[StateUserQuery::SetIsolationLevel] transaction query error [%s]", mysql_error(mysql));
                    return false;
                }
                return true;
            }
            
            case ISO_READ_COMMITTED: {
                if (mysql_real_query(mysql,
                                     STATE_QUERY_COMMENT_STRING" SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                                     sizeof(STATE_QUERY_COMMENT_STRING" SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")) !=
                    0) {
                    error("[StateUserQuery::SetIsolationLevel] transaction query error [%s]", mysql_error(mysql));
                    return false;
                }
                return true;
            }
            
            case ISO_REPEATABLE_READ: {
                if (mysql_real_query(mysql,
                                     STATE_QUERY_COMMENT_STRING" SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                                     sizeof(STATE_QUERY_COMMENT_STRING" SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")) !=
                    0) {
                    error("[StateUserQuery::SetIsolationLevel] transaction query error [%s]", mysql_error(mysql));
                    return false;
                }
                return true;
            }
            
            case ISO_SERIALIZABLE: {
                if (mysql_real_query(mysql,
                                     STATE_QUERY_COMMENT_STRING" SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                                     sizeof(STATE_QUERY_COMMENT_STRING" SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")) !=
                    0) {
                    error("[StateUserQuery::SetIsolationLevel] transaction query error [%s]", mysql_error(mysql));
                    return false;
                }
                return true;
            }
            
            default: {
                error("[StateUserQuery::SetIsolationLevel] unknown level");
                return false;
            }
        }
    }
    
    bool StateUserQuery::SetQueryStateXid(MYSQL *mysql, const uint64_t xid) {
        char query[128] = {
            0,
        };
        snprintf(query, sizeof(query) - 1, STATE_QUERY_COMMENT_STRING" SET SESSION state_log_transaction_id=%lu", xid);
        
        if (mysql_query(mysql, query) != 0) {
            error("[StateUserQuery::SetQueryStateXid] query error [%s] [%s]", query, mysql_error(mysql));
            return false;
        }
        return true;
    }
    
    void StateUserQuery::QueryAndPrint(MYSQL *mysql, const std::string &query) {
        MYSQL_RES *result = NULL;
        if (mysql_real_query(mysql, query.c_str(), query.size()) == 0 &&
            (result = mysql_store_result(mysql)) != NULL) {
            auto num_fields = mysql_num_fields(result);
            
            MYSQL_ROW row;
            while ((row = mysql_fetch_row(result))) {
                auto lengths = mysql_fetch_lengths(result);
                for (auto i = 0; i < num_fields; i++) {
                    printf("[%.*s] ", (int) lengths[i],
                           row[i] ? row[i] : "NULL");
                }
                printf("\n");
            }
            
            mysql_free_result(result);
        }
    }
    
    bool StateUserQuery::SetQueryTime(MYSQL *mysql, const state_log_time &time) {
        std::string query = GetQueryTimeQuery(time);
        
        if (mysql_query(mysql, query.c_str()) != 0) {
            error("[StateUserQuery::SetQueryTime] query error [%s] [%s]", query.c_str(), mysql_error(mysql));
            return false;
        }
        return true;
    }
    
    bool StateUserQuery::ResetQueryTime(MYSQL *mysql) {
        auto now = std::chrono::high_resolution_clock::now();
        
        return SetQueryTime(mysql,
                            state_log_time(
                                {std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count(),
                                 (ulong) (std::chrono::duration_cast<std::chrono::microseconds>(
                                     now.time_since_epoch()).count() % 1000000)}));
    }
    
    void StateUserQuery::AddQuery(std::shared_ptr<StateQuery> base, std::shared_ptr<StateQuery> add) {
        base->transactions.emplace_back(StateQuery::st_transaction(add.get()->transactions.front()));
    }
    
    void StateUserQuery::MergeTableSet(std::vector<std::string> &read_set, std::vector<std::string> &write_set) {
        StateUtil::unique_vector(read_set);
        StateUtil::unique_vector(write_set);
        
        for (auto &i: write_set) {
            auto iter = std::find(read_set.begin(), read_set.end(), i);
            if (iter != read_set.end()) {
                read_set.erase(iter);
            }
        }
    }
    
    void StateUserQuery::MergeQuery(std::shared_ptr<StateQuery> base) {
        SortTransaction(base.get());
        
        base->read_set.clear();
        base->write_set.clear();
        base->foreign_set.clear();
        base->range.clear();
        
        for (auto &i: base->transactions) {
            base->read_set.insert(base->read_set.end(), i.read_set.begin(), i.read_set.end());
            base->write_set.insert(base->write_set.end(), i.write_set.begin(), i.write_set.end());
            base->foreign_set.insert(base->foreign_set.end(), i.foreign_set.begin(), i.foreign_set.end());
            base->range.insert(base->range.end(), i.range.begin(), i.range.end());
        }
        
        MergeTableSet(base->read_set, base->write_set);
        StateForeign::MergeList(base->foreign_set);
        StateRange::OR_ARRANGE(base->range);
    }
    
    std::vector<std::shared_ptr<StateQuery>> StateUserQuery::SplitQuery(std::shared_ptr<StateQuery> query) {
        // SetQueryFile2 에서 사용자 쿼리는 여러 트랜잭션을 갖을수 있음
        // 시간을 기준으로 여러 트랜잭션으로 분리함
        
        std::vector<std::shared_ptr<StateQuery>> query_vec;
        if (query->transactions.size() == 0) {
            return query_vec;
        }
        
        auto curr_time = query->transactions.front().time;
        auto curr_query = std::make_shared<StateQuery>();
        
        for (const auto &i: query->transactions) {
            if (i.time == curr_time) {
                curr_query->transactions.emplace_back(i);
            } else {
                MergeQuery(curr_query);
                curr_query->MakeUpData();
                query_vec.push_back(curr_query);
                
                curr_time = i.time;
                curr_query = std::make_shared<StateQuery>();
                curr_query->transactions.emplace_back(i);
            }
        }
        
        MergeQuery(curr_query);
        curr_query->MakeUpData();
        query_vec.push_back(curr_query);
        
        return query_vec;
    }
    
    std::shared_ptr<StateQuery> StateUserQuery::MakeTransaction(const std::vector<std::shared_ptr<StateQuery>> &list) {
        if (list.size() == 0)
            return std::make_shared<StateQuery>();
        
        auto curr_query = *list.begin();
        
        for (auto iter = list.begin() + 1; iter != list.end(); ++iter) {
            AddQuery(curr_query, *iter);
        }
        
        MergeQuery(curr_query);
        
        return curr_query;
    }
    
    void StateUserQuery::SortTransaction(StateQuery *query) {
        std::sort(query->transactions.begin(),
                  query->transactions.end(),
                  [](const StateQuery::st_transaction &a, const StateQuery::st_transaction &b) {
                      return a.time < b.time;
                  });
    }
    
    int StateUserQuery::SetQueryString(const std::string &query) {
        std::vector<std::future<std::shared_ptr<StateQuery>>> query_result_vec;
        std::vector<std::shared_ptr<StateQuery>> query_vec;
        
        std::string::size_type pos = 0;
        while ((pos = query.find(';', pos)) != std::string::npos) {
            
            query_result_vec.emplace_back(
                StateThreadPool::Instance().EnqueueJob(
                    [](const std::string &db, const std::string &query) {
                        auto handle = StateThreadPool::Instance().PopMySql();
                        auto mysql = handle->handle().get();
                        
                        auto ret = StateUserQuery::ParseThread(mysql, db, query);
                        StateThreadPool::Instance().PushMySql(handle, true);
                        return ret;
                    },
                    database, query.substr(0, pos + 1)));
            
            ++pos;
        }
        
        int ret = EXIT_SUCCESS;
        for (auto &i: query_result_vec) {
            auto query = i.get();
            if (query == NULL) {
                ret = EXIT_FAILURE;
                break;
            }
            
            query->user_query = EN_USER_QUERY_CURRENT;
            query->MakeUpData();
            query_vec.emplace_back(query);
        }
        
        user_transaction_query = MakeTransaction(query_vec);
        user_transaction_query->MakeUpData();
        
        return ret;
    }
    
    int StateUserQuery::SetQueryFile2(const std::string &query_filepath) {
        std::vector<std::future<std::shared_ptr<StateQuery>>> query_result_vec;
        std::vector<std::shared_ptr<StateQuery>> add_query_vec;
        std::vector<std::shared_ptr<StateQuery>> del_query_vec;
        
        std::ifstream infile(query_filepath, std::ifstream::in);
        
        std::string buffer;
        std::string line;
        
        while (std::getline(infile, line)) {
            StateUtil::trim(line);
            if (line.empty())
                continue;
            if (line.find('#') == 0)
                continue;
            
            // 라인이 ; 로 끝나지 않으면 더 읽음
            if (line.rfind(delimiter) != line.length() - delimiter.length()) {
                buffer += (line + '\n');
                continue;
            }
                // ; 제거
            else {
                buffer += line.substr(0, line.length() - delimiter.length());
            }
            
            if (buffer.length() < 3) {
                error("[StateUserQuery::SetQueryFile2] buffer error: %s", buffer.c_str());
                return EXIT_FAILURE;
            }
            
            std::string cmd = buffer.substr(0, 3);
            buffer = buffer.substr(3);
            StateUtil::trim(buffer);
            
            if (StateUtil::iequals(cmd, "USE") == true) {
                std::vector<std::string> vec = StateUtil::space_split(buffer);
                
                if (vec.size() == 1) {
                    database = vec[0];
                    buffer.clear();
                    continue;
                } else {
                    error("[StateUserQuery::SetQueryFile2] db parsing error: %s", buffer.c_str());
                    return EXIT_FAILURE;
                }
            }
            
            int target_comma_count = 0;
            if (StateUtil::iequals(cmd, "ADD") == true) {
                target_comma_count = 2;
            } else if (StateUtil::iequals(cmd, "DEL") == true) {
                target_comma_count = 1;
            }
            
            if (target_comma_count == 0) {
                error("[StateUserQuery::SetQueryFile2] unknown command: %s", buffer.c_str());
                return EXIT_FAILURE;
            }
            
            std::string datetime;
            std::string query;
            std::string::size_type pos;
            
            pos = buffer.find(',');
            if (pos == std::string::npos) {
                error("[StateUserQuery::SetQueryFile2] datetime parsing error: %s", buffer.c_str());
                return EXIT_FAILURE;
            }
            buffer = buffer.substr(pos + 1);
            
            if (target_comma_count == 1) {
                datetime = buffer;
            } else // target_comma_count == 2
            {
                pos = buffer.find(',');
                if (pos == std::string::npos) {
                    error("[StateUserQuery::SetQueryFile2] query parsing error: %s", buffer.c_str());
                    return EXIT_FAILURE;
                }
                datetime = buffer.substr(0, pos);
                query = buffer.substr(pos + 1);
            }
            
            StateUtil::trim(datetime);
            StateUtil::trim(query);
            
            buffer.clear();
            
            // 실패시 종료됨
            state_log_time curr;
            // WARN: not implemented
            // convert_str_to_timestamp(datetime.c_str(), &curr);
            
            if (query.size() > 0) {
                auto query_list = StateUtil::split(delimiter, query);
                for (auto &i: query_list) {
                    query_result_vec.emplace_back(
                        StateThreadPool::Instance().EnqueueJob(
                            [](std::string db, std::string query, state_log_time time) {
                                auto handle = StateThreadPool::Instance().PopMySql();
                                auto mysql = handle->handle().get();
                                auto ret = StateUserQuery::ParseThread(mysql, db, query, true);
                                StateThreadPool::Instance().PushMySql(handle, true);
                                
                                if (ret == NULL) {
                                    error("[StateUserQuery::SetQueryFile2] query being ignored (%s)", query.c_str());
                                } else {
                                    ret->transactions.back().time = time;
                                }
                                
                                return ret;
                            },
                            database, i, curr));
                }
            } else {
                auto data = std::make_shared<StateQuery>();
                data->transactions.emplace_back(StateQuery::st_transaction(curr, SQLCOM_END, ""));
                del_query_vec.push_back(data);
            }
        }
        
        infile.close();
        
        int ret = EXIT_SUCCESS;
        for (auto &i: query_result_vec) {
            auto query = i.get();
            if (query == NULL) {
                ret = EXIT_FAILURE;
                break;
            }
            
            query->user_query = EN_USER_QUERY_CURRENT;
            add_query_vec.emplace_back(query);
        }
        
        user_add_transaction_query = MakeTransaction(add_query_vec);
        user_add_transaction_query->MakeUpData();
        SortTransaction(user_add_transaction_query.get());
        
        user_del_transaction_query = MakeTransaction(del_query_vec);
        user_del_transaction_query->MakeUpData();
        SortTransaction(user_del_transaction_query.get());
        
        debug("[StateUserQuery::SetQueryFile2] user query[%s] : add(%d) del(%d)",
              query_filepath.c_str(), add_query_vec.size(), del_query_vec.size());
        return ret;
    }
    
    int StateUserQuery::SetQueryFile(const std::string &query_filepath, bool is_analyze_all) {
        std::vector<std::future<std::shared_ptr<StateQuery>>> query_result_vec;
        std::vector<std::shared_ptr<StateQuery>> query_vec;
        
        std::ifstream infile(query_filepath, std::ifstream::in);
        
        std::string buffer;
        std::string line;
        std::string::size_type pos;
        
        auto change_db = [delimiter = delimiter](std::string &str, std::string &db) -> bool {
            std::vector<std::string> vec = StateUtil::space_split(str);
            
            if (vec.size() != 2)
                return false;
            
            if (StateUtil::iequals(vec[0], "use") == false)
                return false;
            
            db = vec[1];
            str.clear();
            
            std::string::size_type pos;
            if ((pos = db.find(delimiter)) != std::string::npos) {
                db = db.substr(0, pos);
            }
            
            return true;
        };
        
        auto change_delimiter = [](std::string &str, std::string &deli) -> bool {
            std::vector<std::string> vec = StateUtil::space_split(str);
            
            if (vec.size() != 2)
                return false;
            
            if (StateUtil::iequals(vec[0], "DELIMITER") == false)
                return false;
            
            deli = vec[1];
            str.clear();
            return true;
        };
        
        while (std::getline(infile, line)) {
            StateUtil::trim(line);
            if (line.empty())
                continue;
            if (line.find('#') == 0)
                continue;
            if (line.find("--") == 0)
                continue;
            
            buffer += (line + '\n');
            
            if (change_db(buffer, database) == true)
                continue;
            
            if (change_delimiter(buffer, delimiter) == true)
                continue;
            
            pos = buffer.find(delimiter);
            
            if (pos == std::string::npos) {
                continue;
            }
            
            if (pos > 0) {
                query_result_vec.emplace_back(
                    StateThreadPool::Instance().EnqueueJob(
                        [is_analyze_all](const std::string &db, const std::string &query) {
                            auto handle = StateThreadPool::Instance().PopMySql();
                            auto mysql = handle->handle().get();
                            auto ret = StateUserQuery::ParseThread(mysql, db, query, is_analyze_all);
                            StateThreadPool::Instance().PushMySql(handle, true);
                            return ret;
                        },
                        database, buffer.substr(0, pos)));
            }
            
            buffer.erase(0, pos + delimiter.size());
            StateUtil::trim(buffer);
        }
        
        infile.close();
        
        int ret = EXIT_SUCCESS;
        if (is_analyze_all) {
            for (auto &i: query_result_vec) {
                auto query = i.get();
                if (query == NULL) {
                    ret = EXIT_FAILURE;
                    break;
                }
                
                query->user_query = EN_USER_QUERY_CURRENT;
                query->MakeUpData();
                query_vec.clear();
                query_vec.emplace_back(query);
                auto t = MakeTransaction(query_vec);
                t->MakeUpData();
                user_transaction_query_list.emplace_back(t);
            }
        } else {
            for (auto &i: query_result_vec) {
                auto query = i.get();
                if (query == NULL) {
                    ret = EXIT_FAILURE;
                    break;
                }
                
                query->user_query = EN_USER_QUERY_CURRENT;
                query_vec.emplace_back(query);
            }
            
            user_transaction_query = MakeTransaction(query_vec);
            user_transaction_query->MakeUpData();
        }
        
        debug("[StateUserQuery::SetQueryFile] user query[%s] : %d", query_filepath.c_str(), query_result_vec.size());
        return ret;
    }
    
    std::shared_ptr<StateQuery>
    StateUserQuery::ParseThread(MYSQL *mysql, const std::string &db, const std::string &query, bool is_pass_all) {
        //debug("[StateUserQuery::ParseThread] mysql_state_query_analyze : %s", query.c_str());
        
        
        throw std::runtime_error("fixme: reimplement parseThread");
        unsigned char *ptr;
        size_t len;
        
        /*
        if (mysql_state_query_analyze(mysql, db.c_str(), query.c_str(), &ptr, &len) != 0) {
            error("[StateUserQuery::ParseThread] mysql_state_query_analyze return error (%s)", query.c_str());
            return NULL;
        }
         */
        
        state_log_hdr *hdr = (state_log_hdr *) ptr;
        if (len != (sizeof(state_log_hdr) +
                    hdr->query_length +
                    hdr->read_table_length +
                    hdr->write_table_length +
                    hdr->foreign_length +
                    hdr->data_column_item_length +
                    hdr->where_column_item_length)) {
            free(ptr);
            error("[StateUserQuery::ParseThread] data length error (%s)", query.c_str());
            return NULL;
        }
        
        auto ref = StateTable::ReadLogBlock((const char *) ptr);
        if (is_pass_all) {
            free(ptr);
            return ref;
        }
        free(ptr);
        
        if (ref->transactions.front().write_set.size() == 0) {
            error("[StateUserQuery::ParseThread] no write set (%s)", query.c_str());
            return NULL;
        }
        
        switch (ref->transactions.front().command) {
            case SQLCOM_UPDATE:
            case SQLCOM_UPDATE_MULTI:
            case SQLCOM_INSERT:
            case SQLCOM_INSERT_SELECT:
            case SQLCOM_DELETE:
            case SQLCOM_DELETE_MULTI:
                break;
            
            default:
                error("[StateUserQuery::ParseThread] not supported query type (%s)", query.c_str());
                return NULL;
        }
        
        return ref;
    }
    
    std::string StateUserQuery::GetBackupTableName(MYSQL *mysql, const std::string &db, const std::string &table,
                                                   const state_log_time &undo_time) {
        // 현재 테이블과 BACKUP DB 에 있는 테이블 목록 및 시스템 버저닝 시간 범위 추출
        std::string fullname = db + ".`" + table + "`";
        auto tlist = GetBackupTableList(mysql, db, table);
        if (tlist.size() == 0) {
            error("[StateUserQuery::GetBackupTableName] backup list failed");
            return fullname;
        }
        tlist.insert(tlist.begin(), fullname);
        
        auto rlist = GetVersionRange(mysql, tlist);
        if (tlist.size() != rlist.size()) {
            error("[StateUserQuery::GetBackupTableName] backup range list failed");
            return fullname;
        }
        
        // undo time 에 해당하는 가장 최근의 테이블 리턴
        for (auto iter = rlist.begin(); iter != rlist.end(); ++iter) {
            if (iter->start <= undo_time && iter->end >= undo_time) {
                return iter->name;
            }
        }
        
        // unto time 시점에 데이터가 존재하는 가장 최근의 테이블 리턴
        for (auto iter = rlist.begin(); iter != rlist.end(); ++iter) {
            if (iter->start <= undo_time) {
                return iter->name;
            }
        }
        
        // 못 찾으면 현재 테이블 사용
        return fullname;
    }
    
    std::vector<std::string>
    StateUserQuery::GetBackupTableList(MYSQL *mysql, const std::string &db, const std::string &table) {
        std::vector<std::string> tlist;
        
        std::stringstream ss;
        ss << "SELECT TABLE_NAME FROM information_schema.TABLES WHERE "
              "TABLE_SCHEMA = '" STATE_BACKUP_DATABASE "' AND "
              "TABLE_NAME REGEXP '"
           << db << "." << table << "_[0-9]{10}\\.[0-9]+' ORDER BY TABLE_NAME DESC";
        
        MYSQL_RES *query_result = NULL;
        MYSQL_ROW query_row;
        
        if (mysql_query(mysql, ss.str().c_str()) != 0 ||
            (query_result = mysql_store_result(mysql)) == NULL) {
            error("[StateUserQuery::GetBackupTableList] table select failed [%s] [%s]",
                  ss.str().c_str(), mysql_error(mysql));
            return tlist;
        }
        debug("[StateUserQuery::GetBackupTableList] %s", ss.str().c_str());
        
        while ((query_row = mysql_fetch_row(query_result))) {
            tlist.emplace_back(STATE_BACKUP_DATABASE ".`" + std::string(query_row[0]) + "`");
        }
        mysql_free_result(query_result);
        
        return tlist;
    }
    
    std::mutex mutex;
    
    std::vector<StateUserQuery::Range>
    StateUserQuery::GetVersionRange(MYSQL *mysql, const std::vector<std::string> &tlist) {
        std::unique_lock<std::mutex> lock(mutex);
        std::vector<StateUserQuery::Range> rlist;
        std::stringstream ss;
        
        for (auto &i: tlist) {
            ss.str(std::string());
            ss << "SELECT "
                  "UNIX_TIMESTAMP(MIN(ROW_START)), "
                  "(SELECT UNIX_TIMESTAMP(MAX(ROW_END)) FROM "
               << i << " FOR SYSTEM_TIME ALL WHERE ROW_END < DATE('2038-01-19 12:14:07.999999')) "
                       "FROM "
               << i << " FOR SYSTEM_TIME ALL";
            
            MYSQL_RES *query_result = NULL;
            MYSQL_ROW query_row;
            
            if (mysql_query(mysql, ss.str().c_str()) != 0 ||
                (query_result = mysql_store_result(mysql)) == NULL) {
                error("[StateUserQuery::GetBackupRange] table select failed [%s] [%s]",
                      ss.str().c_str(), mysql_error(mysql));
                return rlist;
            }
            debug("[StateUserQuery::GetBackupRange] %s", ss.str().c_str());
            
            while ((query_row = mysql_fetch_row(query_result))) {
                state_log_time start = {0, 0}, end = {0, 0};
                size_t pos = 0;
                
                if (query_row[0] != NULL) {
                    pos = 0;
                    start.sec = std::stol(query_row[0], &pos, 10);
                    start.sec_part = std::stoul(query_row[0] + pos + 1, NULL, 10);
                }
                
                if (query_row[1] != NULL) {
                    pos = 0;
                    end.sec = std::stol(query_row[1], &pos, 10);
                    end.sec_part = std::stoul(query_row[1] + pos + 1, NULL, 10);
                }
                
                rlist.emplace_back(i, start, end);
            }
            mysql_free_result(query_result);
        }
        
        return rlist;
    }
}