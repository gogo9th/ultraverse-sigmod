#ifndef STATE_USER_QUERY_INCLUDED
#define STATE_USER_QUERY_INCLUDED

#include <memory>
#include <string>
#include <vector>
#include <future>
#include <functional>

#include <mysql/mysql.h>

#include "StateQuery.hpp"
#include "StateTaskContext.hpp"


namespace ultraverse::state {
    
    class StateGraph;
    
    class StateUserQuery {
    private:
        class Range {
        public:
            Range(const std::string &name,
                  const state_log_time &start,
                  const state_log_time &end)
                : name(name), start(start), end(end) {}
            
            std::string name;
            state_log_time start;
            state_log_time end;
        };
    
    public:
        enum enum_tx_isolation {
            ISO_READ_UNCOMMITTED, ISO_READ_COMMITTED,
            ISO_REPEATABLE_READ, ISO_SERIALIZABLE
        };
        
        StateUserQuery();
        
        ~StateUserQuery();
        
        int SetQueryString(const std::string &query);
        
        int SetQueryFile(const std::string &query_filepath, bool is_analyze_all = false);
        
        int SetQueryFile2(const std::string &query_filepath);
        
        const std::shared_ptr<StateQuery> GetUserTransaction();
        
        const std::shared_ptr<StateQuery> GetUserAddTransaction();
        
        const std::shared_ptr<StateQuery> GetUserDelTransaction();
        
        const std::vector<std::shared_ptr<StateQuery>> GetUserTransactionList();
        
        std::string GetDatabase();
        
        static std::shared_ptr<StateQuery> MakeTransaction(const std::vector<std::shared_ptr<StateQuery>> &list);
        
        static void SortTransaction(StateQuery *query);
        
        static void MergeTableSet(std::vector<std::string> &read_set, std::vector<std::string> &write_set);
        
        static void MergeQuery(std::shared_ptr<StateQuery> base);
        
        static void AddQuery(std::shared_ptr<StateQuery> base, std::shared_ptr<StateQuery> add);
        
        static std::vector<std::shared_ptr<StateQuery>> SplitQuery(std::shared_ptr<StateQuery> query);
        
        static std::string MakeFullTableName(const std::string &t);
        
        static bool
        SplitDBNameAndTableName(const std::string &in_fullname, std::string &out_dbname, std::string &out_tablename);
        
        static std::vector<std::string> SplitDBNameAndTableName(const std::string &in_fullname);
        
        static void ReplaceQuery(StateQuery *query);
        
        static bool
        ReplaceQuery(const std::vector<std::tuple<std::string, std::string>> &oldname_list, std::string &query);
        
        static size_t RunQuery(StateQuery *query);
        
        static size_t RunBulkQuery(StateTaskContext &context, StateQuery *query);
        
        static std::string GetQueryTimeQuery(const state_log_time &time);
        
        static void QueryAndPrint(MYSQL *mysql, const std::string &query);
        
        static void ClearResult(MYSQL *mysql);
        
        static bool SetIsolationLevel(MYSQL *mysql, enum_tx_isolation level);
        
        static bool CreateSavePoint(MYSQL *mysql, const std::string &id);
        
        static bool ReleaseSavePoint(MYSQL *mysql, const std::string &id);
        
        static bool RollbackToSavePoint(MYSQL *mysql, const std::string &id);
        
        static bool SetQueryStateXid(MYSQL *mysql, const uint64_t xid);
        
        static bool SetQueryTime(MYSQL *mysql, const state_log_time &time);
        
        static bool ResetQueryTime(MYSQL *mysql);
        
        static bool SetForeignKeyChecks(MYSQL *mysql, bool value);
        
        static bool IsExistsTable(MYSQL *mysql, const std::string &db, const std::string &table);
        
        static std::string GetBackupTableName(MYSQL *mysql, const std::string &db, const std::string &table,
                                              const state_log_time &undo_time);
    
    private:
        static std::shared_ptr<StateQuery>
        ParseThread(MYSQL *mysql, const std::string &db, const std::string &query, bool is_pass_all = false);
        
        static std::vector<std::string>
        GetBackupTableList(MYSQL *mysql, const std::string &db, const std::string &table);
        
        static std::vector<StateUserQuery::Range> GetVersionRange(MYSQL *mysql, const std::vector<std::string> &tlist);
        
        static size_t RunQueryLow(StateQuery *query, std::function<void(MYSQL *, StateQuery *)> init_func,
                                  std::function<void(MYSQL *, StateQuery *)> exit_func);
        
        
        std::string database;
        std::string delimiter;
        std::shared_ptr<StateQuery> user_transaction_query;
        std::shared_ptr<StateQuery> user_add_transaction_query;
        std::shared_ptr<StateQuery> user_del_transaction_query;
        std::vector<std::shared_ptr<StateQuery>> user_transaction_query_list;
    };
}

#endif /* STATE_USER_QUERY_INCLUDED */
