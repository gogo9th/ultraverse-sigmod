//
// Created by cheesekun on 8/29/22.
//

#ifndef ULTRAVERSE_STATECHANGER_HPP
#define ULTRAVERSE_STATECHANGER_HPP

#include <string>

#include "Transaction.hpp"
#include "StateLogReader.hpp"
#include "StateChangeContext.hpp"
#include "StateChangePlan.hpp"
#include "ColumnDependencyGraph.hpp"
#include "HashWatcher.hpp"
#include "ProcLogReader.hpp"
#include "ProcMatcher.hpp"

#include "cluster/CandidateColumn.hpp"
#include "cluster/RowCluster.hpp"

#include "base/DBHandlePool.hpp"
#include "mariadb/state/StateGraphBoost.h"
#include "mariadb/DBHandle.hpp"
#include "utils/log.hpp"
#include "mariadb/state/new/graph/RowGraph.hpp"


namespace ultraverse::state::v2 {
    namespace OperationMode {
        enum Value {
            NORMAL,
            PREPARE,
            FULL_REPLAY
        };
    }
    
    class StateChanger {
    public:
        static const std::string QUERY_TAG_STATECHANGE;
        
        StateChanger(DBHandlePool<mariadb::DBHandle> &dbHandlePool, const StateChangePlan &plan);
        
        void makeCluster();
        
        void prepare();
        void replay();
        
        void fullReplay();
        
    private:
        constexpr static int CLUSTER_EXPAND_FLAG_NO_FLAGS    = 0;
        constexpr static int CLUSTER_EXPAND_FLAG_STRICT      = 0b01;
        constexpr static int CLUSTER_EXPAND_FLAG_INCLUDE_FK  = 0b10;
        constexpr static int CLUSTER_EXPAND_FLAG_WILDCARD    = 0b100;
        constexpr static int CLUSTER_EXPAND_FLAG_DONT_EXPAND = 0b1000;
        
        void replayThreadMain(int workerId, RowGraph &rowGraph);
        
        std::shared_ptr<Transaction> loadUserQuery(const std::string &path);
        std::shared_ptr<Transaction> parseUserQuery(const std::vector<std::string> &queries);
        
        void loadBackup(const std::string &dbName, const std::string &fileName);
        
        /**
         * creates intermediate database.
         */
        void createIntermediateDB();
        
        /**
         * drops intermediate database.
         * called when task has failed?
         */
        void dropIntermediateDB();
        
        /**
         * updates primary keys
         */
        void updatePrimaryKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp);
        
        /**
         * updates foreign keys
         */
        void updateForeignKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp);
        
        int64_t getAutoIncrement(mariadb::DBHandle &dbHandle, std::string table);
        void setAutoIncrement(mariadb::DBHandle &dbHandle, std::string table, int64_t value);
        
        LoggerPtr _logger;
        
        DBHandlePool<mariadb::DBHandle> &_dbHandlePool;
        
        StateChangePlan _plan;
        OperationMode::Value _mode;
        
        std::string _intermediateDBName;
        
        StateLogReader _reader;
        
        std::shared_ptr<StateChangeContext> _context;
        
        std::atomic_bool _isRunning;
        std::vector<std::thread> _executorThreads;
        
        
        /** REMOVE ME */
        std::unordered_map<std::string, state::StateHash> _stateHashMap;
        
        std::unique_ptr<ColumnDependencyGraph> _columnGraph;
        std::unique_ptr<TableDependencyGraph> _tableGraph;
        
        std::unique_ptr<HashWatcher> _hashWatcher;
        std::unique_ptr<ProcLogReader> _procLogReader;
        
        std::mutex _changedTablesMutex;
        std::unordered_set<std::string> _changedTables;
        
        /** REMOVE ME */
        std::atomic_uint64_t _replayedQueries;
        std::atomic_uint64_t _replayedTxns;
        
        double _phase1Time;
        double _phase2Time;
    };
}


#endif //ULTRAVERSE_STATECHANGER_HPP
