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

#include "cluster/CandidateColumn.hpp"
#include "cluster/RowCluster.hpp"

#include "base/DBHandlePool.hpp"
#include "mariadb/state/StateGraphBoost.h"
#include "mariadb/DBHandle.hpp"
#include "utils/log.hpp"


namespace ultraverse::state::v2 {
    class StateChanger {
    public:
        static const std::string QUERY_TAG_STATECHANGE;
        
        StateChanger(DBHandlePool<mariadb::DBHandle> &dbHandlePool, const StateChangePlan &plan);
        
        std::string findCandidateColumn();
        
        void prepare();
        void start();
        
    private:
        constexpr static int CLUSTER_EXPAND_FLAG_NO_FLAGS    = 0;
        constexpr static int CLUSTER_EXPAND_FLAG_STRICT      = 0b01;
        constexpr static int CLUSTER_EXPAND_FLAG_INCLUDE_FK  = 0b10;
        constexpr static int CLUSTER_EXPAND_FLAG_WILDCARD    = 0b100;
        constexpr static int CLUSTER_EXPAND_FLAG_DONT_EXPAND = 0b1000;
        
        void expandClusterMap(RowCluster &rowCluster, Transaction &transaction, int flags);
        
        void processDDLTransaction(std::shared_ptr<Transaction> transaction);
        void processNode(uint64_t nodeIdx);
        
        void __node__processTransaction(
            uint64_t rootNodeId,
            uint64_t nodeId,
            std::shared_ptr<Transaction> transaction,
            mariadb::DBHandle &dbHandle
        );
        
        inline void __node__replayQuery(
            uint64_t rootNodeId,
            uint64_t nodeId,
            std::shared_ptr<Query> query,
            mariadb::DBHandle &dbHandle
        );
        
        bool isTransactionRelatedToPlan(std::shared_ptr<Transaction> transaction) const;
        bool isTransactionRelatedToCluster(std::shared_ptr<Transaction> transaction) const;
        
        std::vector<CandidateColumn>
        buildCandidateColumnList(std::shared_ptr<Transaction> transaction) const;
        
        
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
        
        bool isQueryRelatedWithKeyColumns(Query &query);
        
        int64_t getAutoIncrement(mariadb::DBHandle &dbHandle, std::string table);
        void setAutoIncrement(mariadb::DBHandle &dbHandle, std::string table, int64_t value);
        
        LoggerPtr _logger;
        
        DBHandlePool<mariadb::DBHandle> &_dbHandlePool;
        
        StateChangePlan _plan;
        std::string _intermediateDBName;
        
        StateLogReader _reader;
        
        std::unique_ptr<StateGraphBoost> _stateGraph;
        std::shared_ptr<Transaction> _rollbackTarget;
        // FIXME: keyRanges는 map<keyColumn, StateRange>로 바뀌어야 하는게 맞음
        std::shared_ptr<std::map<std::string, std::vector<std::shared_ptr<StateRange>>>> _keyRanges;
        std::shared_ptr<std::vector<size_t>> _columnSetHashes;
        
        std::shared_ptr<StateChangeContext> _context;
        
        bool _isRunning;
        std::vector<std::thread> _executorThreads;
    
    
        std::unordered_map<std::string, state::StateHash> _stateHashMap;
        
        std::mutex _clusterMutex;
        std::condition_variable _clusterCondvar;
        bool _isClusterReady;
        
        RowCluster _rowCluster;
        // FIXME: 네이밍
        RowCluster _rowCluster2;
        
        std::mutex _clusterMutex2;
        
        std::unique_ptr<ColumnDependencyGraph> _columnGraph;
        std::unique_ptr<TableDependencyGraph> _tableGraph;
        std::unique_ptr<HashWatcher> _hashWatcher;
    
        std::mutex _changedTablesMutex;
        std::unordered_set<std::string> _changedTables;
        
        gid_t _ddlTxnId;
        gid_t _ddlTxnProcessedId;
    };
}


#endif //ULTRAVERSE_STATECHANGER_HPP
