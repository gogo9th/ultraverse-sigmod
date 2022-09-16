//
// Created by cheesekun on 8/29/22.
//

#ifndef ULTRAVERSE_STATECHANGER_HPP
#define ULTRAVERSE_STATECHANGER_HPP

#include <string>

#include "Transaction.hpp"
#include "StateLogReader.hpp"
#include "StateChangeContext.hpp"

#include "cluster/CandidateColumn.hpp"
#include "cluster/RowCluster.hpp"

#include "base/DBHandlePool.hpp"
#include "mariadb/state/StateGraphBoost.h"
#include "mariadb/DBHandle.hpp"
#include "utils/log.hpp"

namespace ultraverse::state::v2 {
    class StateChangePlan {
    public:
        explicit StateChangePlan();
    
        const std::string &dbName() const;
        void setDBName(const std::string &dbName);
    
        gid_t rollbackGid() const;
        void setRollbackGid(gid_t rollbackGid);
    
        const std::string &userQueryPath() const;
        void setUserQueryPath(const std::string &userQueryPath);
    
        const std::string &dbDumpPath() const;
        void setDBDumpPath(const std::string &dbdumpPath);
    
        const std::string &binlogPath() const;
        void setBinlogPath(const std::string &binlogPath);
    
        const std::string &stateLogPath() const;
        void setStateLogPath(const std::string &stateLogPath);
        
        bool isDryRun() const;
        void setDryRun(bool isDryRun);
        
        std::vector<std::string> &keyColumns();

    private:
        std::string _dbName;
        gid_t _rollbackGid;
        std::string _userQueryPath;
        
        std::string _dbdumpPath;
        std::string _binlogPath;
        std::string _stateLogPath;
        
        std::vector<std::string> _keyColumns;
        bool _isDryRun;
    };
    
    class StateChanger {
    public:
        static const std::string QUERY_TAG_STATECHANGE;
        
        StateChanger(DBHandlePool<mariadb::DBHandle> &dbHandlePool, const StateChangePlan &plan);
        
        std::string findCandidateColumn();
        void start();
        
    private:
        void setRollbackTarget(std::shared_ptr<Transaction> transaction);
        
        void processDDLTransaction(std::shared_ptr<Transaction> transaction);
        void processNode(uint64_t nodeIdx);
        
        bool isTransactionRelatedToPlan(std::shared_ptr<Transaction> transaction) const;
        
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
        
        
        LoggerPtr _logger;
        
        DBHandlePool<mariadb::DBHandle> &_dbHandlePool;
        
        StateChangePlan _plan;
        std::string _intermediateDBName;
        
        StateLogReader _reader;
        
        std::unique_ptr<StateGraphBoost> _stateGraph;
        std::shared_ptr<Transaction> _rollbackTarget;
        
        std::shared_ptr<StateChangeContext> _context;
        
        bool _isRunning;
        std::vector<std::thread> _executorThreads;
    
    
        std::mutex _stateHashMutex;
        std::unordered_map<std::string, state::StateHash> _stateHashMap;
    
        std::mutex _clusterMutex;
        std::condition_variable _clusterCondvar;
        bool _isClusterReady;
        
        RowCluster _rowCluster;
    };
}


#endif //ULTRAVERSE_STATECHANGER_HPP
