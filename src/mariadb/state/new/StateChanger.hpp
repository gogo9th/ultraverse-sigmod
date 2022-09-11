//
// Created by cheesekun on 8/29/22.
//

#ifndef ULTRAVERSE_STATECHANGER_HPP
#define ULTRAVERSE_STATECHANGER_HPP

#include "Transaction.hpp"
#include "StateLogReader.hpp"
#include "StateChangeContext.hpp"

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

    private:
        std::string _dbName;
        gid_t _rollbackGid;
        std::string _userQueryPath;
        
        std::string _dbdumpPath;
        std::string _binlogPath;
        std::string _stateLogPath;
        
        bool _isDryRun;
    };
    
    class StateChanger {
    public:
        static const std::string QUERY_TAG_STATECHANGE;
        
        StateChanger(const StateChangePlan &plan);
        
        void prepare();
        void explain();
        void start(uint64_t nodeIdx);
        
    private:
        void processDDLTransaction(std::shared_ptr<Transaction> transaction);
        
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
        mariadb::DBHandle _dbHandle;
        
        const StateChangePlan &_plan;
        std::string _intermediateDBName;
        
        StateLogReader _reader;
        
        StateGraphBoost _stateGraph;
        std::shared_ptr<Transaction> _rollbackTarget;
        
        std::shared_ptr<StateChangeContext> _context;
        
        bool _isRunning;
        std::vector<std::thread> _executorThreads;
    };
}


#endif //ULTRAVERSE_STATECHANGER_HPP
