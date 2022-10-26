//
// Created by cheesekun on 10/27/22.
//

#ifndef ULTRAVERSE_STATECHANGEPLAN_HPP
#define ULTRAVERSE_STATECHANGEPLAN_HPP

#include <string>

#include "Transaction.hpp"

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
        
        bool isDBDumpAvailable() const;
        const std::string &dbDumpPath() const;
        void setDBDumpPath(const std::string &dbdumpPath);
        
        const std::string &binlogPath() const;
        void setBinlogPath(const std::string &binlogPath);
        
        const std::string &stateLogPath() const;
        void setStateLogPath(const std::string &stateLogPath);
        
        const std::string &stateLogName() const;
        void setStateLogName(const std::string &stateLogName);
        
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
        std::string _stateLogName;
        
        std::vector<std::string> _keyColumns;
        bool _isDryRun;
    };
    
}

#endif //ULTRAVERSE_STATECHANGEPLAN_HPP
