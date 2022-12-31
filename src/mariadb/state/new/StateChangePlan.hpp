//
// Created by cheesekun on 10/27/22.
//

#ifndef ULTRAVERSE_STATECHANGEPLAN_HPP
#define ULTRAVERSE_STATECHANGEPLAN_HPP

#include <string>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    
    enum OperationMode {
        ROLLBACK,
        APPEND_ONLY
    };
    
    class StateChangePlan {
    public:
        explicit StateChangePlan();
        
        OperationMode mode() const;
        void setMode(OperationMode mode);
        
        const std::string &dbHost() const;
        void setDBHost(const std::string &dbHost);
    
        const std::string &dbUsername() const;
        void setDBUsername(const std::string &dbUsername);
        
        const std::string &dbPassword() const;
        void setDBPassword(const std::string &dbPassword);
    
        const std::string &dbName() const;
        void setDBName(const std::string &dbName);
        
        gid_t startGid() const;
        void setStartGid(gid_t startGid);
        
        gid_t rollbackGid() const;
        void setRollbackGid(gid_t rollbackGid);
        
        gid_t endGid() const;
        void setEndGid(gid_t endGid);
        
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
        
        bool writeStateLog() const;
        void setWriteStateLog(bool writeStateLog);
        
        bool isDryRun() const;
        void setDryRun(bool isDryRun);
        
        std::vector<std::string> &keyColumns();
        std::vector<std::pair<std::string, std::string>> &columnAliases();
        
        std::vector<uint64_t> &skipGids();
        [[nodiscard]]
        const std::vector<uint64_t> &skipGids() const;
    
    private:
        OperationMode _operationMode;
        
        std::string _dbHost;
        std::string _dbUsername;
        std::string _dbPassword;
        std::string _dbName;
        
        gid_t _startGid;
        gid_t _rollbackGid;
        gid_t _endGid;
        std::string _userQueryPath;
        
        std::string _dbdumpPath;
        std::string _binlogPath;
        std::string _stateLogPath;
        std::string _stateLogName;
        
        bool _writeStateLog;
        
        std::vector<std::string> _keyColumns;
        std::vector<std::pair<std::string, std::string>> _columnAliases;
    
        std::vector<uint64_t> _skipGids;
        bool _isDryRun;
        
    };
    
}

#endif //ULTRAVERSE_STATECHANGEPLAN_HPP
