//
// Created by cheesekun on 10/27/22.
//

#include "StateChangePlan.hpp"

namespace ultraverse::state::v2 {
    StateChangePlan::StateChangePlan() {
    
    }
    
    const std::string &StateChangePlan::dbName() const {
        return _dbName;
    }
    
    void StateChangePlan::setDBName(const std::string &dbName) {
        _dbName = dbName;
    }
    
    gid_t StateChangePlan::rollbackGid() const {
        return _rollbackGid;
    }
    
    void StateChangePlan::setRollbackGid(gid_t rollbackGid) {
        _rollbackGid = rollbackGid;
    }
    
    const std::string &StateChangePlan::userQueryPath() const {
        return _userQueryPath;
    }
    
    void StateChangePlan::setUserQueryPath(const std::string &userQueryPath) {
        _userQueryPath = userQueryPath;
    }
    
    bool StateChangePlan::isDBDumpAvailable() const {
        return !_dbdumpPath.empty();
    }
    
    const std::string &StateChangePlan::dbDumpPath() const {
        return _dbdumpPath;
    }
    
    void StateChangePlan::setDBDumpPath(const std::string &dbdumpPath) {
        _dbdumpPath = dbdumpPath;
    }
    
    const std::string &StateChangePlan::binlogPath() const {
        return _binlogPath;
    }
    
    void StateChangePlan::setBinlogPath(const std::string &binlogPath) {
        _binlogPath = binlogPath;
    }
    
    const std::string &StateChangePlan::stateLogPath() const {
        return _stateLogPath;
    }
    
    void StateChangePlan::setStateLogPath(const std::string &stateLogPath) {
        _stateLogPath = stateLogPath;
    }
    
    const std::string &StateChangePlan::stateLogName() const {
        return _stateLogName;
    }
    
    void StateChangePlan::setStateLogName(const std::string &stateLogName) {
        _stateLogName = stateLogName;
    }
    
    bool StateChangePlan::isDryRun() const {
        return _isDryRun;
    }
    
    void StateChangePlan::setDryRun(bool isDryRun) {
        _isDryRun = isDryRun;
    }
    
    std::vector<std::string> &StateChangePlan::keyColumns() {
        return _keyColumns;
    }
    
    std::vector<std::pair<std::string, std::string>> &StateChangePlan::columnAliases() {
        return _columnAliases;
    }
}