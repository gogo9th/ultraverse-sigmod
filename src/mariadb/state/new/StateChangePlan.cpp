//
// Created by cheesekun on 10/27/22.
//

#include "StateChangePlan.hpp"

namespace ultraverse::state::v2 {
    StateChangePlan::StateChangePlan():
        _startGid(0)
    {
    
    }
    
    OperationMode StateChangePlan::mode() const {
        return _operationMode;
    }
    
    void StateChangePlan::setMode(OperationMode mode) {
        _operationMode = mode;
    }
    
    const std::string &StateChangePlan::dbName() const {
        return _dbName;
    }
    
    void StateChangePlan::setDBName(const std::string &dbName) {
        _dbName = dbName;
    }
    
    gid_t StateChangePlan::startGid() const {
        return _startGid;
    }
    
    void StateChangePlan::setStartGid(gid_t startGid) {
        _startGid = startGid;
    }
    
    gid_t StateChangePlan::rollbackGid() const {
        return _rollbackGid;
    }
    
    void StateChangePlan::setRollbackGid(gid_t rollbackGid) {
        _rollbackGid = rollbackGid;
    }
    
    gid_t StateChangePlan::endGid() const {
        return _endGid;
    }
    
    void StateChangePlan::setEndGid(gid_t endGid) {
        _endGid = endGid;
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
    
    std::vector<uint64_t> &StateChangePlan::skipGids() {
        return _skipGids;
    }
    
    const std::vector<uint64_t> &StateChangePlan::skipGids() const {
        return _skipGids;
    }
}