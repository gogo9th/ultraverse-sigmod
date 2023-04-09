//
// Created by cheesekun on 10/27/22.
//

#include "StateChangePlan.hpp"

namespace ultraverse::state::v2 {
    StateChangePlan::StateChangePlan():
        _startGid(0),
        _writeStateLog(false)
    {
    
    }
    
    const std::string &StateChangePlan::dbHost() const {
        return _dbHost;
    }
    
    void StateChangePlan::setDBHost(const std::string &dbHost) {
        _dbHost = dbHost;
    }
    
    const std::string &StateChangePlan::dbUsername() const {
        return _dbUsername;
    }
    
    void StateChangePlan::setDBUsername(const std::string &dbUsername) {
        _dbUsername = dbUsername;
    }
    
    const std::string &StateChangePlan::dbPassword() const {
        return _dbPassword;
    }
    
    void StateChangePlan::setDBPassword(const std::string &dbPassword) {
        _dbPassword = dbPassword;
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
    
    gid_t StateChangePlan::endGid() const {
        return _endGid;
    }
    
    void StateChangePlan::setEndGid(gid_t endGid) {
        _endGid = endGid;
    }
    
    std::vector<gid_t> &StateChangePlan::rollbackGids() {
        return _rollbackGids;
    }
    
    std::map<gid_t, std::string> &StateChangePlan::userQueries() {
        return _userQueries;
    }
    
    gid_t StateChangePlan::lowestGidAvailable() const {
        if (_rollbackGids.empty()) {
            return _userQueries.begin()->first;
        }
        
        if (_userQueries.empty()) {
            return _rollbackGids[0];
        }
        
        return std::min(
            _rollbackGids[0],
            _userQueries.begin()->first
        );
    }
    
    bool StateChangePlan::isRollbackGid(gid_t gid) const {
        return std::find(_rollbackGids.begin(), _rollbackGids.end(), gid) != _rollbackGids.end();
    }
    
    bool StateChangePlan::hasUserQuery(gid_t gid) const {
        return _userQueries.find(gid) != _userQueries.end();
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

    const std::string &StateChangePlan::procCallLogPath() const {
        return _procCallLogPath;
    }

    void StateChangePlan::setProcCallLogPath(const std::string &procCallLogPath) {
        _procCallLogPath = procCallLogPath;
    }

    bool StateChangePlan::writeStateLog() const {
        return _writeStateLog;
    }
    
    void StateChangePlan::setWriteStateLog(bool writeStateLog) {
        _writeStateLog = writeStateLog;
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