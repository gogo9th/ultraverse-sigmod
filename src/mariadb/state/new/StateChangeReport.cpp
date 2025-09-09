//
// Created by cheesekun on 8/27/23.
//

#include <fstream>
#include <nlohmann/json.hpp>

#include "StateChangeReport.hpp"

namespace ultraverse::state::v2 {
    std::string StateChangeReport::operationTypeToString(StateChangeReport::OperationType operationType) {
        switch (operationType) {
            case MAKE_CLUSTER:
                return "MAKE_CLUSTER";
            case PREPARE:
                return "PREPARE";
            case PREPARE_AUTO:
                return "PREPARE_AUTO";
            case EXECUTE:
                return "EXECUTE";
        }
        
        return "UNKNOWN";
    }
    
    StateChangeReport::StateChangeReport(StateChangeReport::OperationType operationType, StateChangePlan &plan):
        _operationType(operationType),
        _dbName(plan.dbName()),
        _threadNum(plan.threadNum()),
        _rollbackGids(plan.rollbackGids()),
        
        _sqlLoadTime(0.0),
        _executionTime(0.0)
    {
    
    }
    
    void StateChangeReport::setDBName(const std::string &dbName) {
        _dbName = dbName;
    }
    
    void StateChangeReport::setIntermediateDBName(const std::string &intermediateDBName) {
        _intermediateDBName = intermediateDBName;
    }
    
    void StateChangeReport::setReplayGidCount(size_t replayGidCount) {
        _replayGidCount = replayGidCount;
    }
    
    void StateChangeReport::setTotalCount(size_t totalCount) {
        _totalCount = totalCount;
    }
    
    void StateChangeReport::setReplaceQuery(const std::string &replaceQuery) {
        _replaceQuery = replaceQuery;
    }
    
    void StateChangeReport::setSQLLoadTime(double sqlLoadTime) {
        _sqlLoadTime = sqlLoadTime;
    }
    
    void StateChangeReport::setExecutionTime(double executionTime) {
        _executionTime = executionTime;
    }
    
    void StateChangeReport::bench_setRollbackGids(const std::set<gid_t> &rollbackGids) {
        _rollbackGids.insert(_rollbackGids.end(), rollbackGids.begin(), rollbackGids.end());
    }
    
    void StateChangeReport::bench_setTotalQueryCount(size_t totalQueryCount) {
        _totalQueryCount = totalQueryCount;
    }
    
    void StateChangeReport::bench_setReplayQueryCount(size_t replayQueryCount) {
        _replayQueryCount = replayQueryCount;
    }
    
    std::string StateChangeReport::writeToJSON() {
        using namespace nlohmann;
        json document;
        
        document.emplace("operationType", operationTypeToString(_operationType));
        document.emplace("intermediateDBName", _intermediateDBName);
        
        if (_operationType == PREPARE) {
            json rollbackGids;
            for (const auto &gid: _rollbackGids) {
                rollbackGids.emplace_back(gid);
            }
            
            document.emplace("rollbackGids", rollbackGids);
            document.emplace("replaceQuery", _replaceQuery);
            document.emplace("replayGidCount", _replayGidCount);
            document.emplace("totalCount", _totalCount);
        }
        
        if (_operationType == PREPARE_AUTO) {
            json rollbackGids;
            for (const auto &gid: _rollbackGids) {
                rollbackGids.emplace_back(gid);
            }
            
            document.emplace("totalQueryCount", _totalQueryCount);
            document.emplace("replayQueryCount", _replayQueryCount);
            document.emplace("rollbackGids", rollbackGids);
            document.emplace("replayGidCount", _replayGidCount);
            document.emplace("totalCount", _totalCount);
        }
        
        document.emplace("sqlLoadTime", _sqlLoadTime);
        document.emplace("executionTime", _executionTime);
        document.emplace("threadNum", _threadNum);
        
        return document.dump(4);
    }
    
    void StateChangeReport::writeToJSON(const std::string &outputPath) {
        std::ofstream outputStream(outputPath);
        outputStream << writeToJSON();
        outputStream.close();
    }
}