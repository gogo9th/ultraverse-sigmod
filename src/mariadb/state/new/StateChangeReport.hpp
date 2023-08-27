//
// Created by cheesekun on 8/27/23.
//

#ifndef ULTRAVERSE_STATECHANGEREPORT_HPP
#define ULTRAVERSE_STATECHANGEREPORT_HPP

#include <string>
#include <set>

#include "StateChangePlan.hpp"

namespace ultraverse::state::v2 {
    class StateChangeReport {
    public:
        enum OperationType {
            MAKE_CLUSTER,
            PREPARE,
            EXECUTE,
        };
        
        static std::string operationTypeToString(OperationType operationType);
        
        explicit StateChangeReport(OperationType operationType, StateChangePlan &plan);
        
        void setDBName(const std::string &dbName);
        void setIntermediateDBName(const std::string &intermediateDBName);
        
        void setReplayGidCount(size_t replayGidCount);
        void setTotalCount(size_t totalCount);
        
        void setReplaceQuery(const std::string &replaceQuery);
        
        void setSQLLoadTime(double sqlLoadTime);
        void setExecutionTime(double executionTime);
        
        std::string writeToJSON();
        void writeToJSON(const std::string &outputPath);
    private:
        OperationType _operationType;
        
        std::string _dbName;
        std::string _intermediateDBName;
        
        /* PREPARE */
        std::vector<gid_t> _rollbackGids;
        std::string _replaceQuery;
        
        size_t _replayGidCount;
        size_t _totalCount;
        
        /* PREPARE / EXECUTE */
        
        /* EXECUTE */
        
        /* COMMON */
        double _sqlLoadTime;
        double _executionTime;
        
        int _threadNum;
    };
}

#endif //ULTRAVERSE_STATECHANGEREPORT_HPP
