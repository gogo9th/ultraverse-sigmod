//
// Created by cheesekun on 7/14/23.
//

#ifndef ULTRAVERSE_STATECLUSTERWRITER_HPP
#define ULTRAVERSE_STATECLUSTERWRITER_HPP

#include <fstream>
#include <mutex>

#include "TableDependencyGraph.hpp"
#include "cluster/StateCluster.hpp"

namespace ultraverse::state::v2 {
    class StateClusterWriter {
    public:
        StateClusterWriter(const std::string &logPath, const std::string &logName);
        
        void operator<<(StateCluster &cluster);
        void operator<<(TableDependencyGraph &graph);
        
        void operator>>(StateCluster &cluster);
        void operator>>(TableDependencyGraph &graph);
        
        void writeCluster(StateCluster &cluster);
        void writeTableDependencyGraph(TableDependencyGraph &graph);
        
        void readCluster(StateCluster &cluster);
        void readTableDependencyGraph(TableDependencyGraph &graph);
        
    private:
        std::string _logPath;
        std::string _logName;
    };
}


#endif //ULTRAVERSE_STATECLUSTERWRITER_HPP
