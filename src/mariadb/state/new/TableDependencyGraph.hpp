//
// Created by cheesekun on 11/30/22.
//

#ifndef ULTRAVERSE_TABLEDEPENDENCYGRAPH_HPP
#define ULTRAVERSE_TABLEDEPENDENCYGRAPH_HPP

#include <boost/graph/adjacency_list.hpp>

#include "mariadb/state/new/proto/ultraverse_state_fwd.hpp"

#include "Query.hpp"
#include "StateChangeContext.hpp"
#include "utils/log.hpp"

namespace ultraverse::state::v2 {
    
    class TableDependencyGraph {
    public:
        using Graph =
            boost::adjacency_list<boost::setS, boost::vecS, boost::bidirectionalS, std::string>;
        
        TableDependencyGraph();
        
        bool addTable(const std::string &tableName);
        bool addRelationship(const std::string &fromTable, const std::string &toTable);
        bool addRelationship(const ColumnSet &readSet, const ColumnSet &writeSet);
        bool addRelationship(const std::vector<ForeignKey> &foreignKeys);
    
        std::vector<std::string> getDependencies(const std::string &tableName);
        bool hasPeerDependencies(const std::string &tableName);
        
        [[nodiscard]]
        bool isRelated(const std::string &fromTable, const std::string &toTable);
        
        template <typename Archive>
        void save(Archive &archive) const;
        
        template <typename Archive>
        void load(Archive &archive);

        void toProtobuf(ultraverse::state::v2::proto::TableDependencyGraph *out) const;
        void fromProtobuf(const ultraverse::state::v2::proto::TableDependencyGraph &msg);
        
    private:
        LoggerPtr _logger;
        
        Graph _graph;
        std::map<std::string, int> _nodeMap;
    };
    
}

#endif //ULTRAVERSE_TABLEDEPENDENCYGRAPH_HPP
