//
// Created by cheesekun on 11/30/22.
//

#include <cereal/types/memory.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/vector.hpp>

#include "TableDependencyGraph.hpp"

namespace ultraverse::state::v2 {
    template<typename Archive>
    void TableDependencyGraph::save(Archive &archive) const {
        std::map<std::string, std::vector<std::string>>
            serializedGraph;
        
        for (const auto &pair: _nodeMap) {
            auto table = pair.first;
            auto nodeIdx = pair.second;
            
            std::vector<std::string> relatedTables;
    
            boost::graph_traits<Graph>::out_edge_iterator oi, oiEnd, next;
            boost::tie(oi, oiEnd) = boost::out_edges(nodeIdx, _graph);
            
            for (next = oi; oi != oiEnd; oi = next) {
                next++;
                relatedTables.push_back(_graph[oi->m_target]);
            }
            
            serializedGraph.insert({ table, relatedTables });
        }
        
        archive(serializedGraph);
    }
    
    template<typename Archive>
    void TableDependencyGraph::load(Archive &archive) {
        std::map<std::string, std::vector<std::string>>
            serializedGraph;
        
        archive(serializedGraph);
        
        for (const auto &pair: serializedGraph) {
            const auto &table = pair.first;
            
            addTable(table);
        }
        
        for (const auto &pair: serializedGraph) {
             const auto &fromTable = pair.first;
             for (const std::string &toTable: pair.second) {
                 addRelationship(fromTable, toTable);
             }
        }
    }
}