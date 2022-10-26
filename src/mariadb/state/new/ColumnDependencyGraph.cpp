//
// Created by cheesekun on 10/27/22.
//

#include "ColumnDependencyGraph.hpp"

#include "../StateUserQuery.h"


namespace ultraverse::state::v2 {
    ColumnDependencyGraph::ColumnDependencyGraph():
        _logger(createLogger("ColumnDependencyGraph"))
    {
    
    }
    
    bool ColumnDependencyGraph::add(const ColumnSet &columnSet, ColumnAccessType accessType) {
        auto hash = std::hash<ColumnSet>{}(columnSet);
        if (_nodeMap.find(hash) != _nodeMap.end()) {
            return false;
        }
        
        auto nodeIdx = add_vertex(
            std::make_shared<ColumnDependencyNode>(ColumnDependencyNode {
                columnSet, accessType, hash
            }),
            _graph
        );
        
        _nodeMap.insert({ hash, nodeIdx });
        
        boost::graph_traits<Graph>::vertex_iterator vi, viEnd, next;
        boost::tie(vi, viEnd) = vertices(_graph);
        
        for (next = vi; vi != viEnd; vi = next) {
            ++next;
            
            const auto &node = _graph[*vi];
            if (node->hash == hash || node->accessType == READ) {
                // R-R, R-W는 무시
                continue;
            }
            
            for (const auto &column: node->columnSet) {
                auto vec1 = StateUserQuery::SplitDBNameAndTableName(column);
                
                auto it = std::find_if(columnSet.begin(), columnSet.end(), [&vec1](const auto &targetColumn) {
                    auto vec2 = StateUserQuery::SplitDBNameAndTableName(targetColumn);
                    
                    return (
                        (vec1[0] == vec2[0]) &&
                        (vec1[1] == vec2[1] || vec1[1] == "*" || vec2[1] == "*")
                    );
                });
                
                if (it != columnSet.end()) {
                    _logger->trace("creating relationship: (%s) <=> (%s)");
                    add_edge(*vi, nodeIdx, _graph);
                    continue;
                }
            }
        }
        
        return true;
    }
    
    void ColumnDependencyGraph::clear() {
        // not implemented yet
    }
    
    bool ColumnDependencyGraph::isRelated(const ColumnSet &a, const ColumnSet &b) const {
        return isRelated(std::hash<ColumnSet>{}(a), std::hash<ColumnSet>{}(b));
    }
    
    bool ColumnDependencyGraph::isRelated(size_t hashA, size_t hashB) const {
        if (_nodeMap.find(hashA) == _nodeMap.end() || _nodeMap.find(hashB) == _nodeMap.end()) {
            return false;
        }
        
        auto indexA = _nodeMap.at(hashA);
        auto indexB = _nodeMap.at(hashB);
    
        boost::graph_traits<Graph>::adjacency_iterator ai, aiEnd, next;
        boost::tie(ai, aiEnd) = boost::adjacent_vertices(indexA, _graph);
        
        for (next = ai; ai != aiEnd; ai = next) {
            next++;
            
            if (*ai == indexB) {
                return true;
            }
        }
        
        return false;
    }
}