//
// Created by cheesekun on 10/27/22.
//

#include "ColumnDependencyGraph.hpp"

#include "cluster/RowCluster.hpp"
#include "utils/StringUtil.hpp"


namespace ultraverse::state::v2 {
    ColumnDependencyGraph::ColumnDependencyGraph():
        _logger(createLogger("ColumnDependencyGraph"))
    {
    
    }
    
    std::string ColumnDependencyGraph::dumpColumnSet(const ColumnSet &columnSet) const {
        std::stringstream sstream;
        
        for (const auto &column: columnSet) {
            sstream << column << ",";
        }
        
        return sstream.str();
    }
    
    bool ColumnDependencyGraph::add(const ColumnSet &columnSet, ColumnAccessType accessType, const std::vector<ForeignKey> &foreignKeys) {
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
    
        _logger->trace("adding columnset: {}", dumpColumnSet(columnSet));
        _nodeMap.insert({ hash, nodeIdx });
        
        boost::graph_traits<Graph>::vertex_iterator vi, viEnd, next;
        boost::tie(vi, viEnd) = vertices(_graph);
        
        for (next = vi; vi != viEnd; vi = next) {
            ++next;
            
            const auto &node = _graph[*vi];
            if (node->accessType == READ) {
                // R-R, R-W는 무시
                continue;
            }
            
            for (const auto &column: node->columnSet) {
                auto vec1 = utility::splitTableName(
                    RowCluster::resolveForeignKey(column, foreignKeys)
                );
                auto &table1 = vec1.first;
                auto &column1 = vec1.second;
                
                auto it = std::find_if(columnSet.begin(), columnSet.end(), [&foreignKeys, &table1, &column1](const auto &targetColumn) {
                    auto vec2 = utility::splitTableName(
                        RowCluster::resolveForeignKey(targetColumn, foreignKeys)
                    );
                    auto &table2 = vec2.first;
                    auto &column2 = vec2.second;
                    
                    if (column1 == "*" || column2 == "*") {
                        auto it = std::find_if(foreignKeys.begin(), foreignKeys.end(), [&table1, &table2, &column1, column2](const ForeignKey &fk) {
                            return (
                                (fk.fromTable->getCurrentName() == table1 && fk.toTable->getCurrentName() == table2) ||
                                (fk.fromTable->getCurrentName() == table2 && fk.toTable->getCurrentName() == table1)
                            ) && (
                                (fk.fromColumn == column1) || (fk.fromColumn == column2) ||
                                (fk.toColumn == column1)   || (fk.toColumn == column2)
                            );
                        });
                        
                        if (it != foreignKeys.end()) {
                            return true;
                        }
                    }
                    
                    return (
                        (table1 == table2) &&
                        (column1 == column2 || column1 == "*" || column2 == "*")
                    );
                });
                
                if (it != columnSet.end()) {
                    _logger->trace("creating relationship: ({}) <=> ({})", dumpColumnSet(node->columnSet), dumpColumnSet(columnSet));
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