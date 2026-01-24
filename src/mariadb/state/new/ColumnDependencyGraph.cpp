//
// Created by cheesekun on 10/27/22.
//

#include "ColumnDependencyGraph.hpp"

#include <algorithm>
#include <cassert>

#include "cluster/RowCluster.hpp"
#include "utils/StringUtil.hpp"

#include "ultraverse_state.pb.h"


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

    void ColumnDependencyNode::toProtobuf(ultraverse::state::v2::proto::ColumnDependencyNode *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        out->set_access_type(static_cast<uint32_t>(accessType));
        out->set_hash(static_cast<uint64_t>(hash));
        for (const auto &column : columnSet) {
            out->add_column_set(column);
        }
    }

    void ColumnDependencyNode::fromProtobuf(const ultraverse::state::v2::proto::ColumnDependencyNode &msg) {
        columnSet.clear();
        for (const auto &column : msg.column_set()) {
            columnSet.insert(column);
        }
        accessType = static_cast<ColumnAccessType>(msg.access_type());
        hash = static_cast<size_t>(msg.hash());
    }

    void ColumnDependencyGraph::toProtobuf(ultraverse::state::v2::proto::ColumnDependencyGraph *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        for (const auto &pair : _nodeMap) {
            const auto nodeIdx = pair.second;
            const auto &node = _graph[nodeIdx];
            auto *entry = out->add_entries();
            entry->set_node_index(static_cast<int64_t>(nodeIdx));
            node->toProtobuf(entry->mutable_node());

            boost::graph_traits<Graph>::adjacency_iterator ai, aiEnd, next;
            boost::tie(ai, aiEnd) = boost::adjacent_vertices(nodeIdx, _graph);
            for (next = ai; ai != aiEnd; ai = next) {
                next++;
                entry->add_adjacent(static_cast<int64_t>(*ai));
            }
        }
    }

    void ColumnDependencyGraph::fromProtobuf(const ultraverse::state::v2::proto::ColumnDependencyGraph &msg) {
        _graph.clear();
        _nodeMap.clear();

        std::vector<const ultraverse::state::v2::proto::ColumnDependencyGraphEntry *> entries;
        entries.reserve(static_cast<size_t>(msg.entries_size()));
        for (const auto &entry : msg.entries()) {
            entries.push_back(&entry);
        }

        std::sort(entries.begin(), entries.end(),
                  [](const auto *a, const auto *b) { return a->node_index() < b->node_index(); });

        for (const auto *entry : entries) {
            auto node = std::make_shared<ColumnDependencyNode>();
            node->fromProtobuf(entry->node());
            auto newIdx = add_vertex(node, _graph);
            assert(entry->node_index() == static_cast<int64_t>(newIdx));
            _nodeMap.insert({ node->hash, static_cast<int>(newIdx) });
        }

        for (const auto *entry : entries) {
            const auto nodeIdx = static_cast<int>(entry->node_index());
            for (const auto adj : entry->adjacent()) {
                add_edge(nodeIdx, static_cast<int>(adj), _graph);
            }
        }
    }
}
