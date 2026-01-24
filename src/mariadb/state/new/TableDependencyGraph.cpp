//
// Created by cheesekun on 11/30/22.
//

#include "utils/StringUtil.hpp"
#include "TableDependencyGraph.hpp"

#include "ultraverse_state.pb.h"


namespace ultraverse::state::v2 {
    TableDependencyGraph::TableDependencyGraph():
        _logger(createLogger("TableDependencyGraph"))
    {
    
    }
    
    bool TableDependencyGraph::addTable(const std::string &tableName) {
        if (_nodeMap.find(tableName) != _nodeMap.end()) {
            return false;
        }
        
        auto nodeIdx = add_vertex(tableName, _graph);
        _nodeMap.insert({ tableName, nodeIdx });
        
        return true;
    }
    
    bool TableDependencyGraph::addRelationship(const std::string &fromTable, const std::string &toTable) {
        addTable(fromTable);
        addTable(toTable);
        
        if (!isRelated(fromTable, toTable)) {
            _logger->info("adding relation: {} =[W]=> {}", fromTable, toTable);
            add_edge(_nodeMap.at(fromTable), _nodeMap.at(toTable), _graph);
            
            return true;
        }
        
        return false;
    }
    
    bool TableDependencyGraph::addRelationship(const ColumnSet &readSet, const ColumnSet &writeSet) {
        bool isGraphChanged = false;
        
        std::set<std::string> readTableSet;
        std::set<std::string> writeTableSet;
        
        for (const auto &fullColumn: readSet) {
            auto vec = utility::splitTableName(fullColumn);
            const auto &table = vec.first;
            
            readTableSet.insert(table);
        }
        
        for (const auto &fullColumn: writeSet) {
            auto vec = utility::splitTableName(fullColumn);
            const auto &table = vec.first;
            
            writeTableSet.insert(table);
        }
        
        if (writeTableSet.empty()) {
            return false;
        }

        if (readTableSet.empty()) {
            // Write-only queries still affect their target tables (e.g., INSERT values, DROP/TRUNCATE).
            // Model this as dependencies among written tables so they are tracked in the graph.
            readTableSet = writeTableSet;
        }
        
        for (const auto &fromTable: readTableSet) {
            for (const auto &toTable: writeTableSet) {
                isGraphChanged |= addRelationship(fromTable, toTable);
            }
        }
        
        return isGraphChanged;
    }
    
    std::vector<std::string> TableDependencyGraph::getDependencies(const std::string &tableName) {
        std::vector<std::string> dependencies;
        
        if (_nodeMap.find(tableName) == _nodeMap.end()) {
            return dependencies;
        }
        
        auto tableIndex = _nodeMap.at(tableName);
    
        boost::graph_traits<Graph>::out_edge_iterator vi, viEnd, next;
        boost::tie(vi, viEnd) = boost::out_edges(tableIndex, _graph);
    
    
        for (next = vi; vi != viEnd; vi = next) {
            next++;
        
            dependencies.push_back(_graph[vi->m_target]);
        }
    
        return dependencies;
    }
    
    bool TableDependencyGraph::hasPeerDependencies(const std::string &tableName) {
        if (_nodeMap.find(tableName) == _nodeMap.end()) {
            return false;
        }
        
        boost::graph_traits<Graph>::in_edge_iterator ii, iiEnd;
        boost::tie(ii, iiEnd) = boost::in_edges(_nodeMap.at(tableName), _graph);
        
        return ii != iiEnd;
    }
    
    
    bool TableDependencyGraph::addRelationship(const std::vector<ForeignKey> &foreignKeys) {
        bool isGraphChanged = false;
        
        for (const auto &foreignKey: foreignKeys) {
            isGraphChanged |= addRelationship(
                foreignKey.fromTable->getCurrentName(),
                foreignKey.toTable->getCurrentName()
            );
        }
        
        return isGraphChanged;
    }
    
    bool TableDependencyGraph::isRelated(const std::string &fromTable, const std::string &toTable) {
        auto fromIt = _nodeMap.find(fromTable);
        auto toIt = _nodeMap.find(toTable);
        if (fromIt == _nodeMap.end() || toIt == _nodeMap.end()) {
            return false;
        }

        boost::graph_traits<Graph>::in_edge_iterator ii, iiEnd, next;
        boost::tie(ii, iiEnd) = boost::in_edges(toIt->second, _graph);
        
        auto fromTableIndex = fromIt->second;
        
        for (next = ii; ii != iiEnd; ii = next) {
            next++;
            
            if (ii->m_source == fromTableIndex) {
                return true;
            }
        }
        
        return false;
    }

    void TableDependencyGraph::toProtobuf(ultraverse::state::v2::proto::TableDependencyGraph *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        for (const auto &pair : _nodeMap) {
            const auto &table = pair.first;
            const auto nodeIdx = pair.second;

            auto *entry = out->add_entries();
            entry->set_table(table);

            boost::graph_traits<Graph>::out_edge_iterator oi, oiEnd, next;
            boost::tie(oi, oiEnd) = boost::out_edges(nodeIdx, _graph);
            for (next = oi; oi != oiEnd; oi = next) {
                next++;
                entry->add_related_tables(_graph[oi->m_target]);
            }
        }
    }

    void TableDependencyGraph::fromProtobuf(const ultraverse::state::v2::proto::TableDependencyGraph &msg) {
        _graph.clear();
        _nodeMap.clear();

        for (const auto &entry : msg.entries()) {
            addTable(entry.table());
        }

        for (const auto &entry : msg.entries()) {
            for (const auto &related : entry.related_tables()) {
                addRelationship(entry.table(), related);
            }
        }
    }
}
