//
// Created by cheesekun on 6/20/23.
//

#include "StateCluster.hpp"

#include <utility>

namespace ultraverse::state::v2 {
    
    
    StateCluster::StateCluster(const std::set<std::string> &keyColumns):
        _keyColumns(keyColumns)
    {
    
    }
    
    const std::set<std::string> &StateCluster::keyColumns() const {
        return _keyColumns;
    }
    
    const std::map<std::string, StateCluster::Cluster> &StateCluster::clusters() const {
        return _clusters;
    }
    
    bool StateCluster::isKeyColumnItem(const RelationshipResolver &resolver, const StateItem &item) const {
        return std::find_if(
            _keyColumns.begin(), _keyColumns.end(),
            [&resolver, &item](const auto &keyColumn) {
                // 과연 이게 맞는가? alias + foreign key를 고려해야 하지 않을까?
                auto realColumn = resolver.resolveColumnAlias(item.name);
                auto foreignKey = resolver.resolveForeignKey(item.name);
                
                bool x = item.name == keyColumn;
                bool y = realColumn.has_value() && realColumn.value() == keyColumn;
                bool z = foreignKey.has_value() && foreignKey.value() == keyColumn;
                
                return x || y || z;
            }
        ) != _keyColumns.end();
    }
    
    void StateCluster::insert(StateCluster::InsertionType type, const std::string &columnName, const StateRange &range, gid_t gid) {
        std::scoped_lock lock(_clusterInsertionLock);
        if (type == READ) {
            _clusters[columnName].read[range].emplace_back(gid);
        } else if (type == WRITE) {
            _clusters[columnName].write[range].emplace_back(gid);
        }
    }
    
    void StateCluster::insert(StateCluster::InsertionType type, CombinedIterator<StateItem> begin, CombinedIterator<StateItem> end, gid_t gid, const RelationshipResolver &resolver) {
        const auto isKeyColumnItem = [&resolver, this](const StateItem &item) {
            return this->isKeyColumnItem(resolver, item);
        };
        
        auto it = std::move(begin);
        
        while (true) {
            it = std::find_if(it, end, isKeyColumnItem);
            
            if (it == end) {
                break;
            }
            
            auto &item = *it;
            auto &columnName = item.name;
            
            // 과연 이게 맞는가? alias + foreign key를 고려해야 하지 않을까?
            auto realColumn = resolver.resolveColumnAlias(columnName);
            auto foreignKey = resolver.resolveForeignKey(columnName);
            
            if (realColumn.has_value()) {
                auto real = resolver.resolveRowAlias(item);
                if (!real.has_value()) {
                    auto errorMessage = fmt::format(
                        "column {} is alias of {}, but could not resolve row data of {}",
                        columnName, realColumn.value(), item.MakeRange2().MakeWhereQuery(columnName)
                    );
                    
                    _logger->error(errorMessage);
                    throw std::runtime_error(errorMessage);
                }
                
                insert(type, realColumn.value(), real->MakeRange2(), gid);
            } else if (foreignKey.has_value()) {
                insert(type, foreignKey.value(), item.MakeRange2(), gid);
            } else {
                insert(type, columnName, item.MakeRange2(), gid);
            }
            
            ++it;
        }
    }
    
    void StateCluster::insert(const std::shared_ptr<Transaction>& transaction, const RelationshipResolver &resolver) {
        insert(READ, transaction->whereSet_begin(), transaction->whereSet_end(), transaction->gid(), resolver);
        insert(WRITE, transaction->itemSet_begin(), transaction->itemSet_end(), transaction->gid(), resolver);
    }
}