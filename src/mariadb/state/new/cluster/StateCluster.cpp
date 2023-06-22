//
// Created by cheesekun on 6/20/23.
//

#include "StateCluster.hpp"

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
    
    void StateCluster::operator<<(const std::shared_ptr<Transaction> &transaction) {
        std::function<bool(const StateItem &)> isKeyColumn = [this](const StateItem &item) {
            return std::find(_keyColumns.begin(), _keyColumns.end(), item.name) != _keyColumns.end();
        };
        
        {
            // read
            auto it = transaction->whereSet_begin();
            auto itEnd = transaction->whereSet_end();
            
            while (true) {
                it = std::find_if(it, itEnd, isKeyColumn);
                
                if (it == itEnd) {
                    break;
                }
                
                auto &item = *it;
                auto &columnName = item.name;
                
                {
                    std::scoped_lock lock(_clusterInsertionLock);
                    _clusters[columnName].read[item.data_list.front()].emplace_back(transaction->gid());
                }
                
                ++it;
            }
        }
        {
            // write
            auto it = transaction->itemSet_begin();
            auto itEnd = transaction->itemSet_end();
            
            while (true) {
                it = std::find_if(it, itEnd, isKeyColumn);
                
                if (it == itEnd) {
                    break;
                }
                
                auto &item = *it;
                auto &columnName = item.name;
                
                {
                    std::scoped_lock lock(_clusterInsertionLock);
                    _clusters[columnName].write[item.data_list.front()].emplace_back(transaction->gid());
                }
                
                ++it;
            }

        }
    }
}