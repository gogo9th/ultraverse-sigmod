#include <fmt/format.h>

#include "RowCluster.hpp"
#include "mariadb/state/StateUserQuery.h"

namespace ultraverse::state::v2 {
    RowCluster::RowCluster():
        _logger(createLogger("RowCluster"))
    {
    
    }
    
    bool RowCluster::hasKey(const std::string &columnName) const {
        return _clusterMap.find(columnName) != _clusterMap.end();
    }
    
    void RowCluster::addKeyRange(const std::string &columnName, StateRange &range) {
        _clusterMap[columnName].push_back(range);
        mergeCluster(columnName);
    }
    
    void RowCluster::mergeCluster(const std::string &columnName) {
        auto &cluster = _clusterMap[columnName];
        
        MERGE_LOOP:
        for (auto it = cluster.begin(); it != cluster.end(); it++) {
            for (auto it2 = cluster.begin(); it2 != cluster.end(); it2++) {
                if (it == it2) {
                    continue;
                }
                
                auto result = StateRange::AND(*it, *it2);
                if (!result.GetRange()->empty()) {
                    _logger->trace("merging cluster: {} + {}", it->MakeWhereQuery(columnName), it2->MakeWhereQuery(columnName));
                    *it = StateRange::OR(*it, *it2);
                    cluster.erase(it2);
                    _logger->trace("cluster merged: {}", it->MakeWhereQuery(columnName));
                    goto MERGE_LOOP;
                }
            }
        }
    }
    
    StateRange &RowCluster::getKeyRange(const std::string &columnName) {
        // if (!hasKey(columnName)) {
            throw std::runtime_error(fmt::format(
                "{} is not in keyMap",
                columnName
            ));
        // }
        
        // return _clusterMap.at(columnName);
    }
    
    std::unordered_map<std::string, std::vector<StateRange>> &RowCluster::keyMap() {
        return _clusterMap;
    }
    
    bool RowCluster::isQueryRelated(const std::shared_ptr<Query> &query, const std::vector<ForeignKey> foreignKeys) const {
        for (auto expr: query->whereSet()) {
            if (isExprRelated(expr, foreignKeys)) {
                return true;
            }
        }
        
        for (auto expr: query->itemSet()) {
            if (isExprRelated(expr, foreignKeys)) {
                return true;
            }
        }
        
        return false;
    }
    
    bool RowCluster::isExprRelated(const StateItem &expr, const std::vector<ForeignKey> &foreignKeys) const {
        if (!expr.name.empty()) {
            auto name = resolveForeignKey(expr.name, foreignKeys);
            
            if (_clusterMap.find(name) != _clusterMap.end()) {
                auto range = StateItem::MakeRange(expr);
                auto &keyRange = _clusterMap.at(name);
                // if (!StateRange::AND(range, keyRange).GetRange()->empty()) {
                    return true;
                // }
            }
        }
        
        for (auto &subExpr: expr.arg_list) {
            if (isExprRelated(subExpr, foreignKeys)) {
                return true;
            }
        }
        
        return false;
    }
    
    std::string RowCluster::resolveForeignKey(std::string exprName, const std::vector<ForeignKey> &foreignKeys) {
        auto vec = StateUserQuery::SplitDBNameAndTableName(exprName);
        auto tableName = vec[0];
        auto columnName = vec[1];
        
        auto it = std::find_if(foreignKeys.cbegin(), foreignKeys.cend(), [&tableName, &columnName](auto &foreignKey) {
            if (foreignKey.fromTable->getCurrentName() == tableName && columnName == foreignKey.fromColumn) {
                return true;
            }
            return false;
        });
        
        if (it == foreignKeys.end()) {
            return exprName;
        } else {
            return it->toTable->getCurrentName() + "." + it->toColumn;
        }
    }
    
    RowCluster RowCluster::operator&(const RowCluster &other) const {
        RowCluster dst = *this;
        
        std::unordered_set<std::string> keys;
        for (auto &it: this->_clusterMap) {
            keys.insert(it.first);
        }
        
        for (auto &it: other._clusterMap) {
            keys.insert(it.first);
        }
        
        for (auto &key: keys) {
            if (!other.hasKey(key) || !this->hasKey(key)) {
                continue;
            }
            // dst._clusterMap[key] = StateRange::AND(this->_clusterMap.at(key), other._clusterMap.at(key));
        }
    }
    
    RowCluster RowCluster::operator|(const RowCluster &other) const {
        RowCluster dst = *this;
        
        for (auto &it: this->_clusterMap) {
            if (!other.hasKey(it.first)) {
                dst._clusterMap[it.first] = it.second;
            } else {
                // dst._clusterMap[it.first] = StateRange::OR(this->_clusterMap.at(it.first), other._clusterMap.at(it.first));
            }
        }
        
        for (auto &it: other._clusterMap) {
            if (dst.hasKey(it.first)) {
                continue;
            } else if (!this->hasKey(it.first)) {
                dst._clusterMap[it.first] = it.second;
            }
        }
    }
}