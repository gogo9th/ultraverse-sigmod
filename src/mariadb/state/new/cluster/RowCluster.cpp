#include <fmt/format.h>

#include "RowCluster.hpp"
#include "mariadb/state/StateUserQuery.h"

namespace ultraverse::state::v2 {
    RowCluster::RowCluster():
        _logger(createLogger("RowCluster"))
    {
    
    }
    
    bool RowCluster::hasKey(const std::string &columnName) const {
        return _keyMap.find(columnName) != _keyMap.end();
    }
    
    void RowCluster::addKeyRange(const std::string &columnName, StateRange &range) {
        _keyMap[columnName] = StateRange::OR(_keyMap[columnName], range);
    }
    
    StateRange &RowCluster::getKeyRange(const std::string &columnName) {
        if (!hasKey(columnName)) {
            throw std::runtime_error(fmt::format(
                "{} is not in keyMap",
                columnName
            ));
        }
        
        return _keyMap.at(columnName);
    }
    
    const std::unordered_map<std::string, StateRange> &RowCluster::keyMap() const {
        return _keyMap;
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
            
            if (_keyMap.find(name) != _keyMap.end()) {
                auto range = StateItem::MakeRange(expr);
                auto &keyRange = _keyMap.at(name);
                if (!StateRange::AND(range, keyRange).GetRange()->empty()) {
                    return true;
                }
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
        for (auto &it: this->_keyMap) {
            keys.insert(it.first);
        }
        
        for (auto &it: other._keyMap) {
            keys.insert(it.first);
        }
        
        for (auto &key: keys) {
            if (!other.hasKey(key) || !this->hasKey(key)) {
                continue;
            }
            dst._keyMap[key] = StateRange::AND(this->_keyMap.at(key), other._keyMap.at(key));
        }
    }
    
    RowCluster RowCluster::operator|(const RowCluster &other) const {
        RowCluster dst = *this;
        
        for (auto &it: this->_keyMap) {
            if (!other.hasKey(it.first)) {
                dst._keyMap[it.first] = it.second;
            } else {
                dst._keyMap[it.first] = StateRange::OR(this->_keyMap.at(it.first), other._keyMap.at(it.first));
            }
        }
        
        for (auto &it: other._keyMap) {
            if (dst.hasKey(it.first)) {
                continue;
            } else if (!this->hasKey(it.first)) {
                dst._keyMap[it.first] = it.second;
            }
        }
    }
}