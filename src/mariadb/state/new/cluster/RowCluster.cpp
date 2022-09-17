#include <fmt/format.h>

#include "RowCluster.hpp"

namespace ultraverse::state::v2 {
    
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
    
    bool RowCluster::operator&(const std::shared_ptr<Query> &query) const {
        for (auto expr: query->whereSet()) {
            if (isExprRelated(expr)) {
                return true;
            }
        }
        
        for (auto expr: query->itemSet()) {
            if (isExprRelated(expr)) {
                return true;
            }
        }
        
        return false;
    }
    
    bool RowCluster::isExprRelated(const StateItem &expr) const {
        if (_keyMap.find(expr.name) != _keyMap.end()) {
            auto range = StateItem::MakeRange(expr);
            auto &keyRange = _keyMap.at(expr.name);
            if (!StateRange::AND(range, keyRange).GetRange()->empty()) {
                return true;
            }
        }
        
        for (auto &subExpr: expr.arg_list) {
            if (isExprRelated(subExpr)) {
                return true;
            }
        }
        
        return false;
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