//
// Created by cheesekun on 9/17/22.
//

#include <algorithm>

#include "NamingHistory.hpp"

namespace ultraverse::state::v2 {
    NamingHistory::NamingHistory(const std::string &initialName):
        _initialName(initialName)
    {
        _namingHistory.emplace_back(0, initialName);
    }
    
    void NamingHistory::addRenameHistory(const std::string &newName, uint64_t when) {
        _namingHistory.emplace_back(when, newName);
    
        std::sort(_namingHistory.begin(), _namingHistory.end(), [](const auto &a, const auto &b) {
            return a.first < b.first;
        });
    }
    
    std::string NamingHistory::getName(uint64_t when) const {
        std::string name = _initialName;
        for (auto &it: _namingHistory) {
            if (it.first > when) {
                break;
            }
            
            name = it.second;
        }
        
        return name;
    }
    
    std::string NamingHistory::getInitialName() const {
        return _initialName;
    }
    
std::string NamingHistory::getCurrentName() const {
        return _namingHistory.back().second;
}
    
    bool NamingHistory::match(const std::string &name, uint64_t when) const {
        return getName(when) == name;
    }
}
