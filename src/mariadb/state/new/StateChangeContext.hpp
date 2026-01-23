//
// Created by cheesekun on 9/7/22.
//

#ifndef ULTRAVERSE_STATECHANGECONTEXT_HPP
#define ULTRAVERSE_STATECHANGECONTEXT_HPP

#include <cstdint>

#include <string>
#include <list>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

#include <mutex>

#include "cluster/NamingHistory.hpp"

namespace ultraverse::state::v2 {
    struct ForeignKey {
        std::shared_ptr<NamingHistory> fromTable;
        std::string fromColumn;
        
        std::shared_ptr<NamingHistory> toTable;
        std::string toColumn;
    };
    
    class StateChangeContext {
    public:
        std::shared_ptr<NamingHistory> findTable(std::string tableName, uint64_t when) {
            auto it = std::find_if(tables.begin(), tables.end(), [tableName, when](auto &history) {
                return history->match(tableName, when);
            });
            
            if (it != tables.end()) {
                return *it;
            } else {
                // FIXME: 이거 parseDDL에서 해야 함
                auto table = std::make_shared<NamingHistory>(tableName);
                tables.push_back(table);
                
                return table;
            }
        }
        
        std::vector<std::shared_ptr<NamingHistory>> tables;
        std::unordered_set<std::string> primaryKeys;
        std::vector<ForeignKey> foreignKeys;
        
        std::unordered_map<std::string, int64_t> autoIncrements;
        
        // fixme;
        std::mutex contextLock;
    };
}

#endif //ULTRAVERSE_STATECHANGECONTEXT_HPP
