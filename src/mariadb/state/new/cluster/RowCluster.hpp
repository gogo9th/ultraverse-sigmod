//
// Created by cheesekun on 9/14/22.
//

#ifndef ULTRAVERSE_ROWCLUSTER_HPP
#define ULTRAVERSE_ROWCLUSTER_HPP

#include <string>
#include <unordered_map>

#include "mariadb/state/StateItem.h"
#include "mariadb/state/new/Query.hpp"

namespace ultraverse::state::v2 {
    class RowCluster {
    public:
        RowCluster() = default;
        
        bool hasKey(const std::string &columnName) const;
        void addKey(const std::string &columnName) const;
        
        void addKeyRange(const std::string &columnName, StateRange &range);
        
        /**
         * @throws std::runtime_error when given key was not found
         */
        StateRange &getKeyRange(const std::string &columnName);
        
        bool operator&(const std::shared_ptr<Query> &query) const;
        
        RowCluster operator&(const RowCluster &other) const;
        RowCluster operator|(const RowCluster &other) const;
    private:
        bool isExprRelated(const StateItem &expr) const;
        
        std::unordered_map<std::string, StateRange> _keyMap;
    };
}

#endif //ULTRAVERSE_ROWCLUSTER_HPP
