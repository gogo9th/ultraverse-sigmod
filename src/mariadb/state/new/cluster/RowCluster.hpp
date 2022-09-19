//
// Created by cheesekun on 9/14/22.
//

#ifndef ULTRAVERSE_ROWCLUSTER_HPP
#define ULTRAVERSE_ROWCLUSTER_HPP

#include <string>
#include <unordered_map>

#include "mariadb/state/StateItem.h"
#include "mariadb/state/new/Query.hpp"
#include "mariadb/state/new/StateChangeContext.hpp"

#include "utils/log.hpp"

namespace ultraverse::state::v2 {
    class RowCluster {
    public:
        static std::string resolveForeignKey(std::string exprName, const std::vector<ForeignKey> &foreignKeys);
        
        RowCluster();
        
        bool hasKey(const std::string &columnName) const;
        void addKey(const std::string &columnName) const;
        
        void addKeyRange(const std::string &columnName, StateRange &range);
        
        /**
         * @throws std::runtime_error when given key was not found
         */
        StateRange &getKeyRange(const std::string &columnName);
    
        const std::unordered_map<std::string, StateRange> &keyMap() const;
        
        bool isQueryRelated(const std::shared_ptr<Query> &query, const std::vector<ForeignKey> foreignKeys) const;
        
        RowCluster operator&(const RowCluster &other) const;
        RowCluster operator|(const RowCluster &other) const;
    private:
        LoggerPtr _logger;
        
        bool isExprRelated(const StateItem &expr, const std::vector<ForeignKey> &foreignKeys) const;
        
        /**
         * FIXME: 이거 std::string에서 std::pair<NamingHistory, std::string> 같은걸로 바꿔야 할듯
         *        안그러면 이거 테이블 리네임되면 맛감
         */
        std::unordered_map<std::string, StateRange> _keyMap;
    };
}

#endif //ULTRAVERSE_ROWCLUSTER_HPP
