//
// Created by cheesekun on 9/14/22.
//

#ifndef ULTRAVERSE_ROWCLUSTER_HPP
#define ULTRAVERSE_ROWCLUSTER_HPP

#include <string>
#include <unordered_map>

#include "mariadb/state/StateItem.h"
#include "mariadb/state/new/Query.hpp"
#include "mariadb/state/new/Transaction.hpp"
#include "mariadb/state/new/StateChangeContext.hpp"

#include "utils/log.hpp"

namespace ultraverse::state::v2 {
    
    struct RowAlias {
        StateItem alias;
        StateItem real;
    
        template <typename Archive>
        void serialize(Archive &archive);
    };
    
    class RowCluster {
    public:
        static std::string resolveForeignKey(std::string exprName, const std::vector<ForeignKey> &foreignKeys);
        
        RowCluster();
        
        bool hasKey(const std::string &columnName) const;
        void addKey(const std::string &columnName) const;
        
        void addKeyRange(const std::string &columnName, StateRange &range);
    
        void addAlias(StateItem alias, StateItem real);
        static StateItem resolveAlias(const std::vector<RowAlias> &aliases, StateItem alias);
        
        static std::vector<std::unique_ptr<std::pair<std::string, StateRange>>>
        resolveInvertedAliasRange(const std::vector<RowAlias> &aliases, std::string alias, StateRange range);
        
        static std::string resolveAliasName(const std::vector<RowAlias> &aliases, std::string alias);
        
        const std::vector<RowAlias> &aliasSet();
        
        /**
         * @throws std::runtime_error when given key was not found
         */
        StateRange &getKeyRange(const std::string &columnName);
    
        std::unordered_map<std::string, std::vector<StateRange>> &keyMap();
    
        static bool isQueryRelated(std::map<std::string, std::vector<StateRange>> &keyRanges, Query &query, const std::vector<ForeignKey> &foreignKeys, const std::vector<RowAlias> &aliases);
        static bool isQueryRelated(std::string keyColumn, StateRange &keyRange, Query &query, const std::vector<ForeignKey> &foreignKeys, const std::vector<RowAlias> &aliases);
        
        std::vector<StateRange> getKeyRangeOf(Transaction &transaction, const std::string &keyColumn, const std::vector<ForeignKey> &foreignKeys);
        
        RowCluster operator&(const RowCluster &other) const;
        RowCluster operator|(const RowCluster &other) const;
    
        void mergeCluster(const std::string &columnName, bool force);
    
        template <typename Archive>
        void serialize(Archive &archive);
    private:
        LoggerPtr _logger;
        
        
        static bool isExprRelated(std::string keyColumn, StateRange &keyRange, StateItem expr, const std::vector<ForeignKey> &foreignKeys, const std::vector<RowAlias> &aliases);
        
        /**
         * FIXME: 이거 std::string에서 std::pair<NamingHistory, std::string> 같은걸로 바꿔야 할듯
         *        안그러면 이거 테이블 리네임되면 맛감
         */
        std::unordered_map<std::string, std::vector<StateRange>> _clusterMap;
        std::vector<RowAlias> _aliases;
    };
}

#include "RowCluster.cereal.cpp"

#endif //ULTRAVERSE_ROWCLUSTER_HPP
