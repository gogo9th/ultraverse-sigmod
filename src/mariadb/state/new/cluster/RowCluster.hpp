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

#include "StateRelationshipResolver.hpp"

#include "utils/log.hpp"

namespace ultraverse::state::v2 {
    class RowCluster {
    public:
        using AliasMap = std::unordered_map<std::string,
            std::unordered_map<StateData, RowAlias>
        >;
        
        /**
         * @deprecated
         */
        static std::string resolveForeignKey(std::string exprName, const std::vector<ForeignKey> &foreignKeys);
        
        RowCluster();
        
        bool hasKey(const std::string &columnName) const;
        void addKey(const std::string &columnName);
        
        void addKeyRange(const std::string &columnName, std::shared_ptr<StateRange> range, gid_t gid);
        void setWildcard(const std::string &columnName, bool wildcard);
    
        void addAlias(const std::string &elementName, const StateItem &alias, const StateItem &real);
        static const StateItem& resolveAlias(const StateItem &alias, const AliasMap &aliasMap);
        
        static std::vector<std::unique_ptr<std::pair<std::string, std::shared_ptr<StateRange>>>>
        resolveInvertedAliasRange(const std::vector<RowAlias> &aliases, std::string alias, std::shared_ptr<StateRange> range);
        
        const AliasMap &aliasMap() const;
        
        static std::string resolveAliasName(const AliasMap &aliases, std::string alias);
        
        std::unordered_map<std::string, std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>> &keyMap();
    
        static bool isQueryRelated(std::map<std::string, std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>> &keyRanges, Query &query, const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases);
        static bool isQueryRelated(std::string keyColumn, std::shared_ptr<StateRange> keyRange, Query &query, const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases);
        
        bool isTransactionRelated(Transaction &transaction, const std::map<std::string, std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>> &keyRanges);
        static bool isTransactionRelated(gid_t gid, const std::vector<gid_t> &gidList);
        
        std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>> getKeyRangeOf(Transaction &transaction, const std::string &keyColumn, const std::vector<ForeignKey> &foreignKeys);
        std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>> getKeyRangeOf2(Transaction &transaction, const std::string &keyColumn, const std::vector<ForeignKey> &foreignKeys);
        
        RowCluster operator&(const RowCluster &other) const;
        RowCluster operator|(const RowCluster &other) const;
    
        void mergeCluster(const std::string &columnName);
    
        template <typename Archive>
        void serialize(Archive &archive);
    private:
        LoggerPtr _logger;
        using ClusterGraph =
            boost::adjacency_list<boost::vecS, boost::vecS, boost::undirectedS, std::pair<int, bool>>;
        
        
        static bool isExprRelated(std::string keyColumn, StateRange &keyRange, StateItem &expr, const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases);
        
        void mergeClusterUsingGraph(const std::string &columnName);
        void mergeClusterAll(const std::string &columnName);
        
        
        /**
         * FIXME: 이거 std::string에서 std::pair<NamingHistory, std::string> 같은걸로 바꿔야 할듯
         *        안그러면 이거 테이블 리네임되면 맛감
         */
        std::unordered_map<std::string, std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>> _clusterMap;
        std::unordered_map<std::string, ClusterGraph> _clusterGraph;
        std::unordered_map<std::string, bool> _wildcardMap;
        
        AliasMap _aliases;
    };
}

#include "RowCluster.cereal.cpp"

#endif //ULTRAVERSE_ROWCLUSTER_HPP
