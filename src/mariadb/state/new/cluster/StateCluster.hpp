//
// Created by cheesekun on 6/20/23.
//

#ifndef ULTRAVERSE_STATECLUSTER_HPP
#define ULTRAVERSE_STATECLUSTER_HPP

#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <mutex>

#include "../../StateItem.h"
#include "../Transaction.hpp"
#include "../CombinedIterator.hpp"

#include "./StateRelationshipResolver.hpp"

#include "utils/log.hpp"

/**
 * @brief Row-level clustering을 위한 클래스
 *
 * TODO: StateRowCluster로 이름 바꿔야 하지 않을까??
 *
 * +-----------------------+
 * | users.id              |
 * +-----------------------+
 * | +-------------------+ |
 * | | user.id=1         | |
 * | +-------------------+ |
 * | | read = {1, 3, 5}  | |
 * | | write = {2, 4, 6} | |
 * | +-------------------+ |
 * +-----------------------+
 *
 */
namespace ultraverse::state::v2 {
    
    class StateCluster {
    public:
        enum ClusterType {
            READ,
            WRITE
        };
        
        class Cluster {
        public:
            using ClusterMap = std::unordered_map<StateRange, std::unordered_set<gid_t>>;
            ClusterMap read;
            ClusterMap write;
            
            template <typename Archive>
            void serialize(Archive &archive);
            
            static std::optional<StateRange> match(ClusterType type,
                                                   const std::string &columnName,
                                                   const ClusterMap &cluster,
                                                   const std::vector<StateItem> &items,
                                                   const RelationshipResolver &resolver);
        };
    public:
        StateCluster(const std::set<std::string> &keyColumns);
        
        const std::set<std::string> &keyColumns() const;
        const std::map<std::string, Cluster> &clusters() const;
        
        bool isKeyColumnItem(const RelationshipResolver &resolver, const StateItem& item) const;
        
        void insert(ClusterType type, const std::string &columnName, const StateRange &range, gid_t gid);
        void insert(ClusterType type, CombinedIterator<StateItem> begin, CombinedIterator<StateItem> end, gid_t gid, const RelationshipResolver &resolver);
        void insert(const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver);
        
        std::optional<StateRange> match(ClusterType type, const std::string &columnName, const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver) const;
        
        void describe();
        
        void addRollbackTarget(const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver);
        void addPrependTarget(gid_t gid, const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver);
        
        bool shouldReplay(gid_t gid);
        
        template <typename Archive>
        void serialize(Archive &archive);
        
    private:
        class TargetTransactionCache {
        public:
            std::shared_ptr<Transaction> transaction;
            
            std::unordered_map<std::string, StateRange> read;
            std::unordered_map<std::string, StateRange> write;
        };
        
    private:
        std::pair<std::vector<StateItem>, std::vector<StateItem>> merge(
            ClusterType type,
            CombinedIterator<StateItem> begin, CombinedIterator<StateItem> end, const RelationshipResolver &resolver
        ) const;
        
        void invalidateTargetCache(std::unordered_map<gid_t, TargetTransactionCache> &targets, const RelationshipResolver &resolver);
        
        bool shouldReplay(gid_t gid, const TargetTransactionCache &cache);
        
        LoggerPtr _logger;
        
        std::mutex _clusterInsertionLock;
        
        std::set<std::string> _keyColumns;
        std::map<std::string, Cluster> _clusters;
        
        std::mutex _targetCacheLock;
        std::unordered_map<gid_t, TargetTransactionCache> _rollbackTargets;
        std::unordered_map<gid_t, TargetTransactionCache> _prependTargets;
    };
}

#include "StateCluster.cereal.cpp"

#endif //ULTRAVERSE_STATECLUSTER_HPP
