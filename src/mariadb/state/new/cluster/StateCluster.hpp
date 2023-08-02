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

namespace ultraverse::state::v2 {

    /**
     * @brief Row-level clustering을 위한 클래스
     *
     * TODO: StateRowCluster로 이름 바꿔야 하지 않을까??
     * <pre>
     * +-----------------------+-----------------------+
     * | users.id              | posts.id              |
     * +-----------------------+-----------------------+
     * | +-------------------+ | +-------------------+ |
     * | | user.id=1         | | | post.id=1         | |
     * | +-------------------+ | +-------------------+ |
     * | | read = {1, 3, 5}  | | | read = {1, 3, 5}  | |
     * | | write = {2, 4, 6} | | | write = {2, 4, 6} | |
     * | +-------------------+ | +-------------------+ |
     * | +-------------------+ |                       |
     * | | user.id=1         | |                       |
     * | +-------------------+ |                       |
     * | | read = {1, 3, 5}  | |                       |
     * | | write = {2, 4, 6} | |                       |
     * | +-------------------+ |                       |
     * |                       |                       |
     * +-----------------------+ +-----------------------+
     * </pre>
     *
     */
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
        const std::unordered_map<std::string, Cluster> &clusters() const;
        
        /**
         * @brief 주어진 컬럼이 키 컬럼인지 확인한다
         */
        bool isKeyColumnItem(const RelationshipResolver &resolver, const StateItem& item) const;
        
        void insert(ClusterType type, const std::string &columnName, const StateRange &range, gid_t gid);
        void insert(ClusterType type, const std::vector<StateItem> &items, gid_t gid);
        
        /**
         * @brief 주어진 트랜잭션을 클러스터에 추가한다.
         */
        void insert(const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver);
        
        std::optional<StateRange> match(ClusterType type, const std::string &columnName, const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver) const;
        
        void describe();
        
        /**
         * @brief rollback 대상 트랜잭션을 추가한다.
         */
        void addRollbackTarget(const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver, bool revalidate = true);
        /**
         * @brief prepend 대상 트랜잭션을 추가한다.
         */
        void addPrependTarget(gid_t gid, const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver);
        
        /**
         * @brief 주어진 gid를 가진 트랜잭션이 재실행 대상인지 확인한다.
         */
        bool shouldReplay(gid_t gid);
        
        template <typename Archive>
        void serialize(Archive &archive);
        
    private:
        /**
         * @brief rollback / append 대상 트랜잭션 관련 데이터를 캐싱하기 위한 클래스
         */
        class TargetTransactionCache {
        public:
            std::shared_ptr<Transaction> transaction;
            
            /**
             * @brief rollback / append 대상 트랜잭션이 읽어들이는 컬럼(과 그 범위)
             */
            std::unordered_map<std::string, StateRange> read;
            /**
             * @brief rollback / append 대상 트랜잭션이 써내는 컬럼(과 그 범위)
             */
            std::unordered_map<std::string, StateRange> write;
        };
        
    private:
        static std::map<std::string, std::set<std::string>> buildKeyColumnsMap(const std::set<std::string> &keyColumns);
        
        /**
         * @brief 주어진 transaction의 readSet, writeSet으로부터 key column과 관련된 StateItem을 추출한다.
         * @return pair<R, W>
         */
        std::pair<std::vector<StateItem>, std::vector<StateItem>> extractItems(
            Transaction &transaction,
            const RelationshipResolver &resolver
        ) const;
        
        /**
         * rollback / append 대상 트랜잭션의 캐시를 갱신한다.
         */
        void invalidateTargetCache(std::unordered_map<gid_t, TargetTransactionCache> &targets, const RelationshipResolver &resolver);
        
        /**
         * @brief 주어진 gid를 가진 트랜잭션이 재실행 대상인지 확인한다 (internal)
         */
        bool shouldReplay(gid_t gid, const TargetTransactionCache &cache);
        
        LoggerPtr _logger;
        
        std::mutex _clusterInsertionLock;
        
        std::set<std::string> _keyColumns;
        std::map<std::string, std::set<std::string>> _keyColumnsMap;
        std::unordered_map<std::string, Cluster> _clusters;
        
        std::mutex _targetCacheLock;
        std::unordered_map<std::string, std::unordered_map<StateRange, std::reference_wrapper<const std::unordered_set<gid_t>>>> _targetCache;
        std::unordered_map<gid_t, TargetTransactionCache> _rollbackTargets;
        std::unordered_map<gid_t, TargetTransactionCache> _prependTargets;
    };
}

#include "StateCluster.cereal.cpp"

#endif //ULTRAVERSE_STATECLUSTER_HPP
