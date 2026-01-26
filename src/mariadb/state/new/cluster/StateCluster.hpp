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
#include <shared_mutex>

#include "mariadb/state/new/proto/ultraverse_state_fwd.hpp"

#include "../../StateItem.h"
#include "../Transaction.hpp"
#include "../CombinedIterator.hpp"

#include "./StateRelationshipResolver.hpp"

#include "utils/log.hpp"


namespace ultraverse::state::v2 {
    struct ForeignKey;

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
            using PendingClusterMap = std::vector<std::pair<StateRange, std::unordered_set<gid_t>>>;
            
            // for protobuf
            Cluster();
            Cluster(const Cluster &other);
            Cluster(Cluster &&other) noexcept = delete;
            
            ClusterMap read;
            ClusterMap write;
            
            PendingClusterMap pendingRead;
            PendingClusterMap pendingWrite;
            
            std::mutex readLock;
            std::mutex writeLock;
            
            template <typename Archive>
            void serialize(Archive &archive);

            void toProtobuf(ultraverse::state::v2::proto::StateClusterCluster *out) const;
            void fromProtobuf(const ultraverse::state::v2::proto::StateClusterCluster &msg);
            
            decltype(read.begin()) findByRange(ClusterType type, const StateRange &range);
            decltype(pendingRead.begin()) pending_findByRange(ClusterType type, const StateRange &range);
            
            void merge(ClusterType type);
            void finalize();
            
            static std::optional<StateRange> match(ClusterType type,
                                                   const std::string &columnName,
                                                   const ClusterMap &cluster,
                                                   const std::vector<StateItem> &items,
                                                   const RelationshipResolver &resolver);
        };

        struct GroupProjection {
            size_t groupIndex = 0;
            std::vector<std::string> columns;
        };

    public:
        StateCluster(const std::set<std::string> &keyColumns,
                     const std::vector<std::vector<std::string>> &keyColumnGroups = {});
        
        const std::set<std::string> &keyColumns() const;
        const std::unordered_map<std::string, Cluster> &clusters() const;
        
        /**
         * @brief 주어진 컬럼이 키 컬럼인지 확인한다
         */
        bool isKeyColumnItem(const RelationshipResolver &resolver, const StateItem& item) const;
        
        void insert2(ClusterType type, const std::string &columnName, const StateRange &range, gid_t gid);
        void insert(ClusterType type, const std::vector<StateItem> &items, gid_t gid);
        
        /**
         * @brief 주어진 트랜잭션을 클러스터에 추가한다.
         */
        void insert(const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver);

        void normalizeWithResolver(const RelationshipResolver &resolver);
        
        std::optional<StateRange> match(ClusterType type, const std::string &columnName, const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver) const;
        
        void describe();
        
        void merge();
        
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
        
        std::vector<std::string> generateReplaceQuery(const std::string &targetDB,
                                                      const std::string &intermediateDB,
                                                      const RelationshipResolver &resolver,
                                                      const std::vector<ForeignKey> &foreignKeys);
        
        template <typename Archive>
        void serialize(Archive &archive);

        void toProtobuf(ultraverse::state::v2::proto::StateCluster *out) const;
        void fromProtobuf(const ultraverse::state::v2::proto::StateCluster &msg);

        void refreshTargetCache(const RelationshipResolver &resolver);

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

        struct TargetGidSetRef {
            const std::unordered_set<gid_t> *read = nullptr;
            const std::unordered_set<gid_t> *write = nullptr;

            bool contains(gid_t gid) const {
                if (read != nullptr && read->find(gid) != read->end()) {
                    return true;
                }
                if (write != nullptr && write->find(gid) != write->end()) {
                    return true;
                }
                return false;
            }
        };
        
    private:
        
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
        void invalidateTargetCache(const RelationshipResolver &resolver);

        void rebuildResolvedKeyColumnGroups(const RelationshipResolver &resolver);
        
        /**
         * @brief 주어진 gid를 가진 트랜잭션이 재실행 대상인지 확인한다 (internal)
         */
        bool shouldReplay(gid_t gid, const TargetTransactionCache &cache);
        
        LoggerPtr _logger;
        
        std::mutex _clusterInsertionLock;

        std::set<std::string> _keyColumns;
        std::vector<std::vector<std::string>> _keyColumnGroups;
        std::vector<bool> _groupIsComposite;
        std::unordered_map<std::string, std::vector<GroupProjection>> _keyColumnGroupsByTable;
        std::vector<std::vector<std::string>> _resolvedKeyColumnGroups;
        std::vector<bool> _resolvedGroupIsComposite;
        std::unordered_map<std::string, Cluster> _clusters;
        
        std::shared_mutex _targetCacheLock;
        std::unordered_map<std::string, std::unordered_map<StateRange, TargetGidSetRef>> _targetCache;
        std::unordered_map<gid_t, TargetTransactionCache> _rollbackTargets;
        std::unordered_map<gid_t, TargetTransactionCache> _prependTargets;

    };
}

#endif //ULTRAVERSE_STATECLUSTER_HPP
