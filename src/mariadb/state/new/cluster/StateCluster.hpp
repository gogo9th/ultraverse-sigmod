//
// Created by cheesekun on 6/20/23.
//

#ifndef ULTRAVERSE_STATECLUSTER_HPP
#define ULTRAVERSE_STATECLUSTER_HPP

#include <string>
#include <vector>
#include <unordered_set>
#include <map>
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
            std::map<StateRange, std::unordered_set<gid_t>> read;
            std::map<StateRange, std::unordered_set<gid_t>> write;
            
            static std::optional<StateRange> match(const std::string &columnName, const std::map<StateRange, std::unordered_set<gid_t>> &cluster, CombinedIterator<StateItem> begin, CombinedIterator<StateItem> end);
        };
    public:
        StateCluster(const std::set<std::string> &keyColumns);
        
        const std::set<std::string> &keyColumns() const;
        const std::map<std::string, Cluster> &clusters() const;
        
        bool isKeyColumnItem(const RelationshipResolver &resolver, const StateItem& item) const;
        
        void insert(ClusterType type, const std::string &columnName, const StateRange &range, gid_t gid);
        void insert(ClusterType type, CombinedIterator<StateItem> begin, CombinedIterator<StateItem> end, gid_t gid, const RelationshipResolver &resolver);
        void insert(const std::shared_ptr<Transaction> &transaction, const RelationshipResolver &resolver);
        
        std::optional<StateRange> match(ClusterType type, const std::string &columnName, const std::shared_ptr<Transaction> &transaction) const;
        
    private:
        LoggerPtr _logger;
        
        std::mutex _clusterInsertionLock;
        
        std::set<std::string> _keyColumns;
        std::map<std::string, Cluster> _clusters;
    };
}
            

#endif //ULTRAVERSE_STATECLUSTER_HPP
