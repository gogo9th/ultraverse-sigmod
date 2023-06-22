//
// Created by cheesekun on 6/20/23.
//

#ifndef ULTRAVERSE_STATECLUSTER_HPP
#define ULTRAVERSE_STATECLUSTER_HPP

#include <string>
#include <vector>
#include <set>
#include <map>
#include <mutex>

#include "../../StateItem.h"
#include "../Transaction.hpp"

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
        /**
         * TODO: StateRange는 계속 사용함: StateData를 StateRange로 변경할 것
         */
        class Cluster {
        public:
            std::map<StateRange, std::vector<gid_t>> read;
            std::map<StateRange, std::vector<gid_t>> write;
            
            bool operator&(const Transaction &transaction) const;
        };
        
    public:
        StateCluster(const std::set<std::string> &keyColumns);
        
        const std::set<std::string> &keyColumns() const;
        const std::map<std::string, Cluster> &clusters() const;
        
        void operator<<(const std::shared_ptr<Transaction> &transaction);
    private:
        LoggerPtr _logger;
        
        std::mutex _clusterInsertionLock;
        
        std::set<std::string> _keyColumns;
        std::map<std::string, Cluster> _clusters;
    };
}
            

#endif //ULTRAVERSE_STATECLUSTER_HPP
