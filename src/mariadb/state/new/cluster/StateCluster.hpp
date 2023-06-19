//
// Created by cheesekun on 6/20/23.
//

#ifndef ULTRAVERSE_STATECLUSTER_HPP
#define ULTRAVERSE_STATECLUSTER_HPP

#include <string>
#include <vector>
#include <set>
#include <map>

#include "../../StateItem.h"
#include "../Transaction.hpp"

#include "utils/log.hpp"

/**
 * @brief Row-level clustering을 위한 클래스
 *
 * TODO: alias 지원
 * TODO: foreign key 지원
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
 */
namespace ultraverse::state::v2 {
    
    class StateCluster {
    public:
        struct Cluster {
            std::map<StateData, std::vector<gid_t>> read;
            std::map<StateData, std::vector<gid_t>> write;
        };
        
    public:
        StateCluster(const std::vector<std::string> &keyColumns);
        
        const std::vector<std::string> &keyColumns() const;
        const std::map<std::string, Cluster> &clusters() const;
        
        void operator<<(const std::shared_ptr<Transaction> &transaction);
    private:
        LoggerPtr _logger;
        
        std::vector<std::string> _keyColumns;
        std::map<std::string, Cluster> _clusters;
    };
}
            

#endif //ULTRAVERSE_STATECLUSTER_HPP
