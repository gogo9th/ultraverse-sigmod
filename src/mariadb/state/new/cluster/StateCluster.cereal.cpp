//
// Created by cheesekun on 7/14/23.
//

#include <cereal/types/vector.hpp>
#include <cereal/types/set.hpp>
#include <cereal/types/unordered_map.hpp>

#include "StateCluster.hpp"

namespace ultraverse::state::v2 {
    template <typename Archive>
    void StateCluster::Cluster::serialize(Archive &archive) {
        archive(
            read,
            write
        );
    }
    
    template <typename Archive>
    void StateCluster::serialize(Archive &archive) {
        archive(
            _clusters
        );
    }
}
