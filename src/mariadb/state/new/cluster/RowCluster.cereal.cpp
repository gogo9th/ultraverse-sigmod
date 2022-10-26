//
// Created by cheesekun on 10/27/22.
//

#include <cereal/types/vector.hpp>
#include <cereal/types/unordered_map.hpp>

#include "RowCluster.hpp"

namespace ultraverse::state::v2 {
    template <typename Archive>
    void RowCluster::serialize(Archive &archive) {
        archive(_clusterMap);
    }
}