//
// Created by cheesekun on 8/21/22.
//

#include <cereal/types/vector.hpp>
#include <cereal/types/memory.hpp>
#include <cereal/types/unordered_map.hpp>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    template <typename Archive>
    void Transaction::serialize(Archive &archive) {
        archive(
            _timestamp,
            
            _gid,
            _xid,
            _isSuccessful,
            
            _flags,
            _nextPos,
            
            _dependencies,
            
            _queries
        );
    }
}