//
// Created by cheesekun on 8/21/22.
//

#include <cereal/cereal.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/unordered_set.hpp>
#include <cereal/types/set.hpp>
#include <cereal/types/unordered_map.hpp>

#include "Query.hpp"

namespace ultraverse::state::v2 {
    template <typename Archive>
    void Query::serialize(Archive &archive) {
        archive(
            _type,
            _timestamp,

            _database,
            _statement,

            _flags,

            _readSet,
            _writeSet,
            _varMap,

            _readColumns,
            _writeColumns,

            _affectedRows,

            _statementContext
        );
    }
}