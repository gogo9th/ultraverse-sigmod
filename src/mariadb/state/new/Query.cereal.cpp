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
    void Query::save(Archive &archive) const {
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

    template <typename Archive>
    void Query::load(Archive &archive, std::uint32_t const version) {
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

            _affectedRows
        );

        if (version >= 1) {
            archive(_statementContext);
        } else {
            _statementContext.clear();
        }
    }
}

CEREAL_CLASS_VERSION(ultraverse::state::v2::Query, 1);
