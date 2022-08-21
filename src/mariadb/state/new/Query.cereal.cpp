//
// Created by cheesekun on 8/21/22.
//

#include <cereal/types/vector.hpp>
#include <cereal/types/unordered_map.hpp>

#include "Query.hpp"

namespace ultraverse::state::v2 {
    template <typename Archive>
    void Query::serialize(Archive &archive) {
        archive(
            _timestamp,
            
            _database,
            _statement,
            
            _referenceFile,
            _referencePos,
            
            _beforeHash,
            _afterHash,
            
            _affectedTables,
            _readSet,
            _writeSet,
            _foreignKeySet,
            
            _affectedRows,
            _rowSet,
            _changeSet
        );
    }
}