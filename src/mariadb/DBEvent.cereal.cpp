//
// Created by basicneural on 2022/08/25.
//

#include <cereal/types/vector.hpp>
#include <cereal/types/memory.hpp>

#include "DBEvent.hpp"

namespace ultraverse::mariadb {

    template <typename Archive>
    void TableMapEvent::serialize(Archive &archive) {
        archive(_timestamp);
        archive(_tableId);
        archive(_database);
        archive(_table);
        archive(_columns);
    }
}
