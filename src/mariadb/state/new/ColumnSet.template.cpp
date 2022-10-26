//
// Created by cheesekun on 10/27/22.
//

#include <functional>

#include "Query.hpp"

template<>
struct std::hash<ultraverse::state::v2::ColumnSet>
{
    size_t operator()(ultraverse::state::v2::ColumnSet const& columnSet) const noexcept
    {
        size_t hashVal = 0;
        
        for (const auto &column: columnSet) {
            size_t val = std::hash<std::string>{}(column);
            
            hashVal = hashVal ^ (val << 1);
        }
        
        return hashVal;
    }
};
