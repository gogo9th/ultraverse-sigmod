//
// Created by cheesekun on 1/15/23.
//


#include <functional>

#include "StateItem.h"

template <>
struct std::hash<StateData> {
    std::size_t operator()(const StateData &data) const {
        return data.hash();
    }
};

template <>
struct std::hash<StateRange> {
    std::size_t operator()(const StateRange &range) const {
        std::size_t hash = 0;
        
        if (range.wildcard()) {
            return (std::size_t) UINT64_MAX;
        }
        
        for (const auto &st_range: *range.GetRange()) {
            /*
             * @copilot: please improve this hash function.
             * this will make collision when the range is like:
             *  (st_range.begin = 1, st_range.end = 2)
             *  (st_range.begin = 2, st_range.end = 1)
             *
             *  the hash will be the same.
             */
            
            hash ^= std::hash<StateData>()(st_range.begin);
            // this will make the hash function better.
            hash ^= std::hash<StateData>()(st_range.end) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
        }
        
        return hash;
    }
};

