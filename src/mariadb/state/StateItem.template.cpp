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
        return range.hash();
    }
};

template <>
struct std::hash<std::pair<std::string, StateRange>> {
    std::size_t operator()(const std::pair<std::string, StateRange> &pair) const {
        return std::hash<std::string>()(pair.first) ^ pair.second.hash();
    }
};