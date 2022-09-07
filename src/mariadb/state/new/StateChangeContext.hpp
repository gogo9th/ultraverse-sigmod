//
// Created by cheesekun on 9/7/22.
//

#ifndef ULTRAVERSE_STATECHANGECONTEXT_HPP
#define ULTRAVERSE_STATECHANGECONTEXT_HPP

#include <cstdint>

#include <string>
#include <list>
#include <map>

namespace ultraverse::state::v2 {
    struct RenameHistory {
        uint64_t time;
        std::string name;
    };
    
    class StateChangeContext {
    public:
        std::map<std::string, std::list<RenameHistory>> renameHistoryMap;
    };
}

#endif //ULTRAVERSE_STATECHANGECONTEXT_HPP
