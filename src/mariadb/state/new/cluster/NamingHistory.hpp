//
// Created by cheesekun on 9/17/22.
//

#ifndef ULTRAVERSE_NAMINGHISTORY_HPP
#define ULTRAVERSE_NAMINGHISTORY_HPP

#include <cstdint>
#include <string>
#include <vector>
#include <utility>

namespace ultraverse::state::v2 {
    class NamingHistory {
    public:
        NamingHistory(const std::string &initialName);
    
        void addRenameHistory(const std::string &newName, uint64_t when);
        
        std::string getName(uint64_t when) const;
        
        std::string getInitialName() const;
        std::string getCurrentName() const;
        
        bool match(const std::string &name, uint64_t when) const;
    private:
        std::string _initialName;
        std::vector<std::pair<uint64_t, std::string>> _namingHistory;
    };
}

#endif //ULTRAVERSE_NAMINGHISTORY_HPP
