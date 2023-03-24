//
// Created by cheesekun on 3/16/23.
//

#include "ProcMatcher.hpp"

namespace ultraverse::state::v2 {
    ProcMatcher::ProcMatcher(const std::vector<std::string> &procedureCodes):
        _sqlParser(),
        _procedureCodes(procedureCodes)
    {
    
    }
    
    int ProcMatcher::matchForward(const std::string &statement, int fromIndex) {
        for (int i = fromIndex; i < _procedureCodes.size(); i++) {
            if (_procedureCodes[i] == statement) {
                return i;
            }
        }

        return -1;
    }
}