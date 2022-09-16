//
// Created by cheesekun on 9/14/22.
//

#ifndef ULTRAVERSE_CANDIDATECOLUMN_HPP
#define ULTRAVERSE_CANDIDATECOLUMN_HPP

#include <string>

#include "mariadb/state/StateItem.h"

namespace ultraverse::state::v2 {
    class CandidateColumn {
    public:
        CandidateColumn(const std::string &name, const StateRange &range):
            name(name),
            range(range)
        {
        
        }
    
        std::string name;
        StateRange range;
    };
}



#endif //ULTRAVERSE_CANDIDATECOLUMN_HPP
