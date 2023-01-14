//
// Created by cheesekun on 1/15/23.
//


#include <functional>

#include "StateItem.h"

template<>
struct std::hash<StateData>
{
    size_t operator()(StateData const& data) const noexcept
    {
        size_t hashVal = 0;
        
        std::string strVal;
        data.Get(strVal);
        hashVal = std::hash<std::string>{}(strVal);
       
        return hashVal;
    }
};
