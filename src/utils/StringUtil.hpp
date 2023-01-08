//
// Created by cheesekun on 1/9/23.
//

#ifndef ULTRAVERSE_STRINGUTIL_HPP
#define ULTRAVERSE_STRINGUTIL_HPP

#include <string>

namespace ultraverse::utility {
    std::string replaceAll(const std::string &source, const std::string from, const std::string to);
    
    std::string normalizeColumnName(const std::string &columnName);
}


#endif //ULTRAVERSE_STRINGUTIL_HPP
