//
// Created by cheesekun on 1/9/23.
//

#ifndef ULTRAVERSE_STRINGUTIL_HPP
#define ULTRAVERSE_STRINGUTIL_HPP

#include <string>
#include <vector>
#include <utility>

namespace ultraverse::utility {
    std::pair<std::string, std::string> splitTableName(const std::string &input);
    
    std::vector<std::string> split(const std::string &inputStr, char character);
    
    std::string replaceAll(const std::string &source, const std::string from, const std::string to);
    
    std::string normalizeColumnName(const std::string &columnName);

    std::string toLower(const std::string &source);
}


#endif //ULTRAVERSE_STRINGUTIL_HPP
