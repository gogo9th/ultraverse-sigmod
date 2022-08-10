//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_COMMON_HPP
#define ULTRAVERSE_COMMON_HPP

#include <string>

class DBConnectionOptions {
public:
    std::string host;
    int port;
    std::string user;
    std::string password;
};

#endif //ULTRAVERSE_COMMON_HPP
