//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DBHANDLE_HPP
#define ULTRAVERSE_DBHANDLE_HPP

#include <string>

namespace ultraverse::base {
    class DBHandle {
    public:
        virtual void connect(const std::string &host, int port, const std::string &user, const std::string &password) = 0;
        virtual void disconnect() = 0;
        
        virtual int executeQuery(const std::string &query) = 0;
    };
}

#endif //ULTRAVERSE_DBHANDLE_HPP
