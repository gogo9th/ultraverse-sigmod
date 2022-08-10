//
// Created by cheesekun on 8/8/22.
//

#ifndef ULTRAVERSE_DBHANDLE_HPP
#define ULTRAVERSE_DBHANDLE_HPP

#include <memory>

#include <mysql/mysql.h>

namespace ultraverse::mariadb {
    class DBHandle {
    public:
        explicit DBHandle();
        DBHandle(DBHandle &) = delete;
        
        void connect(const std::string &host, int port, const std::string &user, const std::string &password);
        void disconnect();
        
        std::shared_ptr<MYSQL> handle();
        
    protected:
        void disableAutoCommit();
        void disableBinlogChecksum();
        
    private:
        std::shared_ptr<MYSQL> _handle;
    };
}

#endif //ULTRAVERSE_DBHANDLE_HPP
