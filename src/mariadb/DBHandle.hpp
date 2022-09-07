//
// Created by cheesekun on 8/8/22.
//

#ifndef ULTRAVERSE_MARIADB_DBHANDLE_HPP
#define ULTRAVERSE_MARIADB_DBHANDLE_HPP

#include <string>
#include <vector>
#include <memory>

#include <mysql/mysql.h>

#include "base/DBHandle.hpp"

namespace ultraverse::mariadb {
    class DBHandle: base::DBHandle {
    public:
        explicit DBHandle();
        DBHandle(DBHandle &) = delete;
        
        void connect(const std::string &host, int port, const std::string &user, const std::string &password) override;
        void disconnect() override;
        
        std::shared_ptr<MYSQL> handle();
        operator MYSQL *();
    
        void disableAutoCommit();
        void disableBinlogChecksum();
        
    private:
        std::shared_ptr<MYSQL> _handle;
    };
}

#endif //ULTRAVERSE_MARIADB_DBHANDLE_HPP
