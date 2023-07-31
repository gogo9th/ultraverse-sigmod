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
#include "utils/log.hpp"

namespace ultraverse::mariadb {
    /**
     * @brief MySQL DB 핸들 클래스
     */
    class DBHandle: base::DBHandle {
    public:
        explicit DBHandle();
        DBHandle(DBHandle &) = delete;
        
        void connect(const std::string &host, int port, const std::string &user, const std::string &password) override;
        void disconnect() override;
    
        int executeQuery(const std::string &query) override;
    
        std::shared_ptr<MYSQL> handle();
        operator MYSQL *();
        
        void disableAutoCommit();
        void disableBinlogChecksum();
        
    private:
        LoggerPtr _logger;
        std::shared_ptr<MYSQL> _handle;
    };
}

#endif //ULTRAVERSE_MARIADB_DBHANDLE_HPP
