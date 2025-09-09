//
// Created by cheesekun on 8/8/22.
//

#include <stdexcept>

#include <fmt/core.h>

#include "DBHandle.hpp"


namespace ultraverse::mariadb {
    
    DBHandle::DBHandle():
        _handle(mysql_init(nullptr), mysql_close),
        _logger(createLogger("mariadb::DBHandle"))
    {
        unsigned int timeout = 15;
        mysql_options(_handle.get(), MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&timeout);
        mysql_options(_handle.get(), MYSQL_OPT_CONNECT_ATTR_RESET, 0);
        mysql_options4(_handle.get(), MYSQL_OPT_CONNECT_ATTR_ADD, "program_name", "ultraverse");
    
        char reconnect = 1;
        mysql_options(_handle.get(), MYSQL_OPT_RECONNECT, &reconnect);
    }
    
    void DBHandle::connect(const std::string &host, int port, const std::string &user, const std::string &password) {
        mysql_real_connect(_handle.get(), host.c_str(), user.c_str(), password.c_str(), nullptr, port, nullptr, 0);
        if (mysql_errno(_handle.get()) != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}: {}", mysql_errno(_handle.get()), mysql_error(_handle.get()))
            );
        }
        
        disableAutoCommit();
        // disableBinlogChecksum();
    }
    
    void DBHandle::disconnect() {
        mysql_close(_handle.get());
    }
    
    void DBHandle::disableAutoCommit() {
        if (mysql_autocommit(_handle.get(), false) != 0) {
            throw std::runtime_error(
                fmt::format("failed to turn off autocommit: %s", mysql_error(_handle.get()))
            );
        }
    }
    
    void DBHandle::disableBinlogChecksum() {
    
        if (mysql_query(_handle.get(), "SET @master_heartbeat_period=10240") != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}.", mysql_errno(_handle.get()))
            );
        }
        
        // TODO: mariadb는 master_binlog_checksum을 TRUE로, mysql은..
        if (mysql_query(_handle.get(), "SET @master_binlog_checksum='NONE'") != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}.", mysql_errno(_handle.get()))
            );
        }
        if (mysql_query(_handle.get(), "SET @binlog_checksum='NONE'") != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}.", mysql_errno(_handle.get()))
            );
        }
    
        if (mysql_query(_handle.get(), "SET @mariadb_slave_capability=0") != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}.", mysql_errno(_handle.get()))
            );
        }
    
        /*
        if (mysql_query(_handle.get(), "SET @rpl_semi_sync_slave=1") != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}.", mysql_errno(_handle.get()))
            );
        }
         */

    }
    
    int DBHandle::executeQuery(const std::string &query) {
        // _logger->trace("executing query: {}", query);
        
        if (mysql_real_query(_handle.get(), query.c_str(), query.size()) != 0) {
            auto mysqlErrno = mysql_errno(_handle.get());
            auto *message = mysql_error(_handle.get());
            _logger->warn("executeQuery() returned non-zero code: {} ({})", mysqlErrno, message);
            return mysqlErrno;
        }
        
        return 0;
    }
    
    std::shared_ptr<MYSQL> DBHandle::handle() {
        return _handle;
    }
    
    DBHandle::operator MYSQL *() {
        return _handle.get();
    }
 
}