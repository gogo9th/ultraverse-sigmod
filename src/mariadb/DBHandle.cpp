//
// Created by cheesekun on 8/8/22.
//

#include <stdexcept>

#include <fmt/core.h>

#include "DBHandle.hpp"


namespace ultraverse::mariadb {
    
    DBHandle::DBHandle():
        _handle(mysql_init(nullptr), mysql_close)
    {
    
    }
    
    void DBHandle::connect(const std::string &host, int port, const std::string &user, const std::string &password) {
        mysql_real_connect(_handle.get(), host.c_str(), user.c_str(), password.c_str(), nullptr, port, nullptr, 0);
        if (mysql_errno(_handle.get()) != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}.", mysql_errno(_handle.get()))
            );
        }
        
        // TODO: mariadb는 master_binlog_checksum을 TRUE로, mysql은..
        if (mysql_query(_handle.get(), "SET @master_binlog_checksum=TRUE") != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}.", mysql_errno(_handle.get()))
            );
        }
    }
    
    void DBHandle::disconnect() {
        mysql_close(_handle.get());
    }
    
    std::shared_ptr<MYSQL> DBHandle::handle() {
        return _handle;
    }
}