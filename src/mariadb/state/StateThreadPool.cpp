//
// Created by cheesekun on 8/10/22.
//

#include <mysql/mysql.h>
#include <fmt/core.h>

#include "StateThreadPool.h"

#include "mariadb/DBHandle.hpp"
#include "state_log_hdr.h"

namespace ultraverse::state {
    using namespace ultraverse::mariadb;
    
    StateThreadPool *StateThreadPool::_instance = nullptr;
    
    StateThreadPool::StateThreadPool() :
        _stopAll(false) {
    }
    
    StateThreadPool::~StateThreadPool() {
        Release();
        ReleaseMySql();
    }
    
    
    
    void StateThreadPool::initialize(DBConnectionOptions connectionOptions) {
        _connectionOptions = connectionOptions;
        
        _mysql = std::make_shared<mariadb::DBHandle>();
        _mysql->connect(
            connectionOptions.host,
            connectionOptions.port,
            connectionOptions.user,
            connectionOptions.password
        );
    }
    
    std::shared_ptr<mariadb::DBHandle> StateThreadPool::GetMySql() {
        return _mysql;
    }
    
    void StateThreadPool::PushMySql(std::shared_ptr<mariadb::DBHandle> mysql, bool is_commit) {
        if (is_commit) {
            if (mysql_commit(mysql->handle().get()) != 0) {
                throw std::runtime_error(
                    fmt::format("[StateThreadPool::PushMySql] failed to commit [%s]", mysql_error(mysql->handle().get()))
                );
            }
        }
        
        std::unique_lock<std::mutex> lock(_mysqlQMutex);
        _mysqlQueue.push(mysql);
        _mysqlQCond.notify_one();
    }
    
    std::shared_ptr<mariadb::DBHandle> StateThreadPool::PopMySql() {
        std::unique_lock<std::mutex> lock(_mysqlQMutex);
        _mysqlQCond.wait(lock, [this]() {
            if (!this->_mysqlQueue.empty()) {
                return true;
            }
            
            auto mysql = std::make_shared<mariadb::DBHandle>();
            mysql->connect(
                _connectionOptions.host,
                _connectionOptions.port,
                _connectionOptions.user,
                _connectionOptions.password
            );
    
            _mysqlQueue.push(mysql);
            return true;
        });
        
        auto mysql = _mysqlQueue.front();
        _mysqlQueue.pop();
        
        if (mysql->handle()->db[0] == '\0') {
            mysql_select_db(mysql->handle().get(), STATE_CHANGE_DATABASE);
        }
        
        return mysql;
    }
    
    void StateThreadPool::Resize(size_t num_threads) {
        _stopAll = false;
        
        for (size_t i = _workerThreads.size(); i < num_threads; ++i) {
            _workerThreads.emplace_back([this]() { this->WorkerThread(); });
        }
    }
    
    size_t StateThreadPool::Size() {
        return _workerThreads.size();
    }
    
    void StateThreadPool::Release() {
        _stopAll = true;
        _jobQCond.notify_all();
        
        for (auto &t: _workerThreads) {
            t.join();
        }
        _workerThreads.clear();
    }
    
    void StateThreadPool::ReleaseMySql() {
        _mysql = nullptr;
        
        while (!_mysqlQueue.empty()) {
            _mysqlQueue.front()->disconnect();
            _mysqlQueue.pop();
        }
    }
}