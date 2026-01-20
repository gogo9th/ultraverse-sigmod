//
// Created by cheesekun on 8/8/22.
//

#include <stdexcept>

#include <fmt/core.h>

#include "DBHandle.hpp"


namespace ultraverse::mariadb {
    namespace {
        class MySQLResult final: public DBResult {
        public:
            explicit MySQLResult(MYSQL_RES *result):
                _result(result),
                _numFields(result != nullptr ? mysql_num_fields(result) : 0),
                _rowCount(result != nullptr ? mysql_num_rows(result) : 0)
            {
            }

            ~MySQLResult() override {
                if (_result != nullptr) {
                    mysql_free_result(_result);
                }
            }

            bool next(std::vector<std::string> &row) override {
                if (_result == nullptr) {
                    return false;
                }

                MYSQL_ROW mysqlRow = mysql_fetch_row(_result);
                if (mysqlRow == nullptr) {
                    return false;
                }

                row.clear();
                row.reserve(_numFields);

                for (unsigned int i = 0; i < _numFields; i++) {
                    row.emplace_back(mysqlRow[i] != nullptr ? mysqlRow[i] : "");
                }

                return true;
            }

            size_t rowCount() const override {
                return _rowCount;
            }

        private:
            MYSQL_RES *_result;
            unsigned int _numFields;
            size_t _rowCount;
        };

        class MockedResult final: public DBResult {
        public:
            explicit MockedResult(std::vector<std::vector<std::string>> rows):
                _rows(std::move(rows))
            {
            }

            bool next(std::vector<std::string> &row) override {
                if (_index >= _rows.size()) {
                    return false;
                }

                row = _rows[_index++];
                return true;
            }

            size_t rowCount() const override {
                return _rows.size();
            }

        private:
            std::vector<std::vector<std::string>> _rows;
            size_t _index = 0;
        };
    }

    void DBHandle::consumeResults() {
        do {
            auto result = storeResult();
            (void) result;
        } while (nextResult() == 0);
    }

    MySQLDBHandle::MySQLDBHandle():
        _handle(mysql_init(nullptr), mysql_close),
        _logger(createLogger("mariadb::MySQLDBHandle"))
    {
        unsigned int timeout = 15;
        mysql_options(_handle.get(), MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&timeout);
        mysql_options(_handle.get(), MYSQL_OPT_CONNECT_ATTR_RESET, 0);
        mysql_options4(_handle.get(), MYSQL_OPT_CONNECT_ATTR_ADD, "program_name", "ultraverse");
    
        char reconnect = 1;
        mysql_options(_handle.get(), MYSQL_OPT_RECONNECT, &reconnect);
    }
    
    void MySQLDBHandle::connect(const std::string &host, int port, const std::string &user, const std::string &password) {
        mysql_real_connect(_handle.get(), host.c_str(), user.c_str(), password.c_str(), nullptr, port, nullptr, 0);
        if (mysql_errno(_handle.get()) != 0) {
            throw std::runtime_error(
                fmt::format("mysql_real_connect returned {}: {}", mysql_errno(_handle.get()), mysql_error(_handle.get()))
            );
        }
        
        disableAutoCommit();
        // disableBinlogChecksum();
    }
    
    void MySQLDBHandle::disconnect() {
        mysql_close(_handle.get());
    }
    
    void MySQLDBHandle::disableAutoCommit() {
        if (mysql_autocommit(_handle.get(), false) != 0) {
            throw std::runtime_error(
                fmt::format("failed to turn off autocommit: %s", mysql_error(_handle.get()))
            );
        }
    }
    
    void MySQLDBHandle::disableBinlogChecksum() {
    
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
    
    int MySQLDBHandle::executeQuery(const std::string &query) {
        // _logger->trace("executing query: {}", query);
        
        if (mysql_real_query(_handle.get(), query.c_str(), query.size()) != 0) {
            auto mysqlErrno = mysql_errno(_handle.get());
            auto *message = mysql_error(_handle.get());
            _logger->warn("executeQuery() returned non-zero code: {} ({})", mysqlErrno, message);
            return mysqlErrno;
        }
        
        return 0;
    }

    const char *MySQLDBHandle::lastError() const {
        return mysql_error(_handle.get());
    }

    int MySQLDBHandle::lastErrno() const {
        return mysql_errno(_handle.get());
    }

    std::unique_ptr<DBResult> MySQLDBHandle::storeResult() {
        MYSQL_RES *result = mysql_store_result(_handle.get());
        if (result == nullptr) {
            return nullptr;
        }

        return std::make_unique<MySQLResult>(result);
    }

    int MySQLDBHandle::nextResult() {
        return mysql_next_result(_handle.get());
    }

    void MySQLDBHandle::setAutocommit(bool enabled) {
        mysql_autocommit(_handle.get(), enabled);
    }
    
    std::shared_ptr<MYSQL> MySQLDBHandle::handle() {
        return _handle;
    }

    MockedDBHandle::MockedDBHandle():
        _state(defaultSharedState())
    {
    }

    MockedDBHandle::MockedDBHandle(std::shared_ptr<SharedState> state):
        _state(std::move(state))
    {
        if (_state == nullptr) {
            _state = defaultSharedState();
        }
    }

    void MockedDBHandle::connect(const std::string &host, int port, const std::string &user, const std::string &password) {
        (void) host;
        (void) port;
        (void) user;
        (void) password;
    }

    void MockedDBHandle::disconnect() {
    }

    int MockedDBHandle::executeQuery(const std::string &query) {
        std::scoped_lock lock(_state->mutex);
        _state->queries.push_back(query);
        return _state->lastErrno;
    }

    const char *MockedDBHandle::lastError() const {
        return _state->lastError.c_str();
    }

    int MockedDBHandle::lastErrno() const {
        return _state->lastErrno;
    }

    std::unique_ptr<DBResult> MockedDBHandle::storeResult() {
        std::scoped_lock lock(_state->mutex);
        if (_state->results.empty()) {
            return nullptr;
        }

        auto rows = std::move(_state->results.front());
        _state->results.pop();
        return std::make_unique<MockedResult>(std::move(rows));
    }

    int MockedDBHandle::nextResult() {
        std::scoped_lock lock(_state->mutex);
        return _state->results.empty() ? 1 : 0;
    }

    void MockedDBHandle::setAutocommit(bool enabled) {
        (void) enabled;
    }

    std::shared_ptr<MYSQL> MockedDBHandle::handle() {
        return nullptr;
    }

    std::shared_ptr<MockedDBHandle::SharedState> MockedDBHandle::sharedState() const {
        return _state;
    }

    std::shared_ptr<MockedDBHandle::SharedState> MockedDBHandle::defaultSharedState() {
        static auto state = std::make_shared<SharedState>();
        return state;
    }

    void MockedDBHandle::resetDefaultSharedState() {
        auto state = defaultSharedState();
        std::scoped_lock lock(state->mutex);
        state->queries.clear();
        while (!state->results.empty()) {
            state->results.pop();
        }
        state->lastErrno = 0;
        state->lastError.clear();
    }
 
}
