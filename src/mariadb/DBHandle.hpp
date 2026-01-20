//
// Created by cheesekun on 8/8/22.
//

#ifndef ULTRAVERSE_MARIADB_DBHANDLE_HPP
#define ULTRAVERSE_MARIADB_DBHANDLE_HPP

#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include <mysql/mysql.h>

#include "base/DBHandle.hpp"
#include "utils/log.hpp"

namespace ultraverse::mariadb {
    class DBResult {
    public:
        virtual ~DBResult() = default;
        virtual bool next(std::vector<std::string> &row) = 0;
        virtual size_t rowCount() const = 0;
    };

    /**
     * @brief MySQL/MariaDB DB handle abstraction
     */
    class DBHandle: public base::DBHandle {
    public:
        virtual ~DBHandle() override = default;

        virtual const char *lastError() const = 0;
        virtual int lastErrno() const = 0;
        virtual std::unique_ptr<DBResult> storeResult() = 0;
        virtual int nextResult() = 0;
        virtual void setAutocommit(bool enabled) = 0;
        virtual std::shared_ptr<MYSQL> handle() = 0;

        void consumeResults();
    };

    /**
     * @brief MySQL DB handle implementation
     */
    class MySQLDBHandle: public DBHandle {
    public:
        explicit MySQLDBHandle();
        MySQLDBHandle(MySQLDBHandle &) = delete;
        
        void connect(const std::string &host, int port, const std::string &user, const std::string &password) override;
        void disconnect() override;
    
        int executeQuery(const std::string &query) override;

        const char *lastError() const override;
        int lastErrno() const override;
        std::unique_ptr<DBResult> storeResult() override;
        int nextResult() override;
        void setAutocommit(bool enabled) override;
        std::shared_ptr<MYSQL> handle() override;
        
    private:
        void disableAutoCommit();
        void disableBinlogChecksum();
        
        LoggerPtr _logger;
        std::shared_ptr<MYSQL> _handle;
    };

    /**
     * @brief Mocked DB handle implementation for tests
     */
    class MockedDBHandle: public DBHandle {
    public:
        struct SharedState {
            std::mutex mutex;
            std::vector<std::string> queries;
            std::queue<std::vector<std::vector<std::string>>> results;
            int lastErrno = 0;
            std::string lastError;
        };

        MockedDBHandle();
        explicit MockedDBHandle(std::shared_ptr<SharedState> state);

        void connect(const std::string &host, int port, const std::string &user, const std::string &password) override;
        void disconnect() override;
        int executeQuery(const std::string &query) override;

        const char *lastError() const override;
        int lastErrno() const override;
        std::unique_ptr<DBResult> storeResult() override;
        int nextResult() override;
        void setAutocommit(bool enabled) override;
        std::shared_ptr<MYSQL> handle() override;

        std::shared_ptr<SharedState> sharedState() const;
        static std::shared_ptr<SharedState> defaultSharedState();
        static void resetDefaultSharedState();

    private:
        std::shared_ptr<SharedState> _state;
    };
}

#endif //ULTRAVERSE_MARIADB_DBHANDLE_HPP
