//
// Created by cheesekun on 1/20/26.
//

#ifndef ULTRAVERSE_MARIADB_DBHANDLEPOOLADAPTER_HPP
#define ULTRAVERSE_MARIADB_DBHANDLEPOOLADAPTER_HPP

#include <memory>
#include <type_traits>

#include "base/DBHandlePool.hpp"
#include "mariadb/DBHandle.hpp"

namespace ultraverse::mariadb {
    class DBHandleLeaseBase {
    public:
        virtual ~DBHandleLeaseBase() = default;
        virtual DBHandle &get() = 0;
    };

    class DBHandlePoolBase {
    public:
        virtual ~DBHandlePoolBase() = default;
        virtual std::unique_ptr<DBHandleLeaseBase> take() = 0;
        virtual int poolSize() const = 0;
    };

    template <typename T, std::enable_if_t<std::is_base_of_v<DBHandle, T>, bool> = true>
    class DBHandleLeaseAdapter final: public DBHandleLeaseBase {
    public:
        explicit DBHandleLeaseAdapter(ultraverse::DBHandleLease<T> lease):
            _lease(std::move(lease))
        {
        }

        DBHandle &get() override {
            return _lease.get();
        }

    private:
        ultraverse::DBHandleLease<T> _lease;
    };

    template <typename T, std::enable_if_t<std::is_base_of_v<DBHandle, T>, bool> = true>
    class DBHandlePoolAdapter final: public DBHandlePoolBase {
    public:
        explicit DBHandlePoolAdapter(ultraverse::DBHandlePool<T> &pool):
            _pool(pool)
        {
        }

        std::unique_ptr<DBHandleLeaseBase> take() override {
            return std::make_unique<DBHandleLeaseAdapter<T>>(_pool.take());
        }

        int poolSize() const override {
            return _pool.poolSize();
        }

    private:
        ultraverse::DBHandlePool<T> &_pool;
    };
}

#endif //ULTRAVERSE_MARIADB_DBHANDLEPOOLADAPTER_HPP
