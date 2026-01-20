//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DBHANDLEPOOL_HPP
#define ULTRAVERSE_DBHANDLEPOOL_HPP


#include <string>
#include <memory>
#include <functional>
#include <type_traits>
#include <queue>
#include <mutex>
#include <condition_variable>

#include "DBHandle.hpp"

namespace ultraverse {
    
    
    template <typename T, std::enable_if_t<std::is_base_of_v<base::DBHandle, T>, bool> = true>
    class DBHandleLease {
    public:
        DBHandleLease(std::shared_ptr<T> handle, std::function<void()> releaser):
            _handle(handle),
            _releaser(std::move(releaser))
        {
        
        }
        
        ~DBHandleLease() {
            if (this->_releaser) {
                this->_releaser();
            }
        }
        
        DBHandleLease(DBHandleLease &) = delete;
        DBHandleLease(DBHandleLease &&) noexcept = default;
        
        T &get() {
            return *_handle;
        }
        
    private:
        std::shared_ptr<T> _handle;
        std::function<void()> _releaser;
    };
    
    template <typename T, std::enable_if_t<std::is_base_of_v<base::DBHandle, T>, bool> = true>
    class DBHandlePool {
    public:
        explicit DBHandlePool(
            int poolSize,
            const std::string &host,
            int port,
            const std::string &user,
            const std::string &password
        ):
            _poolSize(poolSize)
        {
            for (int i = 0; i < poolSize; i++) {
                 auto dbHandle = std::make_shared<T>();
                 dbHandle->connect(host, port, user, password);
                 _handles.push(dbHandle);
            }
        }
        
        DBHandleLease<T> take() {
            std::unique_lock lock(_mutex);
            
            if (_handles.empty()) {
                _condvar.wait(lock, [this] { return !_handles.empty(); });
            }
            
            auto handle = std::move(_handles.front());
            _handles.pop();
            lock.unlock();
            
            return DBHandleLease<T>(handle, [this, handle]() {
                std::scoped_lock lock(_mutex);
                this->_handles.push(handle);
                this->_condvar.notify_one();
            });
        }
        
        int poolSize() const {
            return _poolSize;
        }
        
    private:
        int _poolSize;
        std::mutex _mutex;
        std::condition_variable _condvar;
        std::queue<std::shared_ptr<T>> _handles;
        
    };
}


#endif //ULTRAVERSE_DBHANDLEPOOL_HPP
