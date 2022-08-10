#ifndef STATE_THREAD_POOL_INCLUDED
#define STATE_THREAD_POOL_INCLUDED

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include <mysql/mysql.h>

#include "mariadb/DBHandle.hpp"
#include "common.hpp"

namespace ultraverse::state {
    class StateThreadPool {
    public:
        static const size_t MAX_MYSQL_CONN_COUNT = 100;
        
        StateThreadPool();
        ~StateThreadPool();
        
        static StateThreadPool &Instance() {
            if (_instance == nullptr) {
                _instance = new StateThreadPool();
            }
            return *_instance;
        }
        
        void initialize(DBConnectionOptions connectionOptions);
        
        std::shared_ptr<mariadb::DBHandle> GetMySql();
        
        void PushMySql(std::shared_ptr<mariadb::DBHandle> mysql, bool is_commit = true);
        
        std::shared_ptr<mariadb::DBHandle> PopMySql();
        
        void Resize(size_t num_threads);
        
        size_t Size();
        
        void Release();
        
        void ReleaseMySql();
        
        
        template<class F, class... Args>
        std::future<typename std::result_of<F(Args...)>::type> EnqueueJob(F &&f, Args &&... args) {
            if (_stopAll) {
                throw std::runtime_error("stop all");
            }
            
            using return_type = typename std::result_of<F(Args...)>::type;
            auto job = std::make_shared<std::packaged_task<return_type()>>(
                std::bind(std::forward<F>(f), std::forward<Args>(args)...));
            std::future<return_type> job_result_future = job->get_future();
            {
                std::unique_lock<std::mutex> lock(_jobQMutex);
                _jobs.push([job]() { (*job)(); });
            }
            _jobQCond.notify_one();
            
            return job_result_future;
        }
    
    private:
        void WorkerThread() {
            mysql_thread_init();
            
            while (true) {
                std::unique_lock<std::mutex> lock(_jobQMutex);
                _jobQCond.wait(lock, [this]() { return !this->_jobs.empty() || _stopAll; });
                if (_stopAll && this->_jobs.empty()) {
                    return;
                }
                
                auto job = std::move(_jobs.front());
                _jobs.pop();
                lock.unlock();
                
                job();
            }
            
            mysql_thread_end();
        }
        
        static StateThreadPool *_instance;
        
        DBConnectionOptions _connectionOptions;
        
        std::vector<std::thread> _workerThreads;
        std::queue<std::function<void()>> _jobs;
        std::condition_variable _jobQCond;
        std::mutex _jobQMutex;
        bool _stopAll;
        
        std::shared_ptr<mariadb::DBHandle> _mysql;
        std::queue<std::shared_ptr<mariadb::DBHandle>> _mysqlQueue;
        std::condition_variable _mysqlQCond;
        std::mutex _mysqlQMutex;
    };
}

#endif /* STATE_THREAD_POOL_INCLUDED */
