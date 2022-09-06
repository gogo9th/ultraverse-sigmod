//
// Created by cheesekun on 9/6/22.
//

#ifndef ULTRAVERSE_TASKEXECUTOR_HPP
#define ULTRAVERSE_TASKEXECUTOR_HPP

#include <iostream>
#include <functional>
#include <future>
#include <mutex>
#include <queue>

class TaskExecutor {
public:
    TaskExecutor(int size) {
        for (int i = 0; i < size; i++) {
            _workers.emplace_back(&TaskExecutor::workerLoop, this);
        }
    }
    
    template <typename T>
    std::shared_ptr<std::promise<T>> post(std::function<T()> workerFn) {
        auto promise = std::make_shared<std::promise<T>>();
    
        {
            std::lock_guard lockGuard(_mutex);
            auto wrapperFn = [workerFn = std::move(workerFn), promise]() {
                promise->set_value(workerFn());
            };
            _tasks.push(wrapperFn);
        }
        _condvar.notify_one();
        
        return promise;
    }
    
private:
    [[noreturn]]
    void workerLoop() {
        while (true) {
            std::unique_lock lock(_mutex);
            _condvar.wait(lock, [this] { return !_tasks.empty(); });
    
            auto task = std::move(_tasks.front());
            _tasks.pop();
            lock.unlock();
            
            task();
        }
    }
    
    std::queue<std::function<void()>> _tasks;
    std::vector<std::thread> _workers;
    std::mutex _mutex;
    std::condition_variable _condvar;
};


#endif //ULTRAVERSE_TASKEXECUTOR_HPP
