#ifndef STATE_SIMPLE_THREAD_POOL_INCLUDED
#define STATE_SIMPLE_THREAD_POOL_INCLUDED

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class StateSimpleThreadPool
{
public:
    StateSimpleThreadPool()
        : stop_all_(false)
    {
    }

    ~StateSimpleThreadPool()
    {
        Release();
    }

    static StateSimpleThreadPool &Instance()
    {
        if (instance_ == NULL)
        {
            instance_ = new StateSimpleThreadPool();
        }

        return *instance_;
    }

    void Resize(size_t num_threads)
    {
        stop_all_ = false;

        for (size_t i = worker_threads_.size(); i < num_threads; ++i)
        {
            worker_threads_.emplace_back([this]()
                                         { this->WorkerThread(); });
        }
    }

    size_t Size()
    {
        return worker_threads_.size();
    }

    void Release()
    {
        stop_all_ = true;
        cv_job_q_.notify_all();

        for (auto &t : worker_threads_)
        {
            t.join();
        }
        worker_threads_.clear();
    }

    template <class F, class... Args>
    std::future<typename std::result_of<F(Args...)>::type> EnqueueJob(F &&f, Args &&...args)
    {
        if (stop_all_)
        {
            throw std::runtime_error("stop all");
        }

        using return_type = typename std::result_of<F(Args...)>::type;
        auto job = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        std::future<return_type> job_result_future = job->get_future();
        {
            std::unique_lock<std::mutex> lock(m_job_q_);
            jobs_.push([job]()
                       { (*job)(); });
        }
        cv_job_q_.notify_one();

        return job_result_future;
    }

private:
    void WorkerThread()
    {
        mysql_thread_init();

        while (true)
        {
            std::unique_lock<std::mutex> lock(m_job_q_);
            cv_job_q_.wait(lock, [this]()
                           { return !this->jobs_.empty() || stop_all_; });
            if (stop_all_ && this->jobs_.empty())
            {
                return;
            }

            auto job = std::move(jobs_.front());
            jobs_.pop();
            lock.unlock();

            job();
        }

        mysql_thread_end();
    }

    static StateSimpleThreadPool *instance_;
    std::vector<std::thread> worker_threads_;
    std::queue<std::function<void()>> jobs_;
    std::condition_variable cv_job_q_;
    std::mutex m_job_q_;
    bool stop_all_;
};

#endif /* STATE_SIMPLE_THREAD_POOL_INCLUDED */
