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

#include "mysql.h"
#include "db_state_change.h"

class StateThreadPool
{
public:
  static const size_t MAX_MYSQL_CONN_COUNT = 100;

  StateThreadPool()
      : stop_all_(false)
  {
    mysql_ = open_mysql();
  }

  ~StateThreadPool()
  {
    Release();
    ReleaseMySql();
  }

  static StateThreadPool &Instance()
  {
    if (instance_ == NULL)
    {
      instance_ = new StateThreadPool();
    }

    return *instance_;
  }

  MYSQL *GetMySql()
  {
    return mysql_;
  }

  void PushMySql(MYSQL *mysql, bool is_commit=true)
  {
    if (is_commit)
    {
      if (mysql_commit(mysql) != 0)
      {
        error("[StateThreadPool::PushMySql] failed to commit [%s]", mysql_error(mysql));
      }
    }

    std::unique_lock<std::mutex> lock(m_mysql_q_);
    mysql_q_.push(mysql);
    cv_mysql_q_.notify_one();
  }

  MYSQL *PopMySql()
  {
    std::unique_lock<std::mutex> lock(m_mysql_q_);
    cv_mysql_q_.wait(lock, [this]() {
      if (!this->mysql_q_.empty()) {
        return true;
      }
      mysql_q_.push(open_mysql());
      return true;
    });

    auto mysql = mysql_q_.front();
    mysql_q_.pop();

    if (mysql->db[0] == '\0')
    {
      mysql_select_db(mysql, STATE_CHANGE_DATABASE);
    }

    return mysql;
  }

  void Resize(size_t num_threads)
  {
    stop_all_ = false;

    for (size_t i = worker_threads_.size(); i < num_threads; ++i)
    {
      worker_threads_.emplace_back([this]() { this->WorkerThread(); });
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

  void ReleaseMySql()
  {
    close_mysql(mysql_);
    mysql_ = NULL;

    while (!mysql_q_.empty())
    {
      close_mysql(mysql_q_.front());
      mysql_q_.pop();
    }
  }

  template <class F, class... Args>
  std::future<typename std::result_of<F(Args...)>::type> EnqueueJob(F &&f, Args &&... args)
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
      jobs_.push([job]() { (*job)(); });
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
      cv_job_q_.wait(lock, [this]() { return !this->jobs_.empty() || stop_all_; });
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

  static StateThreadPool *instance_;
  std::vector<std::thread> worker_threads_;
  std::queue<std::function<void()>> jobs_;
  std::condition_variable cv_job_q_;
  std::mutex m_job_q_;
  bool stop_all_;

  MYSQL *mysql_;
  std::queue<MYSQL*> mysql_q_;
  std::condition_variable cv_mysql_q_;
  std::mutex m_mysql_q_;
};

#endif /* STATE_THREAD_POOL_INCLUDED */
