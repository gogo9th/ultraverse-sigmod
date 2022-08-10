#ifndef STATE_REFERENCE_INCLUDED
#define STATE_REFERENCE_INCLUDED

#include <mutex>
#include <condition_variable>

class StateReference
{
public:
  StateReference()
      : reference_count(0),
        release_count(0),
        try_count(0),
        noti_count(0)
  {
  }

  bool IsReferenced() { return release_count < 0 || reference_count != release_count; }
  void IncReference() { ++reference_count; }
  void DecReference() { ++release_count; }
  void NoneReference() { release_count = -1; }

  bool TryAccess()
  {
    ++try_count;
    if (reference_count == try_count)
    {
      NoneReference();
      return true;
    }
    else
      return false;
  }

  void Notify()
  {
    std::unique_lock<std::mutex> lock(mutex);

    ++noti_count;
    cv.notify_one();
  }

  bool IsWait()
  {
    std::unique_lock<std::mutex> lock(mutex);

    if (reference_count != noti_count)
      return true;
    else
      return false;
  }

  void Wait()
  {
    std::unique_lock<std::mutex> lock(mutex);

    while (reference_count != noti_count)
      cv.wait(lock);
  }

  void Print(const char *hdr, size_t ref_size, const char *query)
  {
    printf("[%15s] ref_size(%2lu) ref(ref_count(%2d) rel_count(%2d) try_count(%2d) noti_count(%2d)) query(%s)\n",
           hdr, ref_size, reference_count, release_count, try_count, noti_count, query);
  }

private:
  int reference_count;
  int release_count;
  int try_count;
  int noti_count;

  std::mutex mutex;
  std::condition_variable cv;
};

#endif /* STATE_REFERENCE_INCLUDED */
