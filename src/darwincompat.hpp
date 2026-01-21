#ifndef __ULTRAVERSE_DARWINCOMPAT_HPP__
#define __ULTRAVERSE_DARWINCOMPAT_HPP__

#include <unistd.h>

#include <cstdint>

#ifdef __APPLE__

// FIXME: is it okay to just map lseek64 to lseek on macOS?
#define lseek64     lseek
#define ftruncate64 ftruncate

inline int syncfs(int fd) {
    #warning "XXX: since syncfs(2) is not available on macOS, using fsync(2) as a workaround. This may not have the same behavior (and may cause performance issues)."
    ::sync();

    return 0;
}

#endif

#endif
