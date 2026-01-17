//
// Created by cheesekun on 1/20/23.
//

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include <fmt/format.h>

#include "darwincompat.hpp"

#include "GIDIndexReader.hpp"

namespace ultraverse::state::v2 {
    GIDIndexReader::GIDIndexReader(const std::string &logPath, const std::string &logName) {
        auto path = fmt::format("{}/{}.ultindex", logPath, logName);
        _fd = open(path.c_str(), O_RDONLY);
        
        if (_fd < 0) {
            throw std::runtime_error(fmt::format("failed to open {}", path));
        }

        _fsize = lseek64(_fd, 0, SEEK_END);

        _addr = mmap(nullptr, _fsize, PROT_READ, MAP_PRIVATE, _fd, 0);
        if (_addr == MAP_FAILED) {
            throw std::runtime_error(fmt::format("mmap() failed: {} (errno {})", strerror(errno), errno));
        }
    }
    
    GIDIndexReader::~GIDIndexReader() {
        munmap(_addr, _fsize);
        close(_fd);
    }
    
    uint64_t GIDIndexReader::offsetOf(gid_t gid) {
        return reinterpret_cast<uint64_t *>(_addr)[gid];
    }
}