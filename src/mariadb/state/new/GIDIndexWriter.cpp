//
// Created by cheesekun on 1/20/23.
//

#include <fcntl.h>
#include <unistd.h>

#include <fmt/format.h>

#include "darwincompat.hpp"

#include "GIDIndexWriter.hpp"

namespace ultraverse::state::v2 {
    GIDIndexWriter::GIDIndexWriter(const std::string &logPath, const std::string &logName) {
        auto path = fmt::format("{}/{}.ultindex", logPath, logName);
        _fd = open(path.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
        _fsize = 0;
        
        if (_fd < 0) {
            throw std::runtime_error(fmt::format("failed to open {}", path));
        }
    }
    
    GIDIndexWriter::~GIDIndexWriter() {
        syncfs(_fd);
        close(_fd);
    }
    
    void GIDIndexWriter::write(gid_t gid, uint64_t offset) {
        if (needsResize(gid)) {
            ftruncate64(_fd, gid * sizeof(uint64_t));
            _fsize = gid * sizeof(uint64_t);
        }
        
        lseek64(_fd, gid * sizeof(uint64_t), SEEK_SET);
        ::write(_fd, &offset, sizeof(uint64_t));
    }
    
    void GIDIndexWriter::append(uint64_t offset) {
        ::write(_fd, &offset, sizeof(uint64_t));
    }
    
    bool GIDIndexWriter::needsResize(gid_t gid) {
        return _fsize < (gid * sizeof(uint64_t));
    }
}