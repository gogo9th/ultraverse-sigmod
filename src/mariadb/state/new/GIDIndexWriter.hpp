//
// Created by cheesekun on 1/20/23.
//

#ifndef ULTRAVERSE_GIDINDEXWRITER_HPP
#define ULTRAVERSE_GIDINDEXWRITER_HPP

#include <string>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    class GIDIndexWriter {
    public:
        GIDIndexWriter(const std::string &logPath, const std::string &logName);
        GIDIndexWriter(GIDIndexWriter &) = delete;
        
        ~GIDIndexWriter();
        
        void write(gid_t gid, uint64_t offset);
        void append(uint64_t offset);
    private:
        bool needsResize(gid_t gid);
        
        int _fd;
        size_t _fsize;
    };
}


#endif //ULTRAVERSE_GIDINDEXWRITER_HPP
