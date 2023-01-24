//
// Created by cheesekun on 1/20/23.
//

#ifndef ULTRAVERSE_GIDINDEXREADER_HPP
#define ULTRAVERSE_GIDINDEXREADER_HPP

#include <string>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    class GIDIndexReader {
    public:
        GIDIndexReader(const std::string &logPath, const std::string &logName);
        GIDIndexReader(GIDIndexReader &) = delete;
        
        ~GIDIndexReader();
        
        uint64_t offsetOf(gid_t gid);
    private:
        int _fd;
        size_t _fsize;
        
        void *_addr;
    };
}



#endif //ULTRAVERSE_GIDINDEXREADER_HPP
