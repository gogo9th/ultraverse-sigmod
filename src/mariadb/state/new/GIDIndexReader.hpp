//
// Created by cheesekun on 1/20/23.
//

#ifndef ULTRAVERSE_GIDINDEXREADER_HPP
#define ULTRAVERSE_GIDINDEXREADER_HPP

#include <string>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    /**
     * @brief state log에서 특정 GID로 빠르게 seek할 수 있도록, GID를 인덱싱한 파일을 읽어주는 클래스
     */
    class GIDIndexReader {
    public:
        GIDIndexReader(const std::string &logPath, const std::string &logName);
        GIDIndexReader(GIDIndexReader &) = delete;
        
        ~GIDIndexReader();
        
        /**
         * 주어진 GID를 가진 트랜잭션의 로그 오프셋을 반환한다.
         */
        uint64_t offsetOf(gid_t gid);
    private:
        int _fd;
        size_t _fsize;
        
        void *_addr;
    };
}



#endif //ULTRAVERSE_GIDINDEXREADER_HPP
