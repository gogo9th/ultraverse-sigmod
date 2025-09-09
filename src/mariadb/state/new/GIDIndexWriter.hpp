//
// Created by cheesekun on 1/20/23.
//

#ifndef ULTRAVERSE_GIDINDEXWRITER_HPP
#define ULTRAVERSE_GIDINDEXWRITER_HPP

#include <string>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    /**
     * @brief state log에서 특정 GID로 빠르게 seek할 수 있도록, 각 트랜잭션의 로그 오프셋을 인덱싱한 파일을 생성해주는 클래스
     */
    class GIDIndexWriter {
    public:
        GIDIndexWriter(const std::string &logPath, const std::string &logName);
        GIDIndexWriter(GIDIndexWriter &) = delete;
        
        ~GIDIndexWriter();
        
        /**
         * 특정 gid를 가진 트랜잭션의 로그 오프셋을 기록한다.
         */
        void write(gid_t gid, uint64_t offset);
        /**
         * 마지막 오프셋 뒤에 다음 트랜잭션의 로그 오프셋을 기록한다.
         */
        void append(uint64_t offset);
    private:
        bool needsResize(gid_t gid);
        
        int _fd;
        size_t _fsize;
    };
}


#endif //ULTRAVERSE_GIDINDEXWRITER_HPP
