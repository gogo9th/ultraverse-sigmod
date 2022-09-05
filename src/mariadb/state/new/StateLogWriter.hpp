//
// Created by cheesekun on 8/21/22.
//

#ifndef ULTRAVERSE_STATE_STATELOGWRITER_HPP
#define ULTRAVERSE_STATE_STATELOGWRITER_HPP

#include <fstream>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    class StateLogWriter {
    public:
        StateLogWriter(const std::string &logPath);
        ~StateLogWriter();
        
        void open(std::ios_base::openmode openMode);
        void close();
        bool seek(int64_t position);
        int64_t pos();
        
        StateLogWriter &operator<<(Transaction &transaction);
        
        void writeCheckpoint();
    private:
        std::string _logPath;
        std::ofstream _stream;
    };
}


#endif //ULTRAVERSE_STATE_STATELOGWRITER_HPP
