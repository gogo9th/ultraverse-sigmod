//
// Created by cheesekun on 8/21/22.
//

#ifndef ULTRAVERSE_STATE_STATELOGWRITER_HPP
#define ULTRAVERSE_STATE_STATELOGWRITER_HPP

#include <fstream>
#include <mutex>

#include "Transaction.hpp"
#include "ColumnDependencyGraph.hpp"

namespace ultraverse::state::v2 {
    class StateLogWriter {
    public:
        StateLogWriter(const std::string &logPath, const std::string &logName);
        ~StateLogWriter();
        
        void open(std::ios_base::openmode openMode);
        void close();
        bool seek(int64_t position);
        int64_t pos();
        
        StateLogWriter &operator<<(Transaction &transaction);
        StateLogWriter &operator<<(ColumnDependencyGraph &graph);
    
        void writeColumnDependencyGraph(ColumnDependencyGraph &graph);
        void writeCheckpoint();
    private:
        std::string _logPath;
        std::string _logName;
        
        std::ofstream _stream;
        std::mutex _mutex;
    };
}


#endif //ULTRAVERSE_STATE_STATELOGWRITER_HPP
