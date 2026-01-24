//
// Created by cheesekun on 3/16/23.
//

#ifndef ULTRAVERSE_PROCLOGREADER_HPP
#define ULTRAVERSE_PROCLOGREADER_HPP

#include <string>
#include <fstream>

#include "ProcCall.hpp"

namespace ultraverse::state::v2 {
    
    class ProcLogReader {
    public:
        ProcLogReader();
        
        bool open(const std::string &path, const std::string &logName);
        bool close();
        
        void seek(uint64_t pos);
    
        bool nextHeader();
        bool nextProcCall();
        
        bool matchForward(uint64_t callId);
        
        std::shared_ptr<ProcCallHeader> currentHeader();
        std::shared_ptr<ProcCall> current();
        
    private:
        std::ifstream _stream;
    
        std::shared_ptr<ProcCallHeader> _currentHeader;
        std::shared_ptr<ProcCall> _current;
    };
}


#endif //ULTRAVERSE_PROCLOGREADER_HPP
