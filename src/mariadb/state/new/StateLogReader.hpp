//
// Created by cheesekun on 8/21/22.
//

#ifndef ULTRAVERSE_STATE_STATELOGREADER_HPP
#define ULTRAVERSE_STATE_STATELOGREADER_HPP

#include <fstream>
#include <memory>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    class StateLogReader {
    public:
        StateLogReader(const std::string &logPath, const std::string &logName);
        ~StateLogReader();
        
        void open();
        void close();
        
        bool next();
    
        std::shared_ptr<TransactionHeader> txnHeader();
        std::shared_ptr<Transaction> txnBody();
        
    private:
        std::string _logPath;
        std::string _logName;
        
        std::ifstream _stream;
        
        std::shared_ptr<TransactionHeader> _currentHeader;
        std::shared_ptr<Transaction> _currentBody;
    };
}



#endif //ULTRAVERSE_STATE_STATELOGREADER_HPP
