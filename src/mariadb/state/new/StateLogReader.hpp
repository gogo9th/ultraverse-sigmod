//
// Created by cheesekun on 8/21/22.
//

#ifndef ULTRAVERSE_STATE_STATELOGREADER_HPP
#define ULTRAVERSE_STATE_STATELOGREADER_HPP

#include <memory>

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    class StateLogReader {
    public:
        void next();
    
        std::unique_ptr<TransactionHeader> txnHeader();
        std::unique_ptr<Transaction> txnBody();
    };
}



#endif //ULTRAVERSE_STATE_STATELOGREADER_HPP
