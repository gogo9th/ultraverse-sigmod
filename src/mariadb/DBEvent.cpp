//
// Created by cheesekun on 8/10/22.
//

#include "DBEvent.hpp"

namespace ultraverse::mariadb {
    TransactionIDEvent::TransactionIDEvent(const MARIADB_RPL_EVENT *rplEvent):
        _timestamp(rplEvent->timestamp),
        _transactionId(rplEvent->event.xid.transaction_nr)
    {
    }
    
    uint64_t TransactionIDEvent::timestamp() {
        return _timestamp;
    }

    uint64_t TransactionIDEvent::transactionId() {
        return _transactionId;
    }
    
    QueryEvent::QueryEvent(const MARIADB_RPL_EVENT *rplEvent):
        _timestamp(rplEvent->timestamp),
        
        _error((int64_t) rplEvent->event.query.errornr),
        
        _statement(std::string(rplEvent->event.query.statement.str, rplEvent->event.query.statement.length - 2)),
        _database(std::string(rplEvent->event.query.database.str, rplEvent->event.query.database.length))
    {
    
    }
    
    uint64_t QueryEvent::timestamp() {
        return _timestamp;
    }
    
    const int64_t QueryEvent::error() {
        return _error;
    }
    
    const std::string &QueryEvent::statement() {
        return _statement;
    }
    
    const std::string &QueryEvent::database() {
        return _database;
    }
    
}