//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_MARIADB_DBEVENT_HPP
#define ULTRAVERSE_MARIADB_DBEVENT_HPP

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include "base/DBEvent.hpp"

namespace ultraverse::mariadb {
    using namespace ultraverse;
    
    class TransactionIDEvent: public base::TransactionIDEventBase {
    public:
        TransactionIDEvent(const MARIADB_RPL_EVENT *rplEvent);
    
        uint64_t timestamp() override;
        uint64_t transactionId() override;
        
    private:
        uint64_t _timestamp;
        uint64_t _transactionId;
    };
    
    class QueryEvent: public base::QueryEventBase {
    public:
        QueryEvent(const MARIADB_RPL_EVENT *rplEvent);
    
        uint64_t timestamp() override;
    
        const int64_t error() override;
    
        const std::string &statement() override;
        const std::string &database() override;
    
    private:
        uint64_t _timestamp;
        
        int64_t _error;
        
        std::string _statement;
        std::string _database;
    };
}



#endif //ULTRAVERSE_MARIADB_DBEVENT_HPP
