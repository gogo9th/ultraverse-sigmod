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
        TransactionIDEvent(uint64_t xid, uint64_t timestamp);
    
        uint64_t timestamp() override;
        uint64_t transactionId() override;
        
    private:
        uint64_t _timestamp;
        uint64_t _transactionId;
    };
    
    class QueryEvent: public base::QueryEventBase {
    public:
        QueryEvent(const MARIADB_RPL_EVENT *rplEvent);
        QueryEvent(
            const std::string &schema,
            const std::string &statement,
            uint64_t timestamp
        );
    
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
    
    
    class RowEvent: public base::DBEvent {
    public:
        enum Type {
            INSERT,
            UPDATE,
            DELETE
        };
        
        event_type::Value eventType() override {
            return event_type::ROW_EVENT;
        }
        
        
        
    private:
        std::vector<std::vector<std::string>> rowSet;
        std::vector<std::vector<std::string>> changeSet;
        
    };
    
    class RowQueryEvent: public base::DBEvent {
    public:
        RowQueryEvent(
            const std::string &statement,
            uint64_t timestamp
        );
        
        event_type::Value eventType() override {
            return event_type::ROW_QUERY;
        }
        
        uint64_t timestamp() override;
        
        std::string statement();
        
    private:
        std::string _statement;
        uint64_t _timestamp;
    };
    
    

}



#endif //ULTRAVERSE_MARIADB_DBEVENT_HPP
