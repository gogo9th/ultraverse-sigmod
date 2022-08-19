//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DBEVENT_HPP
#define ULTRAVERSE_DBEVENT_HPP

#include <cstdint>
#include <cstdio>

#include <string>

#include "SQLParser.h"
#include "mariadb/state/state_log_hdr.h"

namespace ultraverse::event_type {
    enum Value {
        UNKNOWN = 0,
        LOG_ROTATION = 1,
        
        TXNID = 10,
        QUERY = 11,
        
        ROW_EVENT = 20,
        ROW_QUERY = 21,
    };
}

namespace ultraverse::base {
    class DBEvent {
    public:
        virtual event_type::Value eventType() = 0;
        virtual uint64_t timestamp() = 0;
        
        const char *rawObject() {
            return nullptr;
        };
        
        size_t rawObjectSize() {
            return 0;
        };
    };
    
    class TransactionIDEventBase: public DBEvent {
    public:
        event_type::Value eventType() override {
            return event_type::TXNID;
        }
        
        virtual uint64_t transactionId() = 0;
    };
    
    class QueryEventBase: public DBEvent {
    public:
        event_type::Value eventType() override {
            return event_type::QUERY;
        }
        
        virtual const int64_t error() = 0;
        
        virtual const std::string &statement() = 0;
        virtual const std::string &database() = 0;
        
        /**
         * try to tokenize SQL statement.
         * @return returns false if fails.
         */
        bool tokenize();
        
        std::vector<int16_t> tokens() const;
        std::vector<size_t> tokenPos() const;
        
        bool isDDL() const;
        bool isDML() const;
        
    private:
        std::vector<int16_t> _tokens;
        std::vector<size_t> _tokenPos;
    };
}

#endif //ULTRAVERSE_DBEVENT_HPP
