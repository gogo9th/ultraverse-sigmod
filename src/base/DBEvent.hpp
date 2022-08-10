//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DBEVENT_HPP
#define ULTRAVERSE_DBEVENT_HPP

#include <cstdint>
#include <cstdio>

#include <string>

namespace ultraverse::event_type {
    enum Value {
        UNKNOWN = 0,
        LOG_ROTATION = 1,
        
        TXNID = 10,
        QUERY = 11,
    };
}

namespace ultraverse::base {
    class DBEventBase {
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
    
    class TransactionIDEventBase: public DBEventBase {
    public:
        event_type::Value eventType() override {
            return event_type::TXNID;
        }
        
        virtual uint64_t transactionId() = 0;
    };
    
    class QueryEventBase: public DBEventBase {
    public:
        event_type::Value eventType() override {
            return event_type::QUERY;
        }
        
        virtual const int64_t error() = 0;
        
        virtual const std::string &statement() = 0;
        virtual const std::string &database() = 0;
    };
}

#endif //ULTRAVERSE_DBEVENT_HPP
