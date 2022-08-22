//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DBEVENT_HPP
#define ULTRAVERSE_DBEVENT_HPP

#include <cstdint>
#include <cstdio>

#include <string>
#include <unordered_set>

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
        TABLE_MAP = 22,
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
        /**
         * try to parse SQL statement if needed.
         */
        bool parse();
        
        std::vector<int16_t> tokens() const;
        std::vector<size_t> tokenPos() const;
        
        bool isDDL() const;
        bool isDML() const;
    
        std::vector<std::string> &readSet();
        std::vector<std::string> &writeSet();
        
    private:
        void extractReadWriteSet(const hsql::InsertStatement *insert);
        void extractReadWriteSet(const hsql::DeleteStatement *del);
        void extractReadWriteSet(const hsql::UpdateStatement *update);
        void extractReadWriteSet(const hsql::SelectStatement *select);
        
        void walkExpr(const hsql::Expr *expr, std::vector<std::string> &readSet, const std::string &rootTable);
        
        std::vector<int16_t> _tokens;
        std::vector<size_t> _tokenPos;
    
        std::unordered_set<std::string> _readSet;
        std::unordered_set<std::string> _writeSet;
        
        hsql::SQLParserResult _parseResult;
        
        
    };
}

#endif //ULTRAVERSE_DBEVENT_HPP
