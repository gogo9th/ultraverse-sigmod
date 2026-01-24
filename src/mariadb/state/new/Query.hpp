//
// Created by cheesekun on 8/19/22.
//

#ifndef ULTRAVERSE_STATE_QUERY_HPP
#define ULTRAVERSE_STATE_QUERY_HPP

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "mariadb/state/new/proto/ultraverse_state_fwd.hpp"

#include "mariadb/state/StateHash.hpp"
#include "mariadb/state/StateItem.h"

namespace ultraverse::state::v2 {
    using ColumnSet = std::set<std::string>;
    
    class Query {
    public:
        enum QueryType: int32_t {
            UNKNOWN,
            
            CREATE,
            DROP,
            ALTER,
            TRUNCATE,
            RENAME,
            
            SELECT,
            INSERT,
            UPDATE,
            DELETE
        };
    
        static const uint8_t FLAG_IS_IGNORABLE      = 0b00000001;
        static const uint8_t FLAG_IS_DDL            = 0b00000010;

        static const uint8_t FLAG_IS_PROCCALL_RECOVERED_QUERY = 0b00001000;
        static const uint8_t FLAG_IS_PROCCALL_QUERY           = 0b00010000;

        static const uint8_t FLAG_IS_CONTINUOUS = 0b10000000;

        struct UserVar {
            enum ValueType : uint8_t {
                STRING = 0,
                REAL = 1,
                INT = 2,
                DECIMAL = 3
            };

            std::string name;
            ValueType type = STRING;
            bool isNull = false;
            bool isUnsigned = false;
            uint32_t charset = 0;
            std::string value;

            void toProtobuf(ultraverse::state::v2::proto::QueryUserVar *out) const;
            void fromProtobuf(const ultraverse::state::v2::proto::QueryUserVar &msg);
        };

        struct StatementContext {
            bool hasLastInsertId = false;
            uint64_t lastInsertId = 0;
            bool hasInsertId = false;
            uint64_t insertId = 0;
            bool hasRandSeed = false;
            uint64_t randSeed1 = 0;
            uint64_t randSeed2 = 0;
            std::vector<UserVar> userVars { 0 };

            bool empty() const {
                return !hasLastInsertId && !hasInsertId && !hasRandSeed && userVars.empty();
            }

            void clear() {
                *this = StatementContext();
            }

            void toProtobuf(ultraverse::state::v2::proto::QueryStatementContext *out) const;
            void fromProtobuf(const ultraverse::state::v2::proto::QueryStatementContext &msg);
        };

        Query();
        
        QueryType type() const;
        void setType(QueryType type);
        
        uint64_t timestamp() const;
        void setTimestamp(uint64_t timestamp);
        
        std::string database() const;
        void setDatabase(std::string database);
        
        std::string statement() const;
        void setStatement(std::string statement);
        
        uint32_t affectedRows() const;
        void setAffectedRows(uint32_t affectedRows);
        
        StateHash &beforeHash(std::string tableName);
        const std::unordered_map<std::string, StateHash> &beforeHash() const;
        void setBeforeHash(std::string tableName, StateHash hash);
        
        StateHash &afterHash(std::string tableName);
        const std::unordered_map<std::string, StateHash> &afterHash() const;
        void setAfterHash(std::string tableName, StateHash hash);
        bool isAfterHashPresent(std::string tableName);
        
        uint8_t flags();
        void setFlags(uint8_t flags);
        
        /**
         * @brief Returns the set of 'row items' that were affected by the query.
         */
        std::vector<StateItem> &readSet();
        /**
         * @breif Returns the set of 'row items' before the update was applied.
         */
        std::vector<StateItem> &writeSet();

        ColumnSet &readColumns();
        const ColumnSet &readColumns() const;

        ColumnSet &writeColumns();
        const ColumnSet &writeColumns() const;
        std::vector<StateItem> &varMap();

        StatementContext &statementContext();
        const StatementContext &statementContext() const;
        void setStatementContext(const StatementContext &context);
        void clearStatementContext();
        bool hasStatementContext() const;

        std::string varMappedStatement(const std::vector<StateItem> &variableSet) const;

        void toProtobuf(ultraverse::state::v2::proto::Query *out) const;
        void fromProtobuf(const ultraverse::state::v2::proto::Query &msg);
    private:
        QueryType _type;
        uint64_t _timestamp;
        
        std::string _database;
        std::string _statement;
        
        uint8_t _flags;
    
        std::unordered_map<std::string, StateHash> _beforeHash;
        std::unordered_map<std::string, StateHash> _afterHash;
    
        std::vector<StateItem> _readSet;
        std::vector<StateItem> _writeSet;
        std::vector<StateItem> _varMap;

        ColumnSet _readColumns;
        ColumnSet _writeColumns;

        uint32_t _affectedRows;

        StatementContext _statementContext;
    };
}


#include "ColumnSet.template.cpp"

#endif //ULTRAVERSE_STATE_QUERY_HPP
