//
// Created by cheesekun on 8/19/22.
//

#ifndef ULTRAVERSE_STATE_QUERY_HPP
#define ULTRAVERSE_STATE_QUERY_HPP

#include <memory>
#include <unordered_set>
#include <unordered_map>

#include <cereal/access.hpp>

#include "mariadb/state/StateHash.hpp"
#include "mariadb/state/StateItem.h"

namespace ultraverse::state::v2 {
    using ColumnSet = std::set<std::string>;
    
    class Query {
    public:
        enum QueryType: uint8_t {
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
        std::vector<StateItem> &varMap();
        
        std::string varMappedStatement(const std::vector<StateItem> &variableSet) const;
        
        template <typename Archive>
        void serialize(Archive &archive);
        
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
        
        uint32_t _affectedRows;
    };
}


#include "Query.cereal.cpp"
#include "ColumnSet.template.cpp"

#endif //ULTRAVERSE_STATE_QUERY_HPP
