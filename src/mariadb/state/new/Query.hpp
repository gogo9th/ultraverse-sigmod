//
// Created by cheesekun on 8/19/22.
//

#ifndef ULTRAVERSE_STATE_QUERY_HPP
#define ULTRAVERSE_STATE_QUERY_HPP

#include <memory>
#include <unordered_map>

#include <cereal/access.hpp>

#include "mariadb/state/StateHash.hpp"

namespace ultraverse::state::v2 {
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
            UPDATE,
            DELETE
        };
    
        static const uint8_t FLAG_IS_IGNORABLE      = 0b00000001;
        
        static const uint8_t FLAG_IS_DDL            = 0b00000010;
        static const uint8_t FLAG_DEFINES_DATABASE  = 0b10000010;
        static const uint8_t FLAG_DEFINES_TABLE     = 0b01000010;
        static const uint8_t FLAG_DEFINES_INDEX     = 0b00100010;
        static const uint8_t FLAG_DEFINES_TRIGGER   = 0b00010010;
        static const uint8_t FLAG_DEFINES_VIEW      = 0b00001010;
    
        Query();
        
        std::string database() const;
        void setDatabase(std::string database);
        
        std::string statement() const;
        void setStatement(std::string statement);
        
        uint32_t affectedRows() const;
        
        void setBeforeHash(std::string tableName, StateHash &hash);
        void setAfterHash(std::string tableName, StateHash &hash);
        
        uint8_t flags();
        void setFlags(uint8_t flags);
        
        template <typename Archive>
        void serialize(Archive &archive);
        
    private:
        uint64_t _timestamp;
        
        std::string _database;
        std::string _statement;
        
        uint8_t _flags;
    
        // binlog reference
        std::string _referenceFile;
        uint64_t _referencePos;
        
        std::unordered_map<std::string, StateHash> _beforeHash;
        std::unordered_map<std::string, StateHash> _afterHash;
    
        std::vector<std::string> _affectedTables;
        std::vector<std::string> _readSet;
        std::vector<std::string> _writeSet;
        std::vector<std::string> _foreignKeySet;
        
        uint32_t _affectedRows;
        std::vector<std::string> _rowSet;
        std::vector<std::string> _changeSet;
    };
}


#include "Query.cereal.cpp"

#endif //ULTRAVERSE_STATE_QUERY_HPP
