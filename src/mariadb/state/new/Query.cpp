//
// Created by cheesekun on 8/19/22.
//

#include "Query.hpp"

#include <utility>

namespace ultraverse::state::v2 {
    Query::Query():
        _timestamp(0),
        _affectedRows(0),
        _referencePos(0)
    {
    
    }
    
    std::string Query::database() const {
        return _database;
    }
    
    void Query::setDatabase(std::string database) {
        _database = std::move(database);
    }
    
    std::string Query::statement() const {
        return _statement;
    }
    
    void Query::setStatement(std::string statement) {
        _statement = std::move(statement);
    }
    
    uint32_t Query::affectedRows() const {
        return _affectedRows;
    }
    
    void Query::setBeforeHash(std::string tableName, StateHash &hash) {
        _beforeHash[tableName] = hash;
    }
    
    void Query::setAfterHash(std::string tableName, StateHash &hash) {
        _afterHash[tableName] = hash;
    }
    
    uint8_t Query::flags() {
        return _flags;
    }
    
    void Query::setFlags(uint8_t flags) {
        _flags = flags;
    }
}