//
// Created by cheesekun on 8/19/22.
//

#include <functional>
#include <utility>

#include <utils/StringUtil.hpp>

#include "Query.hpp"

namespace ultraverse::state::v2 {
    Query::Query():
        _type(UNKNOWN),
        _flags(0),
        _timestamp(0),
        _affectedRows(0)
    {
    
    }
    
    Query::QueryType Query::type() const {
        return _type;
    }
    
    void Query::setType(Query::QueryType type) {
        _type = type;
    }
    
    uint64_t Query::timestamp() const {
        return _timestamp;
    }
    
    void Query::setTimestamp(uint64_t timestamp) {
        _timestamp = timestamp;
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
    
    void Query::setAffectedRows(uint32_t affectedRows) {
        _affectedRows = affectedRows;
    }
    
    StateHash &Query::beforeHash(std::string tableName) {
        return _beforeHash[tableName];
    }
    
    const std::unordered_map<std::string, StateHash> &Query::beforeHash() const {
        return _beforeHash;
    }
    
    void Query::setBeforeHash(std::string tableName, StateHash hash) {
        _beforeHash[tableName] = hash;
    }
    
    StateHash &Query::afterHash(std::string tableName) {
        return _afterHash[tableName];
    }
    
    const std::unordered_map<std::string, StateHash> &Query::afterHash() const {
        return _afterHash;
    }
    
    void Query::setAfterHash(std::string tableName, StateHash hash) {
        _afterHash[tableName] = hash;
    }
    
    bool Query::isAfterHashPresent(std::string tableName) {
        return _afterHash.find(tableName) != _afterHash.end();
    }
    
    uint8_t Query::flags() {
        return _flags;
    }
    
    void Query::setFlags(uint8_t flags) {
        _flags = flags;
    }
    
    std::vector<StateItem> &Query::readSet() {
        return _readSet;
    }
    
    std::vector<StateItem> &Query::writeSet() {
        return _writeSet;
    }
    
    std::vector<StateItem> &Query::varMap() {
        return _varMap;
    }

    
    std::string Query::varMappedStatement(const std::vector<StateItem> &variableSet) const {
        std::string statement = this->statement();
        
        for (const auto &var: variableSet) {
            const auto &name = var.name;
            const std::string &value = var.data_list.front().getAs<std::string>();
            
            statement = std::move(utility::replaceAll(statement, name, value));
        }
        
        for (const auto &var: _varMap) {
            const auto &name = var.name;
            const std::string &value = var.data_list.front().getAs<std::string>();
            
            statement = std::move(utility::replaceAll(statement, name, value));
        }
        
        return statement;
    }
}