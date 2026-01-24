//
// Created by cheesekun on 8/19/22.
//

#include <functional>
#include <utility>

#include <utils/StringUtil.hpp>

#include "ultraverse_state.pb.h"

#include "Query.hpp"

namespace ultraverse::state::v2 {
    Query::Query():
        _type(UNKNOWN),
        _flags(0),
        _timestamp(0),
        _affectedRows(0),

        _statementContext({ 0 })
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

    ColumnSet &Query::readColumns() {
        return _readColumns;
    }

    const ColumnSet &Query::readColumns() const {
        return _readColumns;
    }

    ColumnSet &Query::writeColumns() {
        return _writeColumns;
    }

    const ColumnSet &Query::writeColumns() const {
        return _writeColumns;
    }
    
    std::vector<StateItem> &Query::varMap() {
        return _varMap;
    }

    Query::StatementContext &Query::statementContext() {
        return _statementContext;
    }

    const Query::StatementContext &Query::statementContext() const {
        return _statementContext;
    }

    void Query::setStatementContext(const StatementContext &context) {
        _statementContext = context;
    }

    void Query::clearStatementContext() {
        _statementContext.clear();
    }

    bool Query::hasStatementContext() const {
        return !_statementContext.empty();
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

    void Query::UserVar::toProtobuf(ultraverse::state::v2::proto::QueryUserVar *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        out->set_name(name);
        out->set_type(static_cast<uint32_t>(type));
        out->set_is_null(isNull);
        out->set_is_unsigned(isUnsigned);
        out->set_charset(charset);
        out->set_value(value);
    }

    void Query::UserVar::fromProtobuf(const ultraverse::state::v2::proto::QueryUserVar &msg) {
        name = msg.name();
        type = static_cast<ValueType>(msg.type());
        isNull = msg.is_null();
        isUnsigned = msg.is_unsigned();
        charset = msg.charset();
        value = msg.value();
    }

    void Query::StatementContext::toProtobuf(
        ultraverse::state::v2::proto::QueryStatementContext *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        out->set_has_last_insert_id(hasLastInsertId);
        out->set_last_insert_id(lastInsertId);
        out->set_has_insert_id(hasInsertId);
        out->set_insert_id(insertId);
        out->set_has_rand_seed(hasRandSeed);
        out->set_rand_seed1(randSeed1);
        out->set_rand_seed2(randSeed2);

        for (const auto &var : userVars) {
            auto *varMsg = out->add_user_vars();
            var.toProtobuf(varMsg);
        }
    }

    void Query::StatementContext::fromProtobuf(
        const ultraverse::state::v2::proto::QueryStatementContext &msg) {
        hasLastInsertId = msg.has_last_insert_id();
        lastInsertId = msg.last_insert_id();
        hasInsertId = msg.has_insert_id();
        insertId = msg.insert_id();
        hasRandSeed = msg.has_rand_seed();
        randSeed1 = msg.rand_seed1();
        randSeed2 = msg.rand_seed2();

        userVars.clear();
        userVars.reserve(static_cast<size_t>(msg.user_vars_size()));
        for (const auto &varMsg : msg.user_vars()) {
            UserVar var;
            var.fromProtobuf(varMsg);
            userVars.emplace_back(std::move(var));
        }
    }

    void Query::toProtobuf(ultraverse::state::v2::proto::Query *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        out->set_type(static_cast<uint32_t>(_type));
        out->set_timestamp(_timestamp);
        out->set_database(_database);
        out->set_statement(_statement);
        out->set_flags(_flags);
        out->set_affected_rows(_affectedRows);

        auto *beforeMap = out->mutable_before_hash();
        beforeMap->clear();
        for (const auto &entry : _beforeHash) {
            auto &hashMsg = (*beforeMap)[entry.first];
            entry.second.toProtobuf(&hashMsg);
        }

        auto *afterMap = out->mutable_after_hash();
        afterMap->clear();
        for (const auto &entry : _afterHash) {
            auto &hashMsg = (*afterMap)[entry.first];
            entry.second.toProtobuf(&hashMsg);
        }

        for (const auto &item : _readSet) {
            auto *itemMsg = out->add_read_set();
            item.toProtobuf(itemMsg);
        }

        for (const auto &item : _writeSet) {
            auto *itemMsg = out->add_write_set();
            item.toProtobuf(itemMsg);
        }

        for (const auto &item : _varMap) {
            auto *itemMsg = out->add_var_map();
            item.toProtobuf(itemMsg);
        }

        for (const auto &column : _readColumns) {
            out->add_read_columns(column);
        }

        for (const auto &column : _writeColumns) {
            out->add_write_columns(column);
        }

        _statementContext.toProtobuf(out->mutable_statement_context());
    }

    void Query::fromProtobuf(const ultraverse::state::v2::proto::Query &msg) {
        _type = static_cast<QueryType>(msg.type());
        _timestamp = msg.timestamp();
        _database = msg.database();
        _statement = msg.statement();
        _flags = static_cast<uint8_t>(msg.flags());
        _affectedRows = msg.affected_rows();

        _beforeHash.clear();
        for (const auto &entry : msg.before_hash()) {
            StateHash hash;
            hash.fromProtobuf(entry.second);
            _beforeHash.emplace(entry.first, std::move(hash));
        }

        _afterHash.clear();
        for (const auto &entry : msg.after_hash()) {
            StateHash hash;
            hash.fromProtobuf(entry.second);
            _afterHash.emplace(entry.first, std::move(hash));
        }

        _readSet.clear();
        _readSet.reserve(static_cast<size_t>(msg.read_set_size()));
        for (const auto &itemMsg : msg.read_set()) {
            StateItem item;
            item.fromProtobuf(itemMsg);
            _readSet.emplace_back(std::move(item));
        }

        _writeSet.clear();
        _writeSet.reserve(static_cast<size_t>(msg.write_set_size()));
        for (const auto &itemMsg : msg.write_set()) {
            StateItem item;
            item.fromProtobuf(itemMsg);
            _writeSet.emplace_back(std::move(item));
        }

        _varMap.clear();
        _varMap.reserve(static_cast<size_t>(msg.var_map_size()));
        for (const auto &itemMsg : msg.var_map()) {
            StateItem item;
            item.fromProtobuf(itemMsg);
            _varMap.emplace_back(std::move(item));
        }

        _readColumns.clear();
        for (const auto &column : msg.read_columns()) {
            _readColumns.insert(column);
        }

        _writeColumns.clear();
        for (const auto &column : msg.write_columns()) {
            _writeColumns.insert(column);
        }

        _statementContext.fromProtobuf(msg.statement_context());
    }
}
