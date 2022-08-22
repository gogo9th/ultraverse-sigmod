//
// Created by cheesekun on 8/11/22.
//

#include <algorithm>

#include "DBEvent.hpp"

#include "SQLParser.h"
#include "bison_parser.h"
#include "mariadb/BinaryLog.hpp"
#include "util/sqlhelper.h"

namespace ultraverse::base {
    bool QueryEventBase::tokenize() {
        return hsql::SQLParser::tokenize(statement(), &_tokens, &_tokenPos);
    }
    
    bool QueryEventBase::parse() {
        auto result = hsql::SQLParser::parse(statement(), &_parseResult);
    
        if (!(_parseResult.isValid() && _parseResult.size() > 0)) {
            return false;
        }
      
        const hsql::SQLStatement* statement = _parseResult.getStatement(0);
        hsql::printStatementInfo(statement);
    
        if (statement->isType(hsql::kStmtInsert)) {
            extractReadWriteSet(static_cast<const hsql::InsertStatement *>(statement));
        } else if (statement->isType(hsql::kStmtDelete)) {
            extractReadWriteSet(static_cast<const hsql::DeleteStatement *>(statement));
        } else if (statement->isType(hsql::kStmtUpdate)) {
            extractReadWriteSet(static_cast<const hsql::UpdateStatement *>(statement));
        }
        
        return result;
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::InsertStatement *insert) {
        std::string tableName(insert->tableName);
        
        if (insert->columns) {
            for (auto column: *insert->columns) {
                _writeSet.insert(tableName + "." + std::string(column));
            }
        }
        
        if (insert->type == hsql::InsertType::kInsertSelect) {
            extractReadWriteSet(insert->select);
        }
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::DeleteStatement *del) {
        std::string tableName(del->tableName);
        std::vector<std::string> readSet;
        walkExpr(del->expr, readSet, tableName);
        
        _readSet.insert(readSet.begin(), readSet.end());
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::UpdateStatement *update) {
        std::string tableName(update->table->name);
        
        std::vector<std::string> readSet;
        walkExpr(update->where, readSet, tableName);
    
        _readSet.insert(readSet.begin(), readSet.end());
        
        for (auto &clause: *update->updates) {
            _writeSet.insert(tableName + "." + clause->column);
        }
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::SelectStatement *select) {
        std::string tableName(select->fromTable->getName());
        
        std::vector<std::string> readSet;
        if (select->selectList != nullptr) {
            for (auto &expr: *select->selectList) {
                walkExpr(expr, readSet, tableName);
            }
        }
        walkExpr(select->whereClause, readSet, tableName);
    }
    
    void QueryEventBase::walkExpr(const hsql::Expr *expr, std::vector<std::string> &readSet, const std::string &rootTable) {
        if (expr == nullptr) {
            return;
        }
        
        if (expr->isType(hsql::kExprOperator)) {
            walkExpr(expr->expr, readSet, rootTable);
            walkExpr(expr->expr2, readSet, rootTable);
            return;
        }
        
        if (expr->isType(hsql::kExprColumnRef)) {
            auto column = expr->table == nullptr ?
                rootTable + "." + std::string(expr->name) :
                std::string(expr->table) + "." + std::string(expr->name);
            
            readSet.push_back(column);
            return;
        }
    }
    
    
    std::vector<int16_t> QueryEventBase::tokens() const {
        return _tokens;
    }
    
    std::vector<size_t> QueryEventBase::tokenPos() const {
        return _tokenPos;
    }
    
    bool QueryEventBase::isDDL() const {
        if (_tokens.empty()) {
            return false;
        }
        return (
            _tokens[0] == SQL_CREATE  ||
            _tokens[0] == SQL_ALTER   ||
            _tokens[0] == SQL_DROP    ||
            _tokens[0] == SQL_RENAME  ||
            _tokens[0] == SQL_COMMENT ||
            _tokens[0] == SQL_TRUNCATE
        );
    }
    
    bool QueryEventBase::isDML() const {
        if (_tokens.empty()) {
            return false;
        }
        return (
            _tokens[0] == SQL_SELECT  ||
            _tokens[0] == SQL_INSERT  ||
            _tokens[0] == SQL_UPDATE  ||
            _tokens[0] == SQL_DELETE  ||
            _tokens[0] == SQL_MERGE   ||
            _tokens[0] == SQL_CALL    ||
            _tokens[0] == SQL_EXPLAIN ||
            _tokens[0] == SQL_LOCK
        );
    }
    
    
    
}