//
// Created by cheesekun on 8/11/22.
//

#include "DBEvent.hpp"

#include "SQLParser.h"
#include "bison_parser.h"
#include "mariadb/BinaryLog.hpp"

namespace ultraverse::base {
    bool QueryEventBase::tokenize() {
        return hsql::SQLParser::tokenize(statement(), &_tokens, &_tokenPos)
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