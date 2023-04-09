//
// Created by cheesekun on 8/11/22.
//

#include <algorithm>

#include "DBEvent.hpp"

#include "SQLParser.h"
#include "bison_parser.h"
#include "mariadb/BinaryLog.hpp"
#include "util/sqlhelper.h"

#include "utils/StringUtil.hpp"

namespace ultraverse::base {
    
    QueryEventBase::QueryEventBase():
        _logger(createLogger("QueryEventBase"))
    {
    
    }
    
    bool QueryEventBase::tokenize() {
        return hsql::SQLParser::tokenize(statement(), &_tokens, &_tokenPos);
    }
    
    bool QueryEventBase::parse() {
        auto result = hsql::SQLParser::parse(statement(), &_parseResult);
    
        if (!(_parseResult.isValid() && _parseResult.size() > 0)) {
            _logger->warn("could not parse SQL statement: {} at line {}, col {}", _parseResult.errorMsg(), _parseResult.errorLine(), _parseResult.errorColumn());
            return false;
        }
      
        const hsql::SQLStatement* statement = _parseResult.getStatement(0);
        // hsql::printStatementInfo(statement);
    
        if (statement->isType(hsql::kStmtInsert)) {
            extractReadWriteSet(static_cast<const hsql::InsertStatement *>(statement));
        } else if (statement->isType(hsql::kStmtDelete)) {
            extractReadWriteSet(static_cast<const hsql::DeleteStatement *>(statement));
        } else if (statement->isType(hsql::kStmtUpdate)) {
            extractReadWriteSet(static_cast<const hsql::UpdateStatement *>(statement));
        }
        
        return result;
    }

    bool QueryEventBase::parseSelect() {
        static const auto tolower = [](unsigned char c) { return std::tolower(c); };

        constexpr int PHASE_COLUMNS = 1;
        constexpr int PHASE_TABLE_NAME = 2;
        constexpr int PHASE_WHERE_COL = 3;
        constexpr int PHASE_WHERE_VAL = 4;

        int phase = PHASE_COLUMNS;
        int depth = 0;

        bool isNameConst = false;
        bool isNameConstVal = false;
        bool skip = false;

        std::vector<int16_t> tokens;
        std::vector<size_t> tokenPos;
        hsql::SQLParser::tokenize(statement(), &tokens, &tokenPos);

        std::string tableName;
        std::string whereCol;
        std::set<std::string> readSet;
        std::unordered_map<std::string, int64_t> whereSet;

        int i = 0;
        for (auto &token: tokens) {
            if (token == SQL_SELECT) {
                phase = PHASE_COLUMNS;
                skip = false;
                goto NEXT_TOKEN;
            } else if (token == SQL_FROM) {
                phase = PHASE_TABLE_NAME;
                skip = false;
                goto NEXT_TOKEN;
            } else if (token == SQL_WHERE) {
                phase = PHASE_WHERE_COL;
                skip = false;
                goto NEXT_TOKEN;
            }

            if (token == SQL_NAME_CONST) {
                isNameConst = true;
                goto NEXT_TOKEN;
            }

            if (token == '(') {
                depth++;
                goto NEXT_TOKEN;
            }

            if (token == ')') {
                depth--;
                goto NEXT_TOKEN;
            }

            if (token == ',' || token == SQL_AND || token == SQL_OR) {
                if (depth == 0) {
                    skip = false;
                    isNameConst = false;
                    isNameConstVal = false;
                }
                goto NEXT_TOKEN;
            } else if (skip) {
                goto NEXT_TOKEN;
            }

            {
                std::string value;
                if (i + 1 == tokens.size()) {
                    value = statement().substr(tokenPos[i]);
                } else {
                    tokenPos[i + 1] - tokenPos[i];
                    value = statement().substr(tokenPos[i], tokenPos[i + 1] - tokenPos[i]);
                }


                if (phase == PHASE_COLUMNS) {
                    std::transform(value.begin(), value.end(), value.begin(), tolower);
                    readSet.insert(utility::normalizeColumnName(value));
                    skip = true;
                } else if (phase == PHASE_WHERE_COL) {
                    std::transform(value.begin(), value.end(), value.begin(), tolower);
                    readSet.insert(utility::normalizeColumnName(value));
                    whereCol = utility::normalizeColumnName(value);

                    phase = PHASE_WHERE_VAL;
                } else if (phase == PHASE_WHERE_VAL) {
                    if (token == '=' ||
                        token == SQL_LESS ||
                        token == SQL_LESSEQ ||
                        token == SQL_EQUAL ||
                        token == SQL_EQUALS ||
                        token == SQL_GREATER ||
                        token == SQL_GREATEREQ
                    ) {
                        goto NEXT_TOKEN;
                    }

                    if (isNameConst && !isNameConstVal) {
                        isNameConstVal = true;
                        goto NEXT_TOKEN;
                    }

                    whereSet.emplace(whereCol, (int64_t) std::stoll(value));

                    skip = true;
                    phase = PHASE_WHERE_COL;
                } else if (phase == PHASE_TABLE_NAME) {
                    if (tableName.empty()) {
                        tableName = utility::normalizeColumnName(value);
                        std::transform(tableName.begin(), tableName.end(), tableName.begin(), tolower);
                    }
                    skip = true;
                }
            }

            NEXT_TOKEN:
            i++;
        }

        for (const auto &token: readSet) {
            if (token.find('.') == std::string::npos) {
                _readSet.insert(tableName + "." + token);
            } else {
                _readSet.insert(token);
            }
        }

        for (const auto &pair: whereSet) {
            StateItem stateItem;
            StateData data;

            data.Set(pair.second);

            if (pair.first.find('.') == std::string::npos) {
                stateItem.name = tableName + "." + pair.first;
            } else {
                stateItem.name = pair.first;
            }

            stateItem.condition_type = EN_CONDITION_NONE;
            stateItem.function_type = FUNCTION_EQ;


            stateItem.data_list.emplace_back(std::move(data));
            _whereSet.emplace_back(std::move(stateItem));
        }

        return true;
    }

    bool QueryEventBase::parseDDL(int limit) {
        std::vector<int16_t> tokens;
        std::vector<size_t> tokenPos;
        hsql::SQLParser::tokenize(statement(), &tokens, &tokenPos);

        int i = 0;
        int j = 0;
        for (auto &token: tokens) {
            if (token == SQL_IDENTIFIER) {
                std::string value;
                if (i + 1 == tokens.size()) {
                    value = statement().substr(tokenPos[i]);
                } else {
                    tokenPos[i + 1] - tokenPos[i];
                    value = statement().substr(tokenPos[i], tokenPos[i + 1] - tokenPos[i]);
                }

                _writeSet.insert(utility::normalizeColumnName(value) + ".*");
                j++;
            }
            i++;
            
            if (limit > 0 && j >= limit) {
                return true;
            }
        }
        
        return true;
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::InsertStatement *insert) {
        std::string tableName = utility::normalizeColumnName(insert->tableName);
        
        _writeSet.insert(tableName + ".*");
        
        if (insert->type == hsql::InsertType::kInsertSelect) {
            extractReadWriteSet(insert->select);
        }
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::DeleteStatement *del) {
        std::string tableName = utility::normalizeColumnName(del->tableName);
    
        _writeSet.insert(tableName + ".*");
        
        std::vector<std::string> readSet;
        StateItem whereExpr;
        walkExpr(del->expr, whereExpr, readSet, tableName, true);
        
        _readSet.insert(readSet.begin(), readSet.end());
        _whereSet.push_back(whereExpr);
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::UpdateStatement *update) {
        std::string tableName = utility::normalizeColumnName(update->table->name);
        
        std::vector<std::string> readSet;
        StateItem whereExpr;
        walkExpr(update->where, whereExpr, readSet, tableName, true);
    
        _readSet.insert(readSet.begin(), readSet.end());
        _whereSet.push_back(whereExpr);
        
        for (auto &clause: *update->updates) {
            StateItem updateExpr;
            
            _writeSet.insert(tableName + "." + utility::normalizeColumnName(clause->column));
            
            updateExpr.name = tableName + "." + utility::normalizeColumnName(clause->column);
            
            if (clause->value->isType(hsql::kExprLiteralString)) {
                std::string strValue(clause->value->name);
                
                if (strValue.find("__ULTRAVERSE_SQLVAR__")) {
                    auto varName = utility::replaceAll(strValue, "__ULTRAVERSE_SQLVAR__", "");
                    auto *stateItem = findStateItem(clause->column);
                    
                    if (stateItem == nullptr) {
                        _logger->warn("SQLVAR {} referenced but not found", strValue);
    
                        StateData value;
                        value.Set(clause->value->name, strlen(clause->value->name));
                        updateExpr.data_list.push_back(value);
                    } else {
                        updateExpr.data_list.push_back(stateItem->data_list[0]);
                        _sqlVarMap[varName] = stateItem->data_list[0];
                    }
                } else {
                    StateData value;
                    value.Set(clause->value->name, strlen(clause->value->name));
                    updateExpr.data_list.push_back(value);
                }
            } else if (clause->value->isType(hsql::kExprLiteralInt)) {
                StateData value;
                value.Set(clause->value->ival);
                
                updateExpr.data_list.push_back(value);
            } else if (clause->value->isType(hsql::kExprLiteralFloat)) {
                StateData value;
                value.Set(clause->value->fval);
                updateExpr.data_list.push_back(value);
            } else if (clause->value->isType(hsql::kExprLiteralNull)) {
                StateData value;
                updateExpr.data_list.push_back(value);
            }
            
            _itemSet.push_back(updateExpr);
        }
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::SelectStatement *select) {
        std::string tableName = utility::normalizeColumnName(select->fromTable->getName());
        
        std::vector<std::string> readSet;
        if (select->selectList != nullptr) {
            for (auto &expr: *select->selectList) {
                StateItem stateItem;
                walkExpr(expr, stateItem, readSet, tableName, true);
                // ?
            }
        }
        {
            StateItem stateItem;
            walkExpr(select->whereClause, stateItem, readSet, tableName, true);
            _whereSet.push_back(stateItem);
        }
    }
    
    void QueryEventBase::walkExpr(const hsql::Expr *expr, StateItem &parent, std::vector<std::string> &readSet, const std::string &rootTable, bool isRoot) {
        if (expr == nullptr) {
            return;
        }
        
        if (expr->isType(hsql::kExprOperator)) {
            StateItem stateItem;
    
            if (expr->name != nullptr) {
                stateItem.name = utility::normalizeColumnName(std::string(expr->name));
            }
            
            stateItem.condition_type = EN_CONDITION_NONE;
            stateItem.function_type = FUNCTION_NONE;
            
            switch (expr->opType) {
                case hsql::OperatorType::kOpAnd:
                    stateItem.condition_type = EN_CONDITION_AND;
                    break;
                case hsql::OperatorType::kOpOr:
                    stateItem.condition_type = EN_CONDITION_OR;
                    break;
                case hsql::OperatorType::kOpEquals:
                    stateItem.function_type = FUNCTION_EQ;
                    break;
                case hsql::OperatorType::kOpNotEquals:
                    stateItem.function_type = FUNCTION_NE;
                    break;
                case hsql::OperatorType::kOpBetween:
                    stateItem.function_type = FUNCTION_BETWEEN;
                    break;
                case hsql::OperatorType::kOpGreaterEq:
                    stateItem.function_type = FUNCTION_GE;
                    break;
                case hsql::OperatorType::kOpGreater:
                    stateItem.function_type = FUNCTION_GT;
                    break;
                case hsql::OperatorType::kOpLessEq:
                    stateItem.function_type = FUNCTION_LE;
                    break;
                case hsql::OperatorType::kOpLess:
                    stateItem.function_type = FUNCTION_LT;
                    break;
                default:
                    break;
            }
            
            walkExpr(expr->expr, stateItem, readSet, rootTable, false);
            walkExpr(expr->expr2, stateItem, readSet, rootTable, false);
            
            if (isRoot) {
                parent.condition_type = EN_CONDITION_OR;
                parent.function_type = FUNCTION_NONE;
                parent.arg_list.push_back(stateItem);
            } else {
                parent.arg_list.push_back(stateItem);
            }
            return;
        }
        
        if (expr->isType(hsql::kExprColumnRef)) {
            auto column = expr->table == nullptr ?
                rootTable + "." + utility::normalizeColumnName(std::string(expr->name)) :
                utility::normalizeColumnName(std::string(expr->table)) + "." + utility::normalizeColumnName(std::string(expr->name));
            
            readSet.push_back(column);
            parent.name = column;
            return;
        }
        
        if (expr->isType(hsql::kExprLiteralString)) {
            std::string strValue(expr->name);
            if (strValue.find("__ULTRAVERSE_SQLVAR__") != std::string::npos) {
                auto varName = utility::replaceAll(strValue, "__ULTRAVERSE_SQLVAR__", "");
                auto stateItem = findStateItem(parent.name);
                if (stateItem == nullptr) {
                    _logger->warn("SQLVAR {} referenced but not found", strValue);
    
                    StateData value;
                    value.Set(expr->name, strlen(expr->name));
                    parent.data_list.push_back(value);
                } else {
                    parent.data_list.push_back(stateItem->data_list[0]);
                    _sqlVarMap[varName] = stateItem->data_list[0];
                }
            } else {
                StateData value;
                value.Set(expr->name, strlen(expr->name));
                parent.data_list.push_back(value);
            }
        } else if (expr->isType(hsql::kExprLiteralInt)) {
            StateData value;
            value.Set(expr->ival);
            parent.data_list.push_back(value);
        } else if (expr->isType(hsql::kExprLiteralFloat)) {
            StateData value;
            value.Set(expr->fval);
            parent.data_list.push_back(value);
        } else if (expr->isType(hsql::kExprLiteralNull)) {
            StateData value;
            parent.data_list.push_back(value);
        }
    }
    
    StateItem *QueryEventBase::findStateItem(const std::string &name) {
        auto it = std::find_if(_itemSet.begin(), _itemSet.end(), [&name](StateItem &item) {
            return item.name == name;
        });
        
        if (it != _itemSet.end()) {
            return &(*it);
        }
        
        return nullptr;
    }
    
    std::unordered_set<std::string> &QueryEventBase::readSet() {
        return _readSet;
    }
    
    std::unordered_set<std::string> &QueryEventBase::writeSet() {
        return _writeSet;
    }
    
    std::vector<StateItem> &QueryEventBase::itemSet() {
        return _itemSet;
    }
    
    std::vector<StateItem> &QueryEventBase::whereSet() {
        return _whereSet;
    }
    
    std::unordered_map<std::string, StateData> &QueryEventBase::sqlVarMap() {
        return _sqlVarMap;
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