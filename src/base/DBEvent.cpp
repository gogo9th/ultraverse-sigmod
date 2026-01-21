//
// Created by cheesekun on 8/11/22.
//

#include <algorithm>
#include <cstdint>

#include <libultparser/libultparser.h>
#include <ultparser_query.pb.h>

#include "DBEvent.hpp"

#include "SQLParser.h"
#include "bison_parser.h"
#include "util/sqlhelper.h"

#include "utils/StringUtil.hpp"


namespace ultraverse::base {
    
    QueryEventBase::QueryEventBase():
        _logger(createLogger("QueryEventBase")),
        _queryType(UNKNOWN)
    {
    
    }
    
    bool QueryEventBase::tokenize() {
        return hsql::SQLParser::tokenize(statement(), &_tokens, &_tokenPos);
    }
    
    bool QueryEventBase::parse() {
        static thread_local uintptr_t s_parser = 0;
        if (s_parser == 0) {
            s_parser = ult_sql_parser_create();
        }
        
        ultparser::ParseResult parseResult;
        
        char *parseResultCStr = nullptr;
        const auto &sqlStatement = statement();
        int64_t parseResultCStrSize = ult_sql_parse_new(
            s_parser,
            (char *) sqlStatement.c_str(),
            static_cast<int64_t>(sqlStatement.size()),
            &parseResultCStr
        );
        
        if (parseResultCStrSize <= 0) {
            _logger->error("could not parse SQL statement: {}", statement());
            return false;
        }
        
        if (!parseResult.ParseFromArray(parseResultCStr, parseResultCStrSize)) {
            free(parseResultCStr);
            
            _logger->error("could not parse SQL statement: {}", statement());
            return false;
        }
        free(parseResultCStr);
        
        if (parseResult.result() != ultparser::ParseResult::SUCCESS) {
            _logger->error("parser error: {}", parseResult.error());
            return false;
        }
        
        if (!parseResult.warnings().empty()) {
            for (const auto &warning: parseResult.warnings()) {
                _logger->warn("parser warning: {}", warning);
            }
        }
        
        assert(parseResult.statements().size() == 1);
        auto &statement = parseResult.statements()[0];
        
        if (statement.has_ddl()) {
            return processDDL(statement.ddl());
        }
        
        if (statement.has_dml()) {
            return processDML(statement.dml());
        }
        
        _logger->error("ASSERTION FAILURE: result has no errors but it contains no DDL or DML: {}", this->statement());
        return false;
    }
    
    void QueryEventBase::buildRWSet(const std::vector<std::string> &keyColumns) {
        if (_queryType == SELECT) {
            _readItems.insert(
                _readItems.end(),
                _whereSet.begin(), _whereSet.end()
            );
        } else if (_queryType == INSERT) {
            _writeItems.insert(
                _writeItems.end(),
                _itemSet.begin(), _itemSet.end()
            );
        } else if (_queryType == UPDATE) {
            {
                auto it = _itemSet.begin();
                
                while (true) {
                    it = std::find_if(it, _itemSet.end(), [this, &keyColumns](const StateItem &item) {
                        return std::find(keyColumns.begin(), keyColumns.end(), item.name) != keyColumns.end() ||
                               std::any_of(_writeColumns.begin(), _writeColumns.end(), [&item](const std::string &colName) {
                                   return item.name == colName;
                               });
                    });
                    
                    if (it == _itemSet.end()) {
                        break;
                    }
                    
                    _writeItems.emplace_back(*it);
                    
                    it++;
                }
            }
            
            _readItems.insert(
                _readItems.end(),
                _whereSet.begin(), _whereSet.end()
            );
        } else if (_queryType == DELETE) {
            _writeItems.insert(
                _writeItems.end(),
                _itemSet.begin(), _itemSet.end()
            );
            
            _readItems.insert(
                _readItems.end(),
                _whereSet.begin(), _whereSet.end()
            );
        }

        const bool needsFullScanWildcard =
            _whereSet.empty() && (
                (_queryType == SELECT && _readItems.empty()) ||
                (_queryType == UPDATE && _writeItems.empty()) ||
                (_queryType == DELETE && _writeItems.empty())
            );

        if (needsFullScanWildcard) {
            auto addWildcardItem = [this](const std::string &name, bool isWrite) {
                if (name.empty()) {
                    return;
                }
                StateItem wildcard = StateItem::Wildcard(name);
                if (isWrite) {
                    _writeItems.emplace_back(std::move(wildcard));
                } else {
                    _readItems.emplace_back(std::move(wildcard));
                }
            };

            const bool isWrite = _queryType == UPDATE || _queryType == DELETE;

            if (!keyColumns.empty()) {
                std::unordered_set<std::string> relatedTablesLower;
                relatedTablesLower.reserve(_relatedTables.size());
                for (const auto &table : _relatedTables) {
                    if (!table.empty()) {
                        relatedTablesLower.insert(utility::toLower(table));
                    }
                }

                for (const auto &keyColumn : keyColumns) {
                    if (keyColumn.empty()) {
                        continue;
                    }
                    const auto normalizedKey = utility::toLower(keyColumn);
                    const auto tablePair = utility::splitTableName(normalizedKey);
                    if (!tablePair.first.empty() &&
                        relatedTablesLower.find(tablePair.first) != relatedTablesLower.end()) {
                        addWildcardItem(normalizedKey, isWrite);
                    }
                }
            } else {
                for (const auto &table : _relatedTables) {
                    if (!table.empty()) {
                        addWildcardItem(utility::toLower(table) + ".*", isWrite);
                    }
                }
            }
        }
    }
    
    bool QueryEventBase::processDDL(const ultparser::DDLQuery &ddlQuery) {
        _logger->warn("FIXME: DDL is not supported yet.");
        return false;
    }
    
    bool QueryEventBase::processDML(const ultparser::DMLQuery &dmlQuery) {
        if (dmlQuery.type() == ultparser::DMLQuery::SELECT) {
            _queryType = SELECT;
            return processSelect(dmlQuery);
        } else if (dmlQuery.type() == ultparser::DMLQuery::INSERT) {
            _queryType = INSERT;
            return processInsert(dmlQuery);
        } else if (dmlQuery.type() == ultparser::DMLQuery::UPDATE) {
            _queryType = UPDATE;
            return processUpdate(dmlQuery);
        } else if (dmlQuery.type() == ultparser::DMLQuery::DELETE) {
            _queryType = DELETE;
            return processDelete(dmlQuery);
        } else {
            _logger->error("ASSERTION FAILURE: unknown DML type: {}", (int) dmlQuery.type());
            return false;
        }
        
        return false;
    }
    
    bool QueryEventBase::processSelect(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        // TODO: support join
        
        _relatedTables.insert(primaryTable);
        
        for (const auto &join: dmlQuery.join()) {
            _relatedTables.insert(join.real().identifier());
        }
        
        for (const auto &select: dmlQuery.select()) {
            const auto &expr = select.real();
            if (expr.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                const std::string &colName = expr.identifier();
                if (colName.find('.') == std::string::npos) {
                    _readColumns.insert(primaryTable + "." + colName);
                } else {
                    _readColumns.insert(colName);
                }
            } else {
                // _logger->trace("not selecting column: {}", expr.DebugString());
            }
        }
        
        if (dmlQuery.has_where()) {
            processWhere(primaryTable, dmlQuery.where());
        }
        
        return true;
    }
    
    bool QueryEventBase::processInsert(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        _relatedTables.insert(primaryTable);
        
        for (const auto &insertion: dmlQuery.update_or_write()) {
            if (insertion.left().value_type() != ultparser::DMLQueryExpr::IDENTIFIER) {
                _logger->error("call ult_map_insert_cols() to resolve column names");
                return false;
            }
            
            std::string colName = insertion.left().identifier();
            if (colName.find('.') == std::string::npos) {
                colName = primaryTable + "." + colName;
            }
            
            _writeColumns.insert(colName);
        }
        
        
        return true;
    }
    
    bool QueryEventBase::processUpdate(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        _relatedTables.insert(primaryTable);
        
        for (const auto &update: dmlQuery.update_or_write()) {
            std::string colName = update.left().identifier();
            if (colName.find('.') == std::string::npos) {
                colName = primaryTable + "." + colName;
            }
            
            _writeColumns.insert(colName);
        }
        
        if (dmlQuery.has_where()) {
            processWhere(primaryTable, dmlQuery.where());
        }
        
        return true;
    }
    
    bool QueryEventBase::processDelete(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        _relatedTables.insert(primaryTable);
        
        _writeColumns.insert(primaryTable + ".*");
        
        if (dmlQuery.has_where()) {
            processWhere(primaryTable, dmlQuery.where());
        }
        
        return true;
    }
    
    bool QueryEventBase::processWhere(const std::string &primaryTable, const ultparser::DMLQueryExpr &expr) {
        std::function<void(const ultparser::DMLQueryExpr&, StateItem &)> visit_node = [this, &primaryTable, &visit_node](const ultparser::DMLQueryExpr &expr, StateItem &parent) {
            if (expr.operator_() == ultparser::DMLQueryExpr::AND || expr.operator_() == ultparser::DMLQueryExpr::OR) {
                assert(parent.function_type == FUNCTION_NONE);
                parent.condition_type = expr.operator_() == ultparser::DMLQueryExpr::AND ? EN_CONDITION_AND : EN_CONDITION_OR;
                
                for (const auto &child: expr.expressions()) {
                    StateItem item;
                    visit_node(child, item);
                    
                    parent.arg_list.emplace_back(std::move(item));
                }
            } else {
                assert(parent.condition_type == EN_CONDITION_NONE);
                // std::cout << expr.left().DebugString() << " " << expr.operator_() << " " << expr.right().DebugString() << std::endl;
                
                if (expr.left().value_type() != ultparser::DMLQueryExpr::IDENTIFIER) {
                    // FIXME: CONCAT(users.id, users.name) = 'foo' is not supported yet.
                    _logger->warn("left side of where expression is not an identifier");
                    return;
                }
                
                std::string left = utility::toLower(expr.left().identifier());
                const auto &right = expr.right();
                
                if (left.find('.') == std::string::npos) {
                    left = primaryTable + "." + left;
                }
                
                parent.name = left;
                
                switch (expr.operator_()) {
                    case ultparser::DMLQueryExpr_Operator_EQ:
                        parent.function_type = FUNCTION_EQ;
                        break;
                    case ultparser::DMLQueryExpr_Operator_NEQ:
                        parent.function_type = FUNCTION_NE;
                        break;
                    case ultparser::DMLQueryExpr_Operator_LT:
                        parent.function_type = FUNCTION_LT;
                        break;
                    case ultparser::DMLQueryExpr_Operator_LTE:
                        parent.function_type = FUNCTION_LE;
                        break;
                    case ultparser::DMLQueryExpr_Operator_GT:
                        parent.function_type = FUNCTION_GT;
                        break;
                    case ultparser::DMLQueryExpr_Operator_GTE:
                        parent.function_type = FUNCTION_GE;
                        break;
                    case ultparser::DMLQueryExpr_Operator_LIKE:
                        _logger->warn("LIKE operator is not supported yet");
                        parent.function_type = FUNCTION_EQ;
                        break;
                    case ultparser::DMLQueryExpr_Operator_NOT_LIKE:
                        _logger->warn("NOT LIKE operator is not supported yet");
                        parent.function_type = FUNCTION_NE;
                        break;
                    
                    case ultparser::DMLQueryExpr_Operator_IN:
                        // treat as (a = 1 or a = 2 or a = 3)
                        parent.function_type = FUNCTION_EQ;
                        break;
                        
                    case ultparser::DMLQueryExpr_Operator_NOT_IN:
                        // treat as (a != 1 and a != 2 and a != 3)
                        parent.function_type = FUNCTION_NE;
                        break;
                    
                    case ultparser::DMLQueryExpr_Operator_BETWEEN:
                        // treat as (a >= 1 and a <= 3)
                        parent.function_type = FUNCTION_EQ;
                        break;
                        
                    case ultparser::DMLQueryExpr_Operator_NOT_BETWEEN:
                        // treat as (a < 1 or a > 3)
                        parent.function_type = FUNCTION_NE;
                        break;
                    default:
                        _logger->warn("unsupported operator: {}", (int) expr.operator_());
                        return;
                }
                
                processRValue(parent, right);
                this->_readColumns.insert(left);
            }
        };
        
        std::function<void(StateItem &)> flatInsertNode = [this, &flatInsertNode](StateItem &item) {
            if (item.condition_type == EN_CONDITION_AND || item.condition_type == EN_CONDITION_OR) {
                std::for_each(item.arg_list.begin(), item.arg_list.end(), flatInsertNode);
            } else {
                _whereSet.emplace_back(item);
            }
        };
        
        StateItem rootItem;
        visit_node(expr, rootItem);
        
        flatInsertNode(rootItem);
        
        return true;
    }
    
    void QueryEventBase::processRValue(StateItem &item, const ultparser::DMLQueryExpr &right) {
        if (right.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
            // _logger->trace("right side of where expression is not a value: {}", right.DebugString());
            const std::string &identifierName = right.identifier();
            
            {
                auto it = std::find_if(_itemSet.begin(), _itemSet.end(), [&item, &identifierName](const StateItem &_item) {
                    return _item.name == item.name || _item.name == identifierName;
                });
                
                if (it != _itemSet.end()) {
                    item.data_list.insert(item.data_list.end(), it->data_list.begin(), it->data_list.end());
                    
                    StateItem tmp = *it; // copy
                    tmp.name = identifierName;
                    _variableSet.emplace_back(std::move(tmp));
                    return;
                }
            }
            
            {
                auto it = std::find_if(_variableSet.begin(), _variableSet.end(), [&item, &identifierName](const StateItem &_item) {
                    return _item.name == item.name || _item.name == identifierName;
                });
                
                if (it != _variableSet.end()) {
                    item.data_list.insert(item.data_list.end(), it->data_list.begin(), it->data_list.end());
                    return;
                }
            }
            
            _logger->warn("cannot map value for {}", item.name);
            return;
        }
        
        switch (right.value_type()) {
            case ultparser::DMLQueryExpr::IDENTIFIER:
                return;
            case ultparser::DMLQueryExpr::INTEGER: {
                StateData data;
                data.Set(right.integer());
                item.data_list.emplace_back(std::move(data));
            }
                break;
            case ultparser::DMLQueryExpr::DOUBLE: {
                StateData data;
                data.Set(right.double_());
                item.data_list.emplace_back(std::move(data));
            }
                break;
            case ultparser::DMLQueryExpr::DECIMAL: {
                StateData data;
                data.SetDecimal(right.decimal());
                item.data_list.emplace_back(std::move(data));
            }
                break;
            case ultparser::DMLQueryExpr::STRING: {
                StateData data;
                data.Set(right.string().c_str(), right.string().size());
                item.data_list.emplace_back(std::move(data));
            }
                break;
            case ultparser::DMLQueryExpr::BOOL: {
                StateData data;
                data.Set(right.bool_() ? (int64_t) 1 : 0);
                item.data_list.emplace_back(std::move(data));
            }
                break;
            case ultparser::DMLQueryExpr::NULL_: {
                _logger->error("putting NULL value in StateData is not supported yet");
                throw std::runtime_error("putting NULL value in StateData is not supported yet");
            }
                break;
            case ultparser::DMLQueryExpr::LIST: {
                for (const auto &child: right.value_list()) {
                    processRValue(item, child);
                }
            }
                break;
            default:
                // :_logger->error("unsupported right side of where expression: {}", right.DebugString());
                throw std::runtime_error("unsupported right side of where expression");
        }
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

                    try {
                        whereSet.emplace(whereCol, (int64_t) std::stoll(value));
                    } catch (std::invalid_argument &e) {

                    }

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
                _readColumns.insert(tableName + "." + token);
            } else {
                _readColumns.insert(token);
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

                _writeColumns.insert(utility::normalizeColumnName(value) + ".*");
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
        
        _writeColumns.insert(tableName + ".*");
        
        if (insert->type == hsql::InsertType::kInsertSelect) {
            extractReadWriteSet(insert->select);
        }
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::DeleteStatement *del) {
        std::string tableName = utility::normalizeColumnName(del->tableName);
    
        _writeColumns.insert(tableName + ".*");
        
        std::vector<std::string> readSet;
        StateItem whereExpr;
        walkExpr(del->expr, whereExpr, readSet, tableName, true);
        
        _readColumns.insert(readSet.begin(), readSet.end());
        _whereSet.push_back(whereExpr);
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::UpdateStatement *update) {
        throw std::runtime_error("deprecated");
    }
    
    void QueryEventBase::extractReadWriteSet(const hsql::SelectStatement *select) {
        throw std::runtime_error("deprecated");
    }
    
    void QueryEventBase::walkExpr(const hsql::Expr *expr, StateItem &parent, std::vector<std::string> &readSet, const std::string &rootTable, bool isRoot) {
        throw std::runtime_error("deprecated");
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
    
    std::vector<StateItem> &QueryEventBase::itemSet() {
        return _itemSet;
    }
    
    std::vector<StateItem> &QueryEventBase::readSet() {
        return _readItems;
    }
    
    std::vector<StateItem> &QueryEventBase::writeSet() {
        return _writeItems;
    }
    
    std::vector<StateItem> &QueryEventBase::variableSet() {
        return _variableSet;
    }

    QueryEventBase::QueryType QueryEventBase::queryType() const {
        return _queryType;
    }

    void QueryEventBase::columnRWSet(std::set<std::string> &readColumns, std::set<std::string> &writeColumns) const {
        for (const auto &column : _readColumns) {
            readColumns.insert(utility::toLower(column));
        }

        for (const auto &column : _writeColumns) {
            writeColumns.insert(utility::toLower(column));
        }

        if (_queryType == INSERT || _queryType == DELETE) {
            for (const auto &table : _relatedTables) {
                if (!table.empty()) {
                    writeColumns.insert(utility::toLower(table) + ".*");
                }
            }
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
