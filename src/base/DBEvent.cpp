//
// Created by cheesekun on 8/11/22.
//

#include <algorithm>
#include <cstdint>

#include <libultparser/libultparser.h>
#include <ultparser_query.pb.h>

#include "DBEvent.hpp"

#include "utils/StringUtil.hpp"


namespace ultraverse::base {
    
    QueryEventBase::QueryEventBase():
        _logger(createLogger("QueryEventBase")),
        _queryType(UNKNOWN)
    {
    
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
        switch (ddlQuery.type()) {
            case ultparser::DDLQuery::CREATE:
                _queryType = CREATE_TABLE;
                break;
            case ultparser::DDLQuery::ALTER:
                _queryType = ALTER_TABLE;
                break;
            case ultparser::DDLQuery::DROP:
                _queryType = DROP_TABLE;
                break;
            case ultparser::DDLQuery::TRUNCATE:
                _queryType = TRUNCATE_TABLE;
                break;
            case ultparser::DDLQuery::RENAME:
                _queryType = RENAME_TABLE;
                break;
            case ultparser::DDLQuery::UNKNOWN:
            default:
                _queryType = DDL_UNKNOWN;
                break;
        }

        _logger->warn("DDL is not supported yet.");
        return true;
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
        
        if (!primaryTable.empty()) {
            _relatedTables.insert(primaryTable);
        }
        
        for (const auto &join: dmlQuery.join()) {
            const std::string joinTable = join.real().identifier();
            if (!joinTable.empty()) {
                _relatedTables.insert(joinTable);
            }
        }

        for (const auto &subquery: dmlQuery.subqueries()) {
            _logger->debug("processing derived table subquery in select");
            ultparser::DMLQueryExpr subqueryExpr;
            subqueryExpr.set_value_type(ultparser::DMLQueryExpr::SUBQUERY);
            *subqueryExpr.mutable_subquery() = subquery;
            processExprForColumns(primaryTable, subqueryExpr);
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
                processExprForColumns(primaryTable, expr);
                // _logger->trace("not selecting column: {}", expr.DebugString());
            }
        }

        for (const auto &groupExpr : dmlQuery.group_by()) {
            processExprForColumns(primaryTable, groupExpr);
        }

        if (dmlQuery.has_having()) {
            processExprForColumns(primaryTable, dmlQuery.having());
        }
        
        if (dmlQuery.has_where()) {
            processWhere(primaryTable, dmlQuery.where());
        }
        
        return true;
    }
    
    bool QueryEventBase::processInsert(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        if (!primaryTable.empty()) {
            _relatedTables.insert(primaryTable);
        }
        
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
            processExprForColumns(primaryTable, insertion.right());
        }
        
        
        return true;
    }
    
    bool QueryEventBase::processUpdate(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        if (!primaryTable.empty()) {
            _relatedTables.insert(primaryTable);
        }
        
        for (const auto &update: dmlQuery.update_or_write()) {
            std::string colName = update.left().identifier();
            if (colName.find('.') == std::string::npos) {
                colName = primaryTable + "." + colName;
            }
            
            _writeColumns.insert(colName);
            processExprForColumns(primaryTable, update.right());
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
            if (expr.value_type() == ultparser::DMLQueryExpr::SUBQUERY) {
                _logger->debug("where clause contains subquery expression");
                processExprForColumns(primaryTable, expr);
                return;
            }
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

    void QueryEventBase::processExprForColumns(const std::string &primaryTable, const ultparser::DMLQueryExpr &expr, bool qualifyUnqualified) {
        auto addIdentifier = [this, &primaryTable, qualifyUnqualified](const std::string &identifier) {
            if (identifier.empty()) {
                return;
            }
            if (identifier.find('.') != std::string::npos) {
                _readColumns.insert(identifier);
                return;
            }
            if (!qualifyUnqualified) {
                _logger->trace("skip unqualified identifier without scope: {}", identifier);
                return;
            }
            if (primaryTable.empty()) {
                _logger->trace("unqualified identifier without primary table: {}", identifier);
                _readColumns.insert(identifier);
            } else {
                _readColumns.insert(primaryTable + "." + identifier);
            }
        };

        if (expr.operator_() == ultparser::DMLQueryExpr::AND || expr.operator_() == ultparser::DMLQueryExpr::OR) {
            for (const auto &child: expr.expressions()) {
                processExprForColumns(primaryTable, child, qualifyUnqualified);
            }
            return;
        }

        switch (expr.value_type()) {
            case ultparser::DMLQueryExpr::IDENTIFIER:
                addIdentifier(expr.identifier());
                return;
            case ultparser::DMLQueryExpr::FUNCTION:
                _logger->trace("processing function expression for columns: {}", expr.function());
                for (const auto &arg: expr.value_list()) {
                    processExprForColumns(primaryTable, arg, qualifyUnqualified);
                }
                return;
            case ultparser::DMLQueryExpr::SUBQUERY: {
                if (!expr.has_subquery()) {
                    _logger->warn("subquery expression has no payload");
                    return;
                }

                _logger->debug("processing subquery expression for columns");
                const auto &subquery = expr.subquery();
                const std::string subqueryPrimary = subquery.table().real().identifier();
                const std::string outerPrimary = primaryTable;
                if (!subqueryPrimary.empty()) {
                    _relatedTables.insert(subqueryPrimary);
                }

                for (const auto &join: subquery.join()) {
                    const std::string joinTable = join.real().identifier();
                    if (!joinTable.empty()) {
                        _relatedTables.insert(joinTable);
                    }
                }

                for (const auto &select: subquery.select()) {
                    processExprForColumns(subqueryPrimary, select.real(), true);
                    if (!outerPrimary.empty() && outerPrimary != subqueryPrimary) {
                        processExprForColumns(outerPrimary, select.real(), false);
                    }
                }

                for (const auto &groupExpr: subquery.group_by()) {
                    processExprForColumns(subqueryPrimary, groupExpr, true);
                    if (!outerPrimary.empty() && outerPrimary != subqueryPrimary) {
                        processExprForColumns(outerPrimary, groupExpr, false);
                    }
                }

                if (subquery.has_having()) {
                    processExprForColumns(subqueryPrimary, subquery.having(), true);
                    if (!outerPrimary.empty() && outerPrimary != subqueryPrimary) {
                        processExprForColumns(outerPrimary, subquery.having(), false);
                    }
                }

                if (subquery.has_where()) {
                    processExprForColumns(subqueryPrimary, subquery.where(), true);
                    if (!outerPrimary.empty() && outerPrimary != subqueryPrimary) {
                        _logger->trace("processing subquery where with outer scope: {}", outerPrimary);
                        processExprForColumns(outerPrimary, subquery.where(), false);
                    }
                }

                for (const auto &derived: subquery.subqueries()) {
                    ultparser::DMLQueryExpr derivedExpr;
                    derivedExpr.set_value_type(ultparser::DMLQueryExpr::SUBQUERY);
                    *derivedExpr.mutable_subquery() = derived;
                    processExprForColumns(subqueryPrimary, derivedExpr, true);
                    if (!outerPrimary.empty() && outerPrimary != subqueryPrimary) {
                        processExprForColumns(outerPrimary, derivedExpr, false);
                    }
                }

                return;
            }
            default:
                break;
        }

        const auto &left = expr.left();
        const auto &right = expr.right();

        auto hasMeaningfulExpr = [](const ultparser::DMLQueryExpr &node) {
            if (node.value_type() != ultparser::DMLQueryExpr::UNKNOWN_VALUE) {
                return true;
            }
            if (node.operator_() != ultparser::DMLQueryExpr::UNKNOWN) {
                return true;
            }
            if (!node.expressions().empty() || !node.value_list().empty()) {
                return true;
            }
            return node.has_subquery();
        };

        if (hasMeaningfulExpr(left)) {
            processExprForColumns(primaryTable, left, qualifyUnqualified);
        }
        if (hasMeaningfulExpr(right)) {
            processExprForColumns(primaryTable, right, qualifyUnqualified);
        }
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
            case ultparser::DMLQueryExpr::FUNCTION: {
                _logger->trace("processing function rvalue for {}", item.name);
                const auto tablePair = utility::splitTableName(item.name);
                processExprForColumns(tablePair.first, right);
            }
                break;
            case ultparser::DMLQueryExpr::SUBQUERY: {
                if (!right.has_subquery()) {
                    _logger->warn("subquery rvalue has no payload for {}", item.name);
                    return;
                }
                _logger->debug("processing subquery rvalue for {}", item.name);
                const auto tablePair = utility::splitTableName(item.name);
                processExprForColumns(tablePair.first, right);
            }
                break;
            default:
                // :_logger->error("unsupported right side of where expression: {}", right.DebugString());
                throw std::runtime_error("unsupported right side of where expression");
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
    
    bool QueryEventBase::isDDL() const {
        return (
            _queryType == DDL_UNKNOWN   ||
            _queryType == CREATE_TABLE ||
            _queryType == ALTER_TABLE  ||
            _queryType == DROP_TABLE   ||
            _queryType == RENAME_TABLE ||
            _queryType == TRUNCATE_TABLE
        );
    }
    
    bool QueryEventBase::isDML() const {
        return (
            _queryType == SELECT ||
            _queryType == INSERT ||
            _queryType == UPDATE ||
            _queryType == DELETE
        );
    }
    
    
    
}
