//
// Created by cheesekun on 8/11/22.
//

#include <algorithm>
#include <cstdint>

#include <libultparser/libultparser.h>
#include <ultparser_query.pb.h>

#include "DBEvent.hpp"

#include "mariadb/state/WhereClauseBuilder.hpp"

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
        
        const int statementCount = parseResult.statements_size();
        if (statementCount == 0) {
            _logger->error("parser returned no statements for SQL: {}", statement());
            return false;
        }
        if (statementCount != 1) {
            _logger->warn("parser returned {} statements; using the first for SQL: {}", statementCount, statement());
        }
        const auto &statement = parseResult.statements(0);
        
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
            processWhere(dmlQuery, dmlQuery.where());
        }
        
        return true;
    }
    
    bool QueryEventBase::processInsert(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        if (!primaryTable.empty()) {
            _relatedTables.insert(primaryTable);
        }

        bool hasExplicitColumn = false;
        bool hasUnknownColumn = false;

        for (const auto &insertion: dmlQuery.update_or_write()) {
            if (insertion.has_left() && insertion.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                std::string colName = insertion.left().identifier();
                if (colName.find('.') == std::string::npos) {
                    colName = primaryTable + "." + colName;
                }
                _writeColumns.insert(colName);
                hasExplicitColumn = true;
            } else {
                hasUnknownColumn = true;
            }

            processExprForColumns(primaryTable, insertion.right());
        }

        if ((!hasExplicitColumn || hasUnknownColumn) && !_itemSet.empty()) {
            for (const auto &item : _itemSet) {
                if (!item.name.empty()) {
                    _writeColumns.insert(item.name);
                }
            }
        }

        if (_writeColumns.empty() && !primaryTable.empty()) {
            _writeColumns.insert(primaryTable + ".*");
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
            processWhere(dmlQuery, dmlQuery.where());
        }
        
        return true;
    }
    
    bool QueryEventBase::processDelete(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        _relatedTables.insert(primaryTable);
        
        _writeColumns.insert(primaryTable + ".*");
        
        if (dmlQuery.has_where()) {
            processWhere(dmlQuery, dmlQuery.where());
        }
        
        return true;
    }
    
    bool QueryEventBase::processWhere(const ultparser::DMLQuery &dmlQuery, const ultparser::DMLQueryExpr &expr) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        ::ultraverse::state::WhereClauseOptions options;
        options.primaryTable = primaryTable;
        options.tableNames.clear();
        if (!primaryTable.empty()) {
            options.tableNames.push_back(primaryTable);
        }
        for (const auto &join : dmlQuery.join()) {
            const std::string joinTable = join.real().identifier();
            if (!joinTable.empty()) {
                options.tableNames.push_back(joinTable);
            }
        }
        options.logger = _logger;
        options.onReadColumn = [this](const std::string &columnName) {
            _readColumns.insert(columnName);
        };
        options.onValueExpr = [this](const std::string &tableName, const ultparser::DMLQueryExpr &valueExpr) {
            processExprForColumns(tableName, valueExpr);
        };
        options.resolveIdentifier = [this](
            const std::string &leftName,
            const std::string &identifierName,
            std::vector<StateData> &outValues
        ) -> bool {
            auto it = std::find_if(_itemSet.begin(), _itemSet.end(), [&leftName, &identifierName](const StateItem &_item) {
                return _item.name == leftName || _item.name == identifierName;
            });
            if (it != _itemSet.end()) {
                outValues.insert(outValues.end(), it->data_list.begin(), it->data_list.end());

                StateItem tmp = *it;
                tmp.name = identifierName;
                _variableSet.emplace_back(std::move(tmp));
                return true;
            }

            auto itVar = std::find_if(_variableSet.begin(), _variableSet.end(), [&leftName, &identifierName](const StateItem &_item) {
                return _item.name == leftName || _item.name == identifierName;
            });
            if (itVar != _variableSet.end()) {
                outValues.insert(outValues.end(), itVar->data_list.begin(), itVar->data_list.end());
                return true;
            }

            return false;
        };
        options.resolveColumnIdentifier = [&options](
            const std::string &,
            const std::string &identifierName,
            std::vector<std::string> &outColumns
        ) -> bool {
            if (identifierName.empty()) {
                return false;
            }
            if (!identifierName.empty() && identifierName[0] == '@') {
                return false;
            }
            std::string normalized = utility::toLower(identifierName);
            if (normalized.find('.') != std::string::npos) {
                outColumns.push_back(normalized);
                return true;
            }
            if (!options.tableNames.empty()) {
                for (const auto &table : options.tableNames) {
                    if (table.empty()) {
                        continue;
                    }
                    outColumns.push_back(utility::toLower(table + "." + normalized));
                }
                return !outColumns.empty();
            }
            if (!options.primaryTable.empty()) {
                outColumns.push_back(utility::toLower(options.primaryTable + "." + normalized));
                return true;
            }
            return false;
        };

        auto whereItems = ::ultraverse::state::buildWhereItems(expr, options);
        _whereSet.insert(_whereSet.end(), whereItems.begin(), whereItems.end());
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
                std::string subqueryPrimary = subquery.table().real().identifier();
                const std::string outerPrimary = primaryTable;
                if (subqueryPrimary.empty()) {
                    // If the subquery only selects from a single derived table, use its base table
                    // as a fallback to qualify unqualified identifiers.
                    auto resolveDerivedPrimary = [](const ultparser::DMLQuery &query) -> std::string {
                        if (query.join_size() != 0 || query.subqueries_size() != 1) {
                            return {};
                        }
                        const auto &derived = query.subqueries(0);
                        const std::string derivedPrimary = derived.table().real().identifier();
                        if (derivedPrimary.empty()) {
                            return {};
                        }
                        if (derived.join_size() != 0 || derived.subqueries_size() != 0) {
                            return {};
                        }
                        return derivedPrimary;
                    };
                    subqueryPrimary = resolveDerivedPrimary(subquery);
                }
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
