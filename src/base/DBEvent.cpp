//
// Created by cheesekun on 8/11/22.
//

#include <algorithm>
#include <thread>

#include <libultparser/libultparser.h>
#include <ultparser_query.pb.h>

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
        int64_t threadId = std::hash<std::thread::id>()(std::this_thread::get_id());
        
        ultparser::ParseResult parseResult;
        
        char *parseResultCStr = nullptr;
        int64_t parseResultCStrSize = ult_sql_parse(
            (char *) statement().c_str(),
            threadId,
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
    
    bool QueryEventBase::processDDL(const ultparser::DDLQuery &ddlQuery) {
        _logger->warn("FIXME: DDL is not supported yet.");
        return false;
    }
    
    bool QueryEventBase::processDML(const ultparser::DMLQuery &dmlQuery) {
        if (dmlQuery.type() == ultparser::DMLQuery::SELECT) {
            return processSelect(dmlQuery);
        } else if (dmlQuery.type() == ultparser::DMLQuery::INSERT) {
            return processInsert(dmlQuery);
        } else if (dmlQuery.type() == ultparser::DMLQuery::UPDATE) {
            return processUpdate(dmlQuery);
        } else if (dmlQuery.type() == ultparser::DMLQuery::DELETE) {
            return processDelete(dmlQuery);
        } else {
            _logger->error("ASSERTION FAILURE: unknown DML type: {}", dmlQuery.type());
            return false;
        }
        
        return false;
    }
    
    bool QueryEventBase::processSelect(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        // TODO: support join
        
        for (const auto &select: dmlQuery.select()) {
            const auto &expr = select.real();
            if (expr.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                const std::string &colName = expr.identifier();
                if (colName.find('.') == std::string::npos) {
                    _readSet.insert(primaryTable + "." + colName);
                } else {
                    _readSet.insert(colName);
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
        
        for (const auto &insertion: dmlQuery.update_or_write()) {
            if (insertion.left().value_type() != ultparser::DMLQueryExpr::IDENTIFIER) {
                _logger->error("call ult_map_insert_cols() to resolve column names");
                return false;
            }
            
            std::string colName = insertion.left().identifier();
            if (colName.find('.') == std::string::npos) {
                colName = primaryTable + "." + colName;
            }
            
            _writeSet.insert(colName);
        }
        
        
        return true;
    }
    
    bool QueryEventBase::processUpdate(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        
        for (const auto &update: dmlQuery.update_or_write()) {
            std::string colName = update.left().identifier();
            if (colName.find('.') == std::string::npos) {
                colName = primaryTable + "." + colName;
            }
            
            _writeSet.insert(colName);
        }
        
        if (dmlQuery.has_where()) {
            processWhere(primaryTable, dmlQuery.where());
        }
        
        return true;
    }
    
    bool QueryEventBase::processDelete(const ultparser::DMLQuery &dmlQuery) {
        const std::string primaryTable = dmlQuery.table().real().identifier();
        
        _writeSet.insert(primaryTable + ".*");
        
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
                        _logger->warn("unsupported operator: {}", expr.operator_());
                        return;
                }
                
                processRValue(parent, right);
                this->_readSet.insert(left);
            }
        };
        
        StateItem rootItem;
        visit_node(expr, rootItem);
        
        _whereSet.emplace_back(std::move(rootItem));
        
        return true;
    }
    
    void QueryEventBase::processRValue(StateItem &item, const ultparser::DMLQueryExpr &right) {
        if (right.operator_() != ultparser::DMLQueryExpr::VALUE || right.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
            // _logger->trace("right side of where expression is not a value: {}", right.DebugString());
            if (right.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                auto it = std::find_if(_itemSet.begin(), _itemSet.end(), [&right](const StateItem &_item) {
                    return _item.name == right.identifier();
                });
                
                if (it != _itemSet.end()) {
                    item.data_list.insert(item.data_list.end(), it->data_list.begin(), it->data_list.end());
                    return;
                }
            }
            
            {
                auto it = std::find_if(_itemSet.begin(), _itemSet.end(), [&item](const StateItem &_item) {
                    return _item.name == item.name;
                });
                
                if (it != _itemSet.end()) {
                    item.data_list.insert(item.data_list.end(), it->data_list.begin(), it->data_list.end());
                } else {
                    _logger->warn("cannot map value for {}", item.name);
                }
            }
            
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
            if (expr->opType == hsql::OperatorType::kOpIn) {
                // TODO: X = A OR X = B OR ... 으로
                std::string name = utility::normalizeColumnName(std::string(expr->expr->name));

                for (auto &expr2: *expr->exprList) {
                    StateItem stateItem;
                    stateItem.name = name;
                    stateItem.condition_type = EN_CONDITION_NONE;
                    stateItem.function_type = FUNCTION_EQ;

                    walkExpr(expr2, stateItem, readSet, rootTable, false);

                    parent.arg_list.push_back(stateItem);
                }

                parent.condition_type = EN_CONDITION_OR;
                parent.function_type = FUNCTION_NONE;
            } else {
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