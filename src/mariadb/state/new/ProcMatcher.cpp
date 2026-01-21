//
// Created by cheesekun on 3/16/23.
//

#include <array>
#include <cstdint>

#include <libultparser/libultparser.h>
#include <ultparser_query.pb.h>

#include "ProcMatcher.hpp"
#include "Query.hpp"
#include "ProcCall.hpp"

#include "mariadb/DBEvent.hpp"

#include "utils/StringUtil.hpp"


namespace ultraverse::state::v2 {
    
    void ProcMatcher::load(const std::string &procedureDefinition, ProcMatcher &instance) {
        static const auto logger = createLogger("ProcMatcher");
        static thread_local uintptr_t s_parser = 0;
        if (s_parser == 0) {
            s_parser = ult_sql_parser_create();
        }
        
        logger->debug(procedureDefinition);
        ultparser::ParseResult parseResult;
        
        char *parseResultCStr = nullptr;
        
        int64_t parseResultCStrSize = ult_sql_parse_new(
            s_parser,
            (char *) procedureDefinition.c_str(),
            static_cast<int64_t>(procedureDefinition.size()),
            &parseResultCStr
        );
        
        if (parseResultCStrSize <= 0) {
            goto FAILURE;
        }
        
        if (!parseResult.ParseFromArray(parseResultCStr, parseResultCStrSize)) {
            free(parseResultCStr);
            
            goto FAILURE;
        }
        
        free(parseResultCStr);
        
        if (parseResult.result() != ultparser::ParseResult::SUCCESS) {
            logger->error("parser error: {}", parseResult.error());
            goto FAILURE;
        }
        
        if (!parseResult.warnings().empty()) {
            for (const auto &warning: parseResult.warnings()) {
                logger->warn("parser warning: {}", warning);
            }
        }
        
        {
            auto &statements = instance._codes;
            const auto &_procedureInfo = parseResult.statements()[0];
            assert(_procedureInfo.type() == ultparser::Query::PROCEDURE);
            
            const auto &procedureInfo = _procedureInfo.procedure();
            
            for (const auto &parameter: procedureInfo.parameters()) {
                instance._parameters.push_back(parameter.name());
            }
         
            const std::function<void(const google::protobuf::RepeatedPtrField<ultparser::Query> &)> insertStatements = [&statements, &insertStatements](const google::protobuf::RepeatedPtrField<ultparser::Query> &_statements) {
                for (const auto &_statement: _statements) {
                    if (_statement.type() == ultparser::Query_QueryType_IF) {
                        auto &ifBlock = _statement.if_block();
                        
                        insertStatements(ifBlock.then_block());
                        insertStatements(ifBlock.else_block());
                        
                        continue;
                    } else if (_statement.type() == ultparser::Query_QueryType_WHILE) {
                        auto &whileBlock = _statement.while_block();
                        
                        insertStatements(whileBlock.block());
                        continue;
                    }
                    
                    statements.push_back(std::make_shared<ultparser::Query>(_statement));
                }
            };
            
            insertStatements(procedureInfo.statements());
        }
        
        return;
        
        FAILURE:
        logger->error("Failed to parse procedure definition");
    }
    
    std::unordered_set<std::string> ProcMatcher::extractTableColumns(const std::string &primaryTable, const ultparser::DMLQueryExpr &expr) {
        std::unordered_set<std::string> columns;
        if (expr.operator_() == ultparser::DMLQueryExpr::AND || expr.operator_() == ultparser::DMLQueryExpr::OR) {
            for (const auto &child: expr.expressions()) {
                auto _columns = std::move(extractTableColumns(primaryTable, child));
                columns.insert(_columns.begin(), _columns.end());
            }
        } else {
            if (expr.left().value_type() != ultparser::DMLQueryExpr::IDENTIFIER) {
                // FIXME: CONCAT(users.id, users.name) = 'foo' is not supported yet.
                return columns;
            }
            
            std::string left = utility::toLower(expr.left().identifier());
            const auto &right = expr.right();
            
            if (left.find('.') == std::string::npos) {
                columns.insert(primaryTable + "." + left);
            } else {
                columns.insert(left);
            }
            
            
            // FIXME: 이거 rvalue는 read고 lvalue는 무조건 w 아님?
            if (right.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                if (right.identifier().find('.') == std::string::npos) {
                    columns.insert(utility::toLower(primaryTable + "." + right.identifier()));
                } else {
                    columns.insert(utility::toLower(right.identifier()));
                }
            }
        }
        
        return std::move(columns);
    }
    
    ProcMatcher::ProcMatcher(const std::string &procedureDefinition):
        _logger(createLogger("ProcMatcher")),
        _definition(procedureDefinition),
        _codes()
    {
        load(procedureDefinition, *this);
        extractRWSets();
    }
    
    ProcMatcher::ProcMatcher(const std::vector<std::string> &procedureCodes)
    {
    }
    
    void ProcMatcher::extractRWSets() {
        for (const auto &statement: _codes) {
            extractRWSets(*statement);
        }
    }
    
    void ProcMatcher::extractRWSets(const ultparser::Query &statement) {
        static const auto insertReadSet = [this](std::unordered_set<std::string> readSet) {
            _readSet.insert(readSet.begin(), readSet.end());
        };
        
        static const auto insertWriteSet = [this](std::unordered_set<std::string> writeSet) {
            _writeSet.insert(writeSet.begin(), writeSet.end());
        };
        
        if (statement.type() == ultparser::Query::DML) {
            const auto &dml = statement.dml();
            
            for (const auto &expr: dml.update_or_write()) {
                insertWriteSet(extractTableColumns(dml.table().real().identifier(), expr));
            }
            
            if (dml.has_where()) {
                insertReadSet(extractTableColumns(dml.table().real().identifier(), dml.where()));
            }
        }
        
        if (statement.type() == ultparser::Query::IF) {
            const auto &block = statement.if_block();
            
            insertReadSet(extractTableColumns("", block.condition()));
            
            for (const auto &q: block.then_block()) {
                extractRWSets(q);
            }
            
            for (const auto &q: block.else_block()) {
                extractRWSets(q);
            }
        }
        
        if (statement.type() == ultparser::Query::WHILE) {
            const auto &block = statement.while_block();
            
            insertReadSet(extractTableColumns("", block.condition()));
            
            for (const auto &q: block.block()) {
                extractRWSets(q);
            }
        }
    }
    
    int ProcMatcher::matchForward(const std::string &statement, int fromIndex) {
        static thread_local uintptr_t s_parser = 0;
        if (s_parser == 0) {
            s_parser = ult_sql_parser_create();
        }

        std::array<unsigned char, 20> statementHash{};
        if (ult_query_hash_new(
                s_parser,
                (char *) statement.c_str(),
                static_cast<int64_t>(statement.size()),
                reinterpret_cast<char *>(statementHash.data())
            ) != 20) {
            return -1;
        }
        
        for (int i = fromIndex; i < _codes.size(); i++) {
            // DML일때만 매칭을 시도한다.
            if (_codes[i]->type() == ultparser::Query::DML) {
                const auto &codeStatement = _codes[i]->dml().statement();
                std::array<unsigned char, 20> codeHash{};
                if (ult_query_hash_new(
                        s_parser,
                        (char *) codeStatement.c_str(),
                        static_cast<int64_t>(codeStatement.size()),
                        reinterpret_cast<char *>(codeHash.data())
                    ) != 20) {
                    continue;
                }

                if (statementHash == codeHash) {
                    return i;
                }
            }
        }

        return -1;
    }
    
    std::vector<StateItem> ProcMatcher::variableSet(const ProcCall &procCall) const {
        std::vector<StateItem> _variableSet;
        
        auto procParameters = procCall.buildItemSet(*this);
        std::copy(procParameters.begin(), procParameters.end(), std::back_inserter(_variableSet));
        
        return std::move(_variableSet);
    }
    
    std::vector<std::shared_ptr<Query>> ProcMatcher::asQuery(int index, const ProcCall &procCall, const std::vector<std::string> &keyColumns) const {
        std::vector<std::shared_ptr<Query>> queries;
        
        auto &code = codes().at(index);
        
        const std::function<void(const ultparser::Query &)> processQuery = [&](const ultparser::Query &code) {
            auto query = std::make_shared<Query>();
            auto procParameters = procCall.buildItemSet(*this);
            
            std::string statement = "SELECT 1";
            
            switch (code.type()) {
                case ultparser::Query_QueryType_DDL:
                    statement = code.ddl().statement();
                    break;
                case ultparser::Query_QueryType_DML:
                    statement = code.dml().statement();
                    break;
                case ultparser::Query_QueryType_IF: {
                    auto &block = code.if_block();
                    for (const auto &_query: block.then_block()) {
                        processQuery(_query);
                    }
                    
                    for (const auto &_query: block.else_block()) {
                        processQuery(_query);
                    }
                }
                    return;
                    
                case ultparser::Query_QueryType_WHILE: {
                    auto &whileBlock = code.while_block();
                    for (const auto &_query: whileBlock.block()) {
                        processQuery(_query);
                    }
                }
                    return;
                default:
                    _logger->error("unsupprted statement type: {}", (int) code.type());
                    break;
            }
            
            auto event = std::make_shared<mariadb::QueryEvent>(
                "fillme",
                statement,
                0
            );
            
            std::copy(procParameters.begin(), procParameters.end(), std::back_inserter(event->variableSet()));

            event->tokenize();
            if (!event->parse()) {
                event->parseDDL(1);
            }
            event->buildRWSet(keyColumns);
            
            query->setStatement(statement);
            query->setFlags(Query::FLAG_IS_PROCCALL_RECOVERED_QUERY);
            if (code.type() == ultparser::Query_QueryType_DDL) {
                query->setFlags(query->flags() | Query::FLAG_IS_DDL);
            }
            
            query->readSet().insert(
                query->readSet().end(),
                event->readSet().begin(), event->readSet().end()
            );
            query->writeSet().insert(
                query->writeSet().end(),
                event->writeSet().begin(), event->writeSet().end()
            );
            {
                ColumnSet readColumns;
                ColumnSet writeColumns;
                event->columnRWSet(readColumns, writeColumns);
                query->readColumns().insert(readColumns.begin(), readColumns.end());
                query->writeColumns().insert(writeColumns.begin(), writeColumns.end());
            }
            
            query->varMap().insert(
                query->varMap().end(),
                event->variableSet().begin(), event->variableSet().end()
            );
            
            return queries.push_back(query);
        };
        
        processQuery(*code);
        
        return queries;
    }
    
    const std::vector<std::string> &ProcMatcher::parameters() const {
        return _parameters;
    }
    
    const std::vector<std::shared_ptr<ultparser::Query>> ProcMatcher::codes() const {
        return _codes;
    }
    
    const std::unordered_set<std::string> &ProcMatcher::readSet() const {
        return _readSet;
    }
    
    const std::unordered_set<std::string> &ProcMatcher::writeSet() const {
        return _writeSet;
    }
}
