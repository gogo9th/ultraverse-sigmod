//
// Created by cheesekun on 12/7/22.
//

#include <stdexcept>

#include <libultparser/libultparser.h>

#include "mariadb/DBEvent.hpp"
#include "StateChanger.hpp"


namespace ultraverse::state::v2 {
    
    std::shared_ptr<Transaction> StateChanger::loadUserQuery(const std::string &path) {
        // read entire file
        std::ifstream ifs(path);
        if (!ifs.is_open()) {
            _logger->error("failed to open user query file: {}", path);
            return nullptr;
        }

        std::stringstream buffer;
        buffer << ifs.rdbuf();

        return std::move(parseUserQuery(buffer.str()));
    }
    
    std::shared_ptr<Transaction> StateChanger::parseUserQuery(const std::string &sql) {
        static thread_local uintptr_t s_parser = 0;
        if (s_parser == 0) {
            s_parser = ult_sql_parser_create();
        }

        ultparser::ParseResult parseResult;

        char *parseResultCStr = nullptr;
        int64_t parseResultCStrSize = ult_sql_parse_new(
            s_parser,
            (char *) sql.c_str(),
            static_cast<int64_t>(sql.size()),
            &parseResultCStr
        );

        if (parseResultCStrSize <= 0) {
            _logger->error("could not parse SQL statement: {}", sql);
            return nullptr;
        }

        if (!parseResult.ParseFromArray(parseResultCStr, parseResultCStrSize)) {
            free(parseResultCStr);

            _logger->error("could not parse SQL statement: {}", sql);
            return nullptr;
        }
        free(parseResultCStr);

        if (parseResult.result() != ultparser::ParseResult::SUCCESS) {
            _logger->error("parser error: {}", parseResult.error());
            return nullptr;
        }

        if (!parseResult.warnings().empty()) {
            for (const auto &warning: parseResult.warnings()) {
                _logger->warn("parser warning: {}", warning);
            }
        }

        auto &statements = parseResult.statements();
        std::vector<mariadb::QueryEvent> queryEvents;
        auto transaction = std::make_shared<Transaction>();

        transaction->setXid(0);
        transaction->setGid(0);
        transaction->setTimestamp(0);

        transaction->setFlags(Transaction::FLAG_FORCE_EXECUTE);

        queryEvents.reserve(statements.size());
        for (const auto &pbStatement : statements) {
            if (pbStatement.has_dml()) {
                queryEvents.emplace_back(_plan.dbName(), pbStatement, 0);
                continue;
            }

            if (pbStatement.has_ddl()) {
                _logger->error("DDL statement is not supported yet: {}", pbStatement.ddl().statement());
                continue;
            }

            _logger->warn("unsupported statement type in user query: {}", (int) pbStatement.type());
        }


        std::vector<std::string> keyColsVec(
            _plan.keyColumns().begin(),
            _plan.keyColumns().end()
        );
        
        for (auto &event: queryEvents) {
            auto query = std::make_shared<Query>();
            
            query->setTimestamp(0);
            query->setDatabase(event.database());
            query->setStatement(event.statement());
            
            if (!event.isDML()) {
                _logger->error("DDL statement is not supported yet: {}", event.statement());
                continue;
            }
            
            event.buildRWSet(keyColsVec);
            
            query->readSet().insert(
                query->readSet().end(),
                event.readSet().begin(), event.readSet().end()
            );
            query->writeSet().insert(
                query->writeSet().end(),
                event.writeSet().begin(), event.writeSet().end()
            );

            {
                ColumnSet readColumns;
                ColumnSet writeColumns;
                event.columnRWSet(readColumns, writeColumns);
                query->readColumns().insert(readColumns.begin(), readColumns.end());
                query->writeColumns().insert(writeColumns.begin(), writeColumns.end());
            }
            
            *transaction << query;
        }
        
        return std::move(transaction);
    }
    
    void StateChanger::loadBackup(const std::string &dbName, const std::string &fileName) {
        _logger->info("loading database backup from {}...", fileName);

        if (_backupLoader == nullptr) {
            throw std::runtime_error("backup loader is not configured");
        }

        _backupLoader->loadBackup(dbName, fileName);
    }
}
