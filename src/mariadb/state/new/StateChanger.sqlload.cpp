//
// Created by cheesekun on 12/7/22.
//

#include <stdexcept>

#include "mariadb/DBEvent.hpp"
#include "StateChanger.hpp"


namespace ultraverse::state::v2 {
    
    std::shared_ptr<Transaction> StateChanger::loadUserQuery(const std::string &path) {
        std::vector<std::string> queries;
        
        std::ifstream stream(path);
        std::string query;
        
        while (std::getline(stream, query, '\n')) {
            query.erase(
                std::remove(query.begin(), query.end(), '\r'),
                query.end()
            );
            
            if (!query.empty()) {
                queries.push_back(query);
            }
        }
        
        return std::move(parseUserQuery(queries));
    }
    
    std::shared_ptr<Transaction> StateChanger::parseUserQuery(const std::vector<std::string> &queries) {
        std::vector<mariadb::QueryEvent> queryEvents;
        auto transaction = std::make_shared<Transaction>();
        
        transaction->setXid(0);
        transaction->setGid(0);
        transaction->setTimestamp(0);
        
        transaction->setFlags(Transaction::FLAG_FORCE_EXECUTE);
        
        std::transform(
            queries.begin(), queries.end(),
            std::back_inserter(queryEvents),
            [this] (const std::string &queryString) {
                return mariadb::QueryEvent(_plan.dbName(), queryString, 0);
            }
        );
        
        std::vector<std::string> keyColsVec(
            _plan.keyColumns().begin(),
            _plan.keyColumns().end()
        );
        
        for (auto &event: queryEvents) {
            auto query = std::make_shared<Query>();
            
            query->setTimestamp(0);
            query->setDatabase(event.database());
            query->setStatement(event.statement());
            
            event.tokenize();
            
            if (!event.isDML()) {
                _logger->error("DDL statement is not supported yet: {}", event.statement());
                continue;
            }
            
            if (!event.parse()) {
                _logger->warn("cannot parse SQL statement: {}", event.statement());
                event.parseDDL(1);
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
