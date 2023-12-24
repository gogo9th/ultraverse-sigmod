//
// Created by cheesekun on 12/7/22.
//

#include <unistd.h>
#include <sys/wait.h>

#include <fmt/format.h>

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
        
        /*
        for (auto &event: queryEvents) {
            auto query = std::make_shared<Query>();
            
            query->setTimestamp(0);
            query->setDatabase(event.database());
            query->setStatement(event.statement());
            
            event.tokenize();
            
            if (!event.isDML()) {
                _logger->error("DDL statement is not supported yet");
                continue;
            }
            
            if (!event.parse()) {
                _logger->warn("cannot parse SQL statement: {}", event.statement());
                event.parseDDL(1);
            }
            
            query->readSet().insert(
                event.readSet().begin(), event.readSet().end()
            );
            query->writeSet().insert(
                event.writeSet().begin(), event.writeSet().end()
            );
            
            query->itemSet().insert(
                query->itemSet().begin(),
                event.itemSet().begin(), event.itemSet().end()
            );
            query->whereSet().insert(
                query->whereSet().begin(),
                event.whereSet().begin(), event.whereSet().end()
            );
            
            *transaction << query;
        }
         */
        
        throw std::runtime_error("TODO: reimplement this");
        
        return std::move(transaction);
    }
    
    void StateChanger::loadBackup(const std::string &dbName, const std::string &fileName) {
        _logger->info("loading database backup from {}...", fileName);
        
        int fd = open(fileName.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("failed to load backup file");
        }
        
        auto pid = fork();
        if (pid == -1) {
            throw std::runtime_error("failed to fork process");
        }
       
        if (pid == 0) {
            dup2(fd, STDIN_FILENO);
            std::string password = "-p" + _plan.dbPassword();
            int retval = execl(
                "/usr/bin/mysql",
                "mysql",
                "-h", _plan.dbHost().c_str(),
                "-u", _plan.dbUsername().c_str(),
                password.c_str(),
                dbName.c_str(),
                nullptr
            );
            
            close(fd);
    
            if (retval == -1) {
                throw std::runtime_error(fmt::format("failed to execute mysql: {}", strerror(errno)));
            }
        } else {
            close(fd);
            
            int wstatus = 0;
            waitpid(pid, &wstatus, 0);
            
            if (!WIFEXITED(wstatus) || WEXITSTATUS(wstatus) != 0) {
                throw std::runtime_error(
                    fmt::format("failed to restore backup: WEXITSTATUS {}", WEXITSTATUS(wstatus))
                );
            }
        }
    }
}