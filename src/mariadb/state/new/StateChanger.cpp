//
// Created by cheesekun on 8/29/22.
//

#include <algorithm>
#include <sstream>

#include <fmt/color.h>

#include <bison_parser.h>
#include <SQLParser.h>
#include <SQLParserResult.h>

#include "StateChanger.hpp"

namespace ultraverse::state::v2 {
    
    const std::string StateChanger::QUERY_TAG_STATECHANGE = "/* STATECHANGE_QUERY */ ";
    
    StateChangePlan::StateChangePlan() {
    
    }
    
    const std::string &StateChangePlan::dbName() const {
        return _dbName;
    }
    
    void StateChangePlan::setDBName(const std::string &dbName) {
        _dbName = dbName;
    }
    
    gid_t StateChangePlan::rollbackGid() const {
        return _rollbackGid;
    }
    
    void StateChangePlan::setRollbackGid(gid_t rollbackGid) {
        _rollbackGid = rollbackGid;
    }
    
    const std::string &StateChangePlan::userQueryPath() const {
        return _userQueryPath;
    }
    
    void StateChangePlan::setUserQueryPath(const std::string &userQueryPath) {
        _userQueryPath = userQueryPath;
    }
    
    const std::string &StateChangePlan::dbDumpPath() const {
        return _dbdumpPath;
    }
    
    void StateChangePlan::setDBDumpPath(const std::string &dbdumpPath) {
        _dbdumpPath = dbdumpPath;
    }
    
    const std::string &StateChangePlan::binlogPath() const {
        return _binlogPath;
    }
    
    void StateChangePlan::setBinlogPath(const std::string &binlogPath) {
        _binlogPath = binlogPath;
    }
    
    const std::string &StateChangePlan::stateLogPath() const {
        return _stateLogPath;
    }
    
    void StateChangePlan::setStateLogPath(const std::string &stateLogPath) {
        _stateLogPath = stateLogPath;
    }
    
    bool StateChangePlan::isDryRun() const {
        return _isDryRun;
    }
    
    void StateChangePlan::setDryRun(bool isDryRun) {
        _isDryRun = isDryRun;
    }
    
    StateChanger::StateChanger(const StateChangePlan &plan):
        _logger(createLogger("StateChanger")),
        _plan(plan),
        _intermediateDBName(fmt::format("ult_intermediate_{}", (int) time(nullptr))), // FIXME
        _reader(plan.stateLogPath()),
        _context(new StateChangeContext),
        _stateGraph(_context)
    {
    
    }
    
    void StateChanger::prepare() {
        _logger->info("reading state log");
        _reader.open();
        
        _isRunning = true;
        
        while (_reader.next()) {
            auto transactionHeader = _reader.txnHeader();
            auto gid = transactionHeader->gid;
            auto flags = transactionHeader->flags;
            _logger->trace("read gid {}; flags {}", gid, flags);
            
            if (transactionHeader->gid == _plan.rollbackGid()) {
                _rollbackTarget = _reader.txnBody();
            }
            
            if (flags & Transaction::FLAG_CONTAINS_DDL) {
                processDDLTransaction(_reader.txnBody());
            }
            
            auto node = _stateGraph.addTransaction(_reader.txnBody());
            if (node.second) {
                _executorThreads.emplace_back(&StateChanger::start, this, node.first);
            }
            
            // _stateGraph.dump();
        }
        
        
    }
    
    void StateChanger::processDDLTransaction(std::shared_ptr<Transaction> transaction) {
        static const std::vector<int16_t> RENAME_TOKEN = {SQL_RENAME, SQL_TABLE, SQL_IDENTIFIER};
        
        for (auto &query: transaction->queries()) {
            if (query->database() != _plan.dbName()) {
                continue;
            }
            
            std::vector<int16_t> tokens;
            std::vector<size_t> tokenPos;
            if (!hsql::SQLParser::tokenize(query->statement(), &tokens, &tokenPos)) {
                _logger->warn("processDDLTransaction(): invalid sql statement: {}", query->statement());
                continue;
            }
            
            if (tokens.size() < 5 ||
                std::search(
                    tokens.begin(), tokens.end(),
                    RENAME_TOKEN.begin(), RENAME_TOKEN.end()
                ) != tokens.begin()) {
                _logger->trace("not RENAME statement, skipping");
                continue;
            }
            
            
            std::string prevTableName;
            std::string newTableName;
            
            int i = 0;
            for (auto &token: tokens) {
                if (token == SQL_IDENTIFIER) {
                    std::string value;
                    if (i + 1 == tokens.size()) {
                        value = query->statement().substr(tokenPos[i]);
                    } else {
                        tokenPos[i + 1] - tokenPos[i];
                        value = query->statement().substr(tokenPos[i], tokenPos[i + 1] - tokenPos[i]);
                    }
                    
                    value.erase(std::remove_if(
                        value.begin(), value.end(),
                        [](auto c) { return c == ' '; }
                    ), value.end());
                    
                    if (prevTableName.empty()) {
                        prevTableName = value;
                    } else if (newTableName.empty()) {
                        newTableName = value;
                    }
                }
                i++;
            }
           
            if (prevTableName.size() == 0 || newTableName.size() == 0) {
                _logger->error("[StateChanger::MakeRenameHistory] unknown query failed [{}]", query->statement());
                break;
            }
            
            _logger->trace("[processDDLTransaction] [{}] {} -> {}", query->timestamp(), prevTableName, newTableName);
            
            bool isFound = false;
            for (auto &i: _context->renameHistoryMap) {
                if (i.second.back().name == prevTableName) {
                    i.second.emplace_back(RenameHistory { query->timestamp(), newTableName });
                    isFound = true;
                    break;
                }
            }
            if (!isFound) {
                _context->renameHistoryMap.emplace(prevTableName, std::list<RenameHistory>());
                _context->renameHistoryMap[prevTableName].emplace_back(RenameHistory { query->timestamp(), newTableName });
            }
          
        }
    
        for (auto &i: _context->renameHistoryMap) {
            i.second.sort([](const auto &a, const auto &b) {
                return a.time < b.time;
            });
        }
    }
    
    void StateChanger::explain() {
        /*
        std::stringstream planExplanation;
        if (_plan.dbDumpPath().empty()) {
            planExplanation << " - [!] START FROM SCRATCH\n";
        } else {
            planExplanation << fmt::format(" - Rollback database using backup file ({})\n", _plan.dbDumpPath());
        }
        
        planExplanation << " - Replay Transactions:\n";
        
        auto headList = _stateGraph.getTransactions();
        for (auto &x : headList) {
            auto *node = x;
            
            int level = 1;
            while (node != nullptr) {
                for (int i = 0; i < level * 2; i++) {
                    planExplanation << ' ';
                }
                if (_rollbackTarget->gid() == node->transaction->gid()) {
                     planExplanation << fmt::format(
                         fmt::emphasis::bold | fg(fmt::color::red),
                         "/> GID #{} (ROLLBACK TARGET; has {} queries)\n",
                         node->transaction->gid(),
                         node->transaction->queries().size()
                    );
                } else {
                    planExplanation << fmt::format(
                        "=> GID #{} (has {} queries)\n",
                        node->transaction->gid(),
                        node->transaction->queries().size()
                    );
                }
                node = node->next();
                level++;
            }
            planExplanation << "\n";
        }
        
        std::cout << planExplanation.str();
         */
    }
    
    void StateChanger::start(uint64_t nodeIdx) {
        _logger->trace("[#{}] thread created", nodeIdx);
        auto node = _stateGraph.getTxnNode(nodeIdx);
    
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(100ms);
        
        while (node != nullptr) {
            for (auto depIdx: node->dependencies) {
                if (!_stateGraph.getTxnNode(depIdx)->isProcessed) {
                    _logger->trace("[#{}->#{}] waiting for dependencies: #{}", nodeIdx, node->nodeIdx, depIdx);
                    
                    while (!_stateGraph.getTxnNode(depIdx)->isProcessed) {
                        std::this_thread::sleep_for(100ms);
                    }
                }
            }
    
            if (node->isProcessed) {
                break;
            }
            
            for (auto &query: node->transaction->queries()) {
                _logger->debug("[#{}->#{}] TODO: execute query: {}", nodeIdx, node->nodeIdx, query->statement());
            }
            
            _stateGraph.removeTransaction(node->nodeIdx);
            node = node->next();
        }
    
        _logger->trace("[#{}] thread end", nodeIdx);
        
        /*
        // TODO: statetable 로직 이식 (안그러면 테이블 이름 변경 등으로 인해 디펜던시 계산 제대로 안됨)
        // createIntermediateDB();
    
        auto headList = _stateGraph.getTransactions();
        for (auto &head: headList) {
            auto node = head;
            while (node != nullptr) {
                auto &transaction = node->transaction;
                for (auto &query: transaction->queries()) {
                    if (query->database() == "") {
                        continue;
                    }
    
                    auto statement = QUERY_TAG_STATECHANGE + query->statement();
                    _logger->debug("[#{}] TODO: execute {}", transaction->gid(), query->statement());
                    if (mysql_real_query(_dbHandle, statement.c_str(), statement.size()) != 0) {
                        // TODO:
                    }
                }
                node = node->next();
            }
        }
        */
    }
    
    void StateChanger::createIntermediateDB() {
        _logger->info("creating intermediate database: {}", _intermediateDBName);
        
        auto query = QUERY_TAG_STATECHANGE + fmt::format("CREATE DATABASE IF NOT EXISTS {}", _intermediateDBName);
        if (mysql_real_query(_dbHandle, query.c_str(), query.size()) != 0) {
            _logger->error("cannot create intermediate database: {}", mysql_error(_dbHandle));
            throw std::runtime_error(mysql_error(_dbHandle));
        }
    }
}
