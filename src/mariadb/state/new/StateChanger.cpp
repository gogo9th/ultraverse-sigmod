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
#include "cluster/RowCluster.hpp"

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
    
    std::vector<std::string> &StateChangePlan::keyColumns() {
        return _keyColumns;
    }
    
    StateChanger::StateChanger(DBHandlePool<mariadb::DBHandle> &dbHandlePool, const StateChangePlan &plan):
        _logger(createLogger("StateChanger")),
        _dbHandlePool(dbHandlePool),
        _plan(plan),
        _intermediateDBName(fmt::format("ult_intermediate_{}", (int) time(nullptr))), // FIXME
        _reader(plan.stateLogPath()),
        _context(new StateChangeContext)
    {
        _stateGraph = std::make_unique<StateGraphBoost>(_context);
    }
    
    /**
     * StateTable.cc로부터 이식
     */
    std::string StateChanger::findCandidateColumn() {
        _logger->info("reading state log");
        _reader.open();
        
        std::unordered_map<std::string, std::vector<StateRange>> candidate_maps;
    
        while (_reader.next()) {
            auto transactionHeader = _reader.txnHeader();
            auto gid = transactionHeader->gid;
            auto flags = transactionHeader->flags;
            _logger->trace("read gid {}; flags {}", gid, flags);
        
            if (transactionHeader->gid == _plan.rollbackGid()) {
                _rollbackTarget = _reader.txnBody();
            }
        
            auto transaction = _reader.txnBody();
            
            if (!isTransactionRelatedToPlan(transaction)) {
                _logger->trace("skipping transaction #{}", gid);
                continue;
            }
    
            for (auto &column: buildCandidateColumnList(transaction)) {
                candidate_maps[column.name].push_back(column.range);
            }
        }
    
        {
            double lowest_sigma = 0.0f;
            std::string lowest_name = "";
            for (const auto &c: candidate_maps) {
                // sigma 를 계산하여 가장 작은것이 후보임
                double sigma = 0.0f;
                for (auto &r: c.second) {
                    // 동일한 범위의 쿼리의 개수
                    double count = 0;
                    for (auto &i: c.second) {
                        auto range = StateRange::AND(i, r);
                        if (range.GetRange()->size() > 0) {
                            count += 1.0f;
                        }
                    }
            
                    sigma += std::pow(count, 2.0f);
                }
        
                if (lowest_name.size() == 0) {
                    lowest_sigma = sigma;
                    lowest_name = c.first;
                } else if (sigma < lowest_sigma) {
                    lowest_sigma = sigma;
                    lowest_name = c.first;
                }
            }
    
            if (lowest_name.empty()) {
                _logger->error("cannot find candidate column");
            } else {
                _logger->info("candidate column found: {}", lowest_name);
                
                for (auto &range: candidate_maps[lowest_name]) {
                    _logger->info("    (WHERE {})", range.MakeWhereQuery(lowest_name));
                }
            }
        }
    
    }
    
    std::vector<CandidateColumn>
    StateChanger::buildCandidateColumnList(std::shared_ptr<Transaction> transaction) const {
        // insert, update, delete 쿼리를 대상으로
        // 쿼리에 컬럼 정보가 있는지 확인하고, 컬럼정보가 있을때만 분석함
        // item_set 은 외부 쿼리에 사용한 컬럼 정보가 있고,
        // where_set 은 내부 쿼리에 사용한 컬럼 정보가 있음
        // 외부 쿼리와 내부 쿼리에 동일한 컬럼명이 있으면 후보 컬럼임
    
        std::vector<std::tuple<std::string, std::string, std::string, StateItem>> cols;
        std::vector<CandidateColumn> resultCols;
    
        // q->is_valid_query = 0;
        for (const auto &query: transaction->queries()) {
            cols.clear();
        
            for (auto &i: query->itemSet()) {
                if (i.condition_type == EN_CONDITION_NONE &&
                    i.function_type == FUNCTION_NONE) {
                    const auto vec = StateUserQuery::SplitDBNameAndTableName(i.name);
                    if (vec.size() != 2) {
                        continue;
                    }
                    cols.emplace_back(std::make_tuple(i.name, vec[0], vec[1], i));
                }
            }
        
            if (cols.size() > 1) {
                // 컬럼이 있는 경우만 컬럼 분석을 수행
                // 그 외 쿼리는 필요없음
                // q->is_valid_query = 1;
            }
        
            // FIXME: 이거 nested 된거에서 망가지지 않아?
            for (auto &w: query->whereSet()) {
                if (w.condition_type != EN_CONDITION_NONE &&
                    w.function_type == FUNCTION_NONE) {
                    // 실제 where 절 컬럼 목록
                    for (auto &a: w.arg_list) {
                        const auto &vec = StateUserQuery::SplitDBNameAndTableName(a.name);
                        if (vec.size() != 2) {
                            continue;
                        }
                        for (auto &c: cols) {
                            // 테이블명은 다르고 column명이 동일할때 후보임
                            if (std::get<1>(c) != vec[0] && std::get<2>(c) == vec[1]) {
                                _logger->trace("adding column {} as candidate", vec[1]);
                                resultCols.emplace_back(std::get<0>(c), std::get<3>(c).MakeRange());
                            }
                        }
                    }
                }
            }
            
            
            /*
            std::function<void(StateItem &)> visitExpr = [&visitExpr, &resultCols] (StateItem &expr) {
                if (expr.condition_type != EN_CONDITION_NONE) {
                    for (auto &subExpr: expr.arg_list) {
                        visitExpr(subExpr);
                    }
                } else {
                    resultCols.emplace_back(expr.name, expr.MakeRange());
                }
            };
            
            for (auto &expr: query->whereSet()) {
                visitExpr(expr);
            }
             */
        }
    
        return resultCols;
    }
    
    /**
     * TODO:
     *  - turn off foreign key check? (필요 없을듯)
     *  - lock table
     *  - swap table
     *  - unlock table
     *  - DB 이름을 바꾸거나, 특정 테이블을 옮기거나 해야 하는데 테이블 스왑의 경우 FK나 트리거 등을 드랍했다가 다시 설정해야 함
     *  - hash check
     *
     *  - Transaction에서 dbname.table 같은 식으로 R/W set 저장하게 해야 함
     */
    void StateChanger::start() {
        createIntermediateDB();
        
        _logger->info("reading state log");
        _reader.open();
        
        _isRunning = true;
        
        while (_reader.next()) {
            auto transactionHeader = _reader.txnHeader();
            auto gid = transactionHeader->gid;
            auto flags = transactionHeader->flags;
            _logger->trace("read gid {}; flags {}", gid, flags);
            
            
            if (transactionHeader->gid == _plan.rollbackGid()) {
                setRollbackTarget(_reader.txnBody());
            }
            
            auto transaction = _reader.txnBody();
            if (!isTransactionRelatedToPlan(transaction)) {
                _logger->trace("skipping transaction #{}", gid);
                continue;
            }
            
            if (flags & Transaction::FLAG_CONTAINS_DDL) {
                processDDLTransaction(transaction);
            }
            
            auto node = _stateGraph->addTransaction(transaction);
            if (node.second) {
                _executorThreads.emplace_back(&StateChanger::processNode, this, node.first);
            }
        }
        
        for (auto &thread: _executorThreads) {
            thread.join();
        }
    }
    
    void StateChanger::setRollbackTarget(std::shared_ptr<Transaction> transaction) {
        _rollbackTarget = std::move(transaction);
        
        std::function<void(StateItem &)> walkStateItem = [this, &walkStateItem](StateItem &stateItem) {
            auto &keyColumns = _plan.keyColumns();
            if (std::find(keyColumns.begin(), keyColumns.end(), stateItem.name) != keyColumns.end()) {
                assert(stateItem.condition_type == EN_CONDITION_NONE);
                assert(stateItem.function_type != FUNCTION_NONE);
                StateRange stateRange;
                
                switch (stateItem.function_type) {
                    case FUNCTION_EQ:
                        stateRange.SetValue(stateItem.data_list[0], true);
                        break;
                    case FUNCTION_NE:
                        stateRange.SetValue(stateItem.data_list[0], false);
                        break;
                    case FUNCTION_GT:
                        stateRange.SetBegin(stateItem.data_list[0], false);
                        break;
                    case FUNCTION_GE:
                        stateRange.SetBegin(stateItem.data_list[0], true);
                        break;
                    case FUNCTION_LT:
                        stateRange.SetEnd(stateItem.data_list[0], false);
                        break;
                    case FUNCTION_LE:
                        stateRange.SetEnd(stateItem.data_list[0], true);
                        break;
                    case FUNCTION_BETWEEN:
                        stateRange.SetBetween(stateItem.data_list[0], stateItem.data_list[1]);
                        break;
                    default:
                        break;
                }
                
                _logger->info("adding key range: {}", stateItem.name);
                _rowCluster.addKeyRange(stateItem.name, stateRange);
            }
            
            for (auto &subStateItem: stateItem.arg_list) {
                walkStateItem(subStateItem);
            }
        };
        
        if (!_plan.keyColumns().empty()) {
            for (auto &query: _rollbackTarget->queries()) {
                for (auto &stateItem: query->whereSet()) {
                    walkStateItem(stateItem);
                }
                
                for (auto &stateItem: query->itemSet()) {
                    walkStateItem(stateItem);
                }
            }
        } else {
            // FIXME
        }
        
        _isClusterReady = true;
    }
    
    bool StateChanger::isTransactionRelatedToPlan(std::shared_ptr<Transaction> transaction) const {
        for (auto &query: transaction->queries()) {
            if (query->database() == _plan.dbName()) {
                return true;
            }
        }
        
        return false;
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
    
    void StateChanger::processNode(uint64_t nodeIdx) {
        _logger->trace("[#{}] thread created", nodeIdx);
        auto node = _stateGraph->getTxnNode(nodeIdx);
    
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(100ms);
        
        std::unique_lock clusterLock(_clusterMutex);
        
        if (!_isClusterReady) {
            _clusterCondvar.wait(clusterLock, [this]() { return _isClusterReady; });
        }
        
        clusterLock.unlock();
        
        
        while (node != nullptr) {
            for (auto depIdx: node->dependencies) {
                if (!_stateGraph->getTxnNode(depIdx)->isProcessed) {
                    _logger->info("[#{}->#{}] waiting for dependencies: #{}", nodeIdx, node->nodeIdx, depIdx);
                    
                    while (!_stateGraph->getTxnNode(depIdx)->isProcessed) {
                        std::this_thread::sleep_for(100ms);
                    }
                }
            }
    
            if (node->isProcessed || !node->processLock.try_lock()) {
                _logger->trace("[#{}->#{}] this node is already processed by another thread", nodeIdx, node->nodeIdx);
                break;
            }
    
            { // @with(dbHandleLease);
                _logger->trace("[#{}->#{}] leasing dbHandle", nodeIdx, node->nodeIdx);
                auto dbHandleLease = _dbHandlePool.take();
                auto &dbHandle = dbHandleLease.get();
                _logger->info("[#{}->#{}] replaying transaction", nodeIdx, node->nodeIdx);
                dbHandle.executeQuery("use " + _intermediateDBName);
                dbHandle.executeQuery("BEGIN");
            
                for (auto &query: node->transaction->queries()) {
                    if (query->database() != _plan.dbName()) {
                        continue;
                    }
                    
                    {
                        std::scoped_lock<std::mutex> _hashLock(_stateHashMutex);
                        auto relatedTable = *query->writeSet().begin();
                        auto it = relatedTable.find('.');
    
                        if (it != std::string::npos) {
                            relatedTable.erase(it, relatedTable.size());
                        }
                        
        
                        if (!_stateHashMap[relatedTable].isInitialized()) {
                            _stateHashMap[relatedTable] = StateHash(query->beforeHash(relatedTable));
                            _logger->trace("[#{}->#{}] hash init: {}", nodeIdx, node->nodeIdx, relatedTable);
                            _stateHashMap[relatedTable].hexdump();
                        }
    
                        auto &hash = _stateHashMap[relatedTable];
                        auto beforeHash = hash;
                        
                        // FIXME
                        if (query->type() != Query::UNKNOWN && hash == query->afterHash(relatedTable)) {
                            _logger->trace("[#{}->#{}] skipping query: table hash is equal", nodeIdx, node->nodeIdx);
                            continue;
                        }
        
                        for (int i = 0; i < query->affectedRows(); i++) {
                            switch (query->type()) {
                                case Query::INSERT:
                                    hash += query->rowSet()[i];
                                    break;
                                case Query::DELETE:
                                    hash -= query->rowSet()[i];
                                    break;
                                case Query::UPDATE:
                                    hash -= query->rowSet()[i];
                                    hash += query->changeSet()[i];
                                    break;
                                default:
                                    break;
                            }
                        }
                        
                        
                        if (hash == query->afterHash(relatedTable)) {
                            // hash matches
                        } else {
                            // not matches
                        }
                    }
                    
                    if (!(query->flags() & Query::FLAG_IS_DDL) && !(_rowCluster & query)) {
                        _logger->trace("query is not related with key column(s); skipping");
                        continue;
                    }
            
                    auto statement = QUERY_TAG_STATECHANGE + query->statement();
                    _logger->trace("[#{}->#{}] executing query: (timestamp={}) {}", nodeIdx, node->nodeIdx, query->timestamp(), query->statement());
                    if (dbHandle.executeQuery(fmt::format("SET TIMESTAMP={}", query->timestamp())) != 0) {
                        _logger->warn("[#{}->#{}] failed to set timestamp", nodeIdx, node->nodeIdx);
                    }
                    
                    if (dbHandle.executeQuery(statement) != 0) {
                        _logger->error("[#{}->#{}] query execution failed: {}", nodeIdx, node->nodeIdx, mysql_error(dbHandle));
                        dbHandle.executeQuery("ROLLBACK");
                        throw std::runtime_error(mysql_error(dbHandle));
                    }
                }
    
                _logger->trace("[#{}->#{}] finalizing transaction", nodeIdx, node->nodeIdx);
                dbHandle.executeQuery("COMMIT");
                _logger->debug("[#{}->#{}] releasing dbHandle", nodeIdx, node->nodeIdx);
            } // @release(dbHandleLease);
            
            _stateGraph->removeTransaction(node->nodeIdx);
            node = node->next();
        }
    
        _logger->trace("[#{}] thread end", nodeIdx);
    }
    
    void StateChanger::createIntermediateDB() {
        _logger->info("creating intermediate database: {}", _intermediateDBName);
        
        auto query = QUERY_TAG_STATECHANGE + fmt::format("CREATE DATABASE IF NOT EXISTS {}", _intermediateDBName);
        auto dbHandleLease = _dbHandlePool.take();
        auto &dbHandle = dbHandleLease.get();
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot create intermediate database: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
        dbHandle.executeQuery("COMMIT");
    }
}
