//
// Created by cheesekun on 8/29/22.
//

#include <algorithm>
#include <sstream>

#include <fmt/color.h>

#include <bison_parser.h>
#include <SQLParser.h>
#include <SQLParserResult.h>

#include "StateLogWriter.hpp"
#include "cluster/RowCluster.hpp"

#include "StateChanger.hpp"
#include "base/TaskExecutor.hpp"

namespace ultraverse::state::v2 {
    
    const std::string StateChanger::QUERY_TAG_STATECHANGE = "/* STATECHANGE_QUERY */ ";
    
    StateChanger::StateChanger(DBHandlePool<mariadb::DBHandle> &dbHandlePool, const StateChangePlan &plan):
        _logger(createLogger("StateChanger")),
        _dbHandlePool(dbHandlePool),
        _mode(OperationMode::NORMAL),
        _plan(plan),
        _intermediateDBName(fmt::format("ult_intermediate_{}", (int) time(nullptr))), // FIXME
        _reader(plan.stateLogPath(), plan.stateLogName()),
        _columnGraph(std::make_unique<ColumnDependencyGraph>()),
        _tableGraph(std::make_unique<TableDependencyGraph>()),
        _keyRanges(std::make_shared<std::map<std::string, std::vector<std::shared_ptr<StateRange>>>>()),
        _columnSetHashes(std::make_shared<std::vector<size_t>>()),
        _context(new StateChangeContext),
        _ddlTxnId(0),
        _ddlTxnProcessedId(0)
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
                        if (range->GetRange()->size() > 0) {
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
            // cols.clear();
        
            for (auto &i: query->itemSet()) {
                const auto vec = StateUserQuery::SplitDBNameAndTableName(i.name);
                if (vec.size() != 2) {
                    continue;
                }
                cols.emplace_back(std::make_tuple(i.name, vec[0], vec[1], i));
            }
        
            std::function<void(StateItem &)> walkWhereSet = [this, &walkWhereSet, &resultCols, &cols](StateItem &w) {
                if (!w.name.empty()) {
                    const auto &vec = StateUserQuery::SplitDBNameAndTableName(w.name);
                    if (vec.size() != 2) {
                        return;
                    }
                    for (auto &c: cols) {
                        // 테이블명은 다르고 column명이 동일할때 후보임
                        auto x = std::get<1>(c);
                        if (std::get<1>(c) != vec[0] && std::get<2>(c) == vec[1]) {
                            _logger->trace("adding column {} as candidate", vec[1]);
                            resultCols.emplace_back(std::get<0>(c), *std::get<3>(c).MakeRange());
                        }
                    }
                } else {
                    for (auto &subItem: w.arg_list) {
                        walkWhereSet(subItem);
                    }
                }
            };

            for (auto &w: query->whereSet()) {
                walkWhereSet(w);
            }
        }
    
        return resultCols;
    }
    
    
    void StateChanger::prepare() {
        _mode = OperationMode::PREPARE;
        
        std::mutex mutex;
        TaskExecutor taskExecutor(8);
        StateLogWriter stateLogWriter(_plan.stateLogPath(), _plan.stateLogName());
        createIntermediateDB();
        
        std::queue<std::shared_ptr<std::promise<int>>> taskQueue;
        
        if (!_plan.dbDumpPath().empty()) {
            loadBackup(_intermediateDBName, _plan.dbDumpPath());
        }
        
        _reader.open();
        
        while (_reader.next()) {
            auto transactionHeader = _reader.txnHeader();
            auto transaction = _reader.txnBody();
            auto gid = transactionHeader->gid;
            auto flags = transactionHeader->flags;
            _logger->trace("read gid {}; flags {}", gid, flags);
            
            if (gid < _plan.startGid()) {
                _logger->trace("skipping gid {} / {}", gid, _plan.startGid());
                continue;
            }
            
            if (_plan.endGid() != 0 && gid > _plan.endGid()) {
                break;
            }
    
            auto task = taskExecutor.post<int>([this, &mutex, &stateLogWriter, transaction, gid, flags]() {
                if (!isTransactionRelatedToPlan(transaction)) {
                    _logger->trace("skipping transaction #{}", gid);
                    return 0;
                }
        
                if (flags & Transaction::FLAG_CONTAINS_DDL) {
                    std::scoped_lock scopedLock(mutex);
                    processDDLTransaction(transaction);
            
                    auto dbHandleLease = _dbHandlePool.take();
                    auto &dbHandle = dbHandleLease.get();
            
                    dbHandle.executeQuery("use " + _intermediateDBName);
                    dbHandle.executeQuery("BEGIN");
                    for (auto &query: transaction->queries()) {
                        if (query->database() == _plan.dbName()) {
                            dbHandle.executeQuery(query->statement());
                        }
    
                        
                        updatePrimaryKeys(dbHandle, query->timestamp());
                        updateForeignKeys(dbHandle, query->timestamp());
    
                        bool isTableGraphChanged =
                            _tableGraph->addRelationship(_context->foreignKeys);
                        
                        if (isTableGraphChanged) {
                            _logger->info("updating table dependency graph");
                            stateLogWriter << *_tableGraph;
                        }
                    }
                    dbHandle.executeQuery("COMMIT");
                } else {
                    for (auto &query: transaction->queries()) {
                        std::scoped_lock scopedLock(mutex);
                        bool isColumnGraphChanged =
                            _columnGraph->add(query->readSet(), READ, _context->foreignKeys) ||
                            _columnGraph->add(query->writeSet(), WRITE, _context->foreignKeys);
                        
                        bool isTableGraphChanged =
                            _tableGraph->addRelationship(query->readSet(), query->writeSet());
                
                        if (isColumnGraphChanged) {
                            _logger->info("updating column dependency graph");
                            stateLogWriter << *_columnGraph;
                        }
                        
                        if (isTableGraphChanged) {
                            _logger->info("updating table dependency graph");
                            stateLogWriter << *_tableGraph;
                        }
                    }
    
                    if (isTransactionRelatedToCluster(transaction)) {
                        expandClusterMap(_rowCluster, *transaction, CLUSTER_EXPAND_FLAG_STRICT | CLUSTER_EXPAND_FLAG_WILDCARD);
                    }
                }
    
                return 0;
            });
            
            taskQueue.emplace(std::move(task));
        }
        
        while (!taskQueue.empty()) {
            _logger->trace("{} task(s) left", taskQueue.size());
            auto task = std::move(taskQueue.front());
            auto future = task->get_future();
            future.wait();
            taskQueue.pop();
        }
        
        for (auto &pair: _rowCluster.keyMap()) {
            _logger->info("MERGING CLUSTER: {}", pair.first);
            _rowCluster.mergeCluster(pair.first);
        }
        
        for (auto &pair: _rowCluster.keyMap()) {
            for (auto &cluster: pair.second) {
                _logger->info("{}: WHERE {}", pair.first, cluster->MakeWhereQuery(pair.first));
            }
        }
        
        stateLogWriter << _rowCluster;
        
        dropIntermediateDB();
        taskExecutor.shutdown();
    }

    void StateChanger::start() {
        _mode = OperationMode::NORMAL;
        
        TaskExecutor taskExecutor(8);
        std::queue<std::shared_ptr<std::promise<int>>> taskQueue;
        
        auto stateLogWriter = _plan.writeStateLog() ?
            std::make_unique<StateLogWriter>(_plan.stateLogPath(), fmt::format("{}_statechange_{}", _plan.stateLogName(), (int) time(nullptr))) :
            nullptr;
        gid_t outGid = 0;
        
        auto writeStateLog = [&stateLogWriter, &outGid](std::shared_ptr<Transaction> &source) {
            if (stateLogWriter == nullptr) {
                return;
            }
            
            Transaction dst(*source);
            dst.setGid(outGid++);
            
            *stateLogWriter << dst;
        };
        
        
        _logger->info("loading column dependency graph");
        _reader >> *_columnGraph;
        _reader >> *_tableGraph;
        
        _logger->info("loading row cluster");
        _reader >> _rowCluster;
        
        // _hashWatcher = std::make_unique<HashWatcher>(_plan.binlogPath(), _plan.stateLogName() + ".index", _intermediateDBName);
        
        createIntermediateDB();
    
        if (!_plan.dbDumpPath().empty()) {
            loadBackup(_intermediateDBName, _plan.dbDumpPath());
        }
    
        _logger->info("opening state log");
        _reader.open();
        
        _isRunning = true;
        
        auto addTransaction = [this, &taskExecutor, &taskQueue] (std::shared_ptr<Transaction> transaction) {
            auto node = _stateGraph->addTransaction(transaction);
            if (node.second) {
                if (transaction->flags() & Transaction::FLAG_CONTAINS_DDL) {
                    _ddlTxnId = transaction->gid();
                }
                
                auto task = taskExecutor.post<int>([this, node = std::move(node)]() {
                    this->processNode(node.first);
                    return 0;
                });
                
                taskQueue.push(std::move(task));
            }
        };
        
        while (_reader.next()) {
            auto transactionHeader = _reader.txnHeader();
            auto gid = transactionHeader->gid;
            auto flags = transactionHeader->flags;
            auto transaction = _reader.txnBody();
            _logger->trace("read gid {}; flags {}", gid, flags);
            
            if (_plan.isRollbackGid(transactionHeader->gid)) {
                _logger->info("rollback target found");
                _rollbackTarget = _reader.txnBody();
                
                for (const auto &keyColumn: _plan.keyColumns()) {
                    auto &ranges = (*_keyRanges)[keyColumn];
                    auto newRanges = _rowCluster.getKeyRangeOf(*_rollbackTarget, keyColumn, _context->foreignKeys);
                    ranges.insert(
                        ranges.end(),
                        newRanges.begin(), newRanges.end()
                    );
                }
                
                for (auto &query: _rollbackTarget->queries()) {
                    _columnSetHashes->push_back(std::hash<ColumnSet>{}(query->writeSet()));
                    
                    // FIXME
                    if (query->type() == Query::INSERT || query->type() == Query::DELETE) {
                        _tableGraph->addRelationship(query->writeSet(), query->writeSet());
                    }
                }
            }
            
            if (_plan.isRollbackGid(transactionHeader->gid) || _plan.hasUserQuery(transactionHeader->gid)) {
                _isClusterReady = true;
                _clusterCondvar.notify_all();
            }
    
            // FIXME: gid == rollbackGid시 스킵
            writeStateLog(transaction);
            
            if (!isTransactionRelatedToPlan(transaction)) {
                _logger->trace("skipping transaction #{}", gid);
                continue;
            }
            
            if (flags & Transaction::FLAG_CONTAINS_DDL) {
                processDDLTransaction(transaction);
            }
            
            while (_ddlTxnProcessedId < _ddlTxnId) {
                using namespace std::chrono_literals;
                _logger->debug("{} / {}", _ddlTxnProcessedId, _ddlTxnId);
                std::this_thread::sleep_for(100ms);
            }
    
            if (_plan.hasUserQuery(transactionHeader->gid)) {
                auto userQuery = std::move(loadUserQuery(_plan.userQueries()[transactionHeader->gid]));
                _logger->info("executing user-provided query");
    
                for (const auto &keyColumn: _plan.keyColumns()) {
                    auto &ranges = (*_keyRanges)[keyColumn];
                    auto newRanges = _rowCluster.getKeyRangeOf(*userQuery, keyColumn, _context->foreignKeys);
                    ranges.insert(
                        ranges.end(),
                        newRanges.begin(), newRanges.end()
                    );
                }
                
                addTransaction(userQuery);
                writeStateLog(userQuery);
            }
            
            addTransaction(transaction);
        }
        
        while (!taskQueue.empty()) {
            auto task = std::move(taskQueue.front());
            auto future = task->get_future();
            future.wait();
            taskQueue.pop();
        }
        
        for (auto &pair: _rowCluster2.keyMap()) {
            _logger->info("MERGING CLUSTER: {}", pair.first);
            _rowCluster2.mergeCluster(pair.first);
        }
        
        if (_hashWatcher != nullptr) {
            _hashWatcher->stop();
        }
        
        _logger->trace("== REPLAY FINISHED ==");
        
        std::stringstream queryBuilder;
        queryBuilder << fmt::format("USE {};\n", _plan.dbName());
        queryBuilder << fmt::format("SET AUTOCOMMIT = FALSE;\n");
        
        queryBuilder << "BEGIN;\n";
        
        std::map<
            std::string,                    // table name
            std::map<
                std::string,                // fk name (OR로 묶음)
                std::vector<std::string>    // WHERE
            >
        > result;
        
        for (auto it: _rowCluster2.keyMap()) {
            auto vec = StateUserQuery::SplitDBNameAndTableName(it.first);
            auto tableName = vec[0];
            auto columnName = vec[1];
            auto fkName = RowCluster::resolveForeignKey(it.first, _context->foreignKeys);
            
            auto &ranges = (*_keyRanges)[fkName];
    
            if (_hashWatcher != nullptr && _hashWatcher->isHashMatched(tableName)) {
                continue;
            }
            
            if (ranges.empty()) {
                continue;
            }
            
            if (fkName != it.first) {
                auto vec2 = StateUserQuery::SplitDBNameAndTableName(fkName);
                auto fkTableName = vec2[0];
                auto fkColumnName = vec2[1];
    
                std::string where;
                where += "(";
                for (auto &keyRange: ranges) {
                    where += keyRange->MakeWhereQuery(columnName);
                    where += " OR ";
                }
                where = where.substr(0, where.size() - 4);
                where += ")";
                
                result[tableName][fkName].push_back(where);
            } else {
                // TODO: alias
                std::string where;
                where += "(";
                for (auto &keyRange: ranges) {
                    where += keyRange->MakeWhereQuery(columnName);
                    where += " OR ";
                }
                where = where.substr(0, where.size() - 4);
                where += ")";
                
                result[tableName][fkName].push_back(where);
            }
        }
        
        
        for (auto &pair: result) {
            auto &table = pair.first;
            auto &foreignKeys = pair.second;
            
            if (_changedTables.find(table) == _changedTables.end() || foreignKeys.empty()) {
                continue;
            }
            
            std::string where;
            
            for (auto &foreignKey: foreignKeys) {
                where += "(";
                for (const auto &range: foreignKey.second) {
                    where += range;
                    where += " OR ";
                }
                
                where = where.substr(0, where.size() - 4);
                
                where += ")";
                where += " AND ";
            }
            
            where = where.substr(0, where.size() - 5);
            
            queryBuilder << fmt::format("REPLACE INTO {} SELECT * FROM {}.{} WHERE {};\n", table, _intermediateDBName, table, where);
        }
    
        
        queryBuilder << "COMMIT;\n";
        
        queryBuilder << fmt::format("SET FOREIGN_KEY_CHECKS = TRUE;\n\n");
    
        _logger->trace("TODO: EXECUTE QUERY:\n{}", queryBuilder.str());
    
        taskExecutor.shutdown();
    }
    
    bool StateChanger::isQueryRelatedWithKeyColumns(Query &query) {
        const auto &keyColumns = _plan.keyColumns();
        const std::string tableName = StateUserQuery::SplitDBNameAndTableName(
            *(query.writeSet().begin())
        )[0];
        
        bool hasKeyColumn = std::find_if(keyColumns.begin(), keyColumns.end(), [&tableName](const auto &column) {
            return StateUserQuery::SplitDBNameAndTableName(column)[0] == tableName;
        }) == keyColumns.end();
        
        auto resolveFKAlias = [this](const std::string &column) {
            return RowCluster::resolveForeignKey(
                RowCluster::resolveAliasName(_rowCluster.aliasMap(), column),
                _context->foreignKeys
            );
        };
        
        // TODO: foreign key / alias 해결해야 함
        auto isRWSetContains = [&resolveFKAlias, &query](const std::string &column) {
            auto findFn = [&resolveFKAlias, &column] (const std::string &column2) {
                return resolveFKAlias(column2) == column;
            };
            
            return (
                std::find_if(query.readSet().begin(), query.readSet().end(), findFn) == query.readSet().end() ||
                std::find_if(query.writeSet().begin(), query.writeSet().end(), findFn) == query.writeSet().end()
            );
        };
        
        bool usingEntireKeyColumn = std::find_if_not(keyColumns.begin(), keyColumns.end(), isRWSetContains) == keyColumns.end();
        
        auto tableDependencies = _tableGraph->getDependencies(tableName);
        std::set<std::string> keyColumnDependencies;
    
        {
            decltype(keyColumnDependencies) tmp;
            std::copy_if(
                keyColumns.begin(), keyColumns.end(),
                std::inserter(tmp, tmp.begin()),
                [&tableDependencies](const auto &column) {
                    return std::find(tableDependencies.begin(), tableDependencies.end(),
                                     StateUserQuery::SplitDBNameAndTableName(column)[0]) != tableDependencies.end();
                }
            );
            
            std::transform(
                tmp.begin(), tmp.end(),
                std::inserter(keyColumnDependencies, keyColumnDependencies.begin()),
                [](const auto &column) {
                    return StateUserQuery::SplitDBNameAndTableName(column)[0];
                }
            );
        }
    
        if (keyColumnDependencies.empty()) {
            if (hasKeyColumn) {
                // 1) 테이블이 클러스터컬럼을 보유하고 있으면서, 자신 이외의 그 어떤 클러스터컬럼 테이블에 대해서도 의존하고 있지 않은 경우
                return usingEntireKeyColumn;
            } else {
                // 2) 테이블이 클러스터컬럼을 보유하고 있지 않으면서, 자신 이외의 그 어떤 클러스터컬럼 테이블에 대해서도 의존하고 있지 않은 경우
                // 만일 이 테이블을 Write Set으로 지니고 있는 쿼리가 있없 경우, 이 쿼리는 키값이 { } (empty set) 이다. 즉, 클러스터키값을 가지고 있지 않아도 되는 쿼리다.
                // (왜냐면 이 테이블은 변화가 없으므로 이 테이블은 마치 constant 상수 취급을 해도 되기 때문이다).
                //
                // 이 테이블을 Write Set으로 지니고 있는 쿼리가 있을 경우, 이 쿼리의 키값은 * (wild card)가 된다.
                // (왜냐면 그러한 쿼리가 상태전환쿼리로 선택될 경우, 그 쿼리가 변경하는 테이블 row에 대한 클러스터 배정이 불가능하기 때문이다)
        
                return _tableGraph->hasPeerDependencies(tableName);
            }
        } else if (keyColumnDependencies.size() <= 1) {
            // 3) 외부 1개의 클러스터컬럼 테이블에 대한 의존 테이블일 경우
            // 이 쿼리는 자신이 의존하고 있는 source 테이블의 클러스터컬럼에 대한 키값을 사용해야 한다. 만약 사용하지 않을 경우, 이 쿼리의 키값은 * (wild card)가 된다.
    
            const auto &dependencyTable = *keyColumnDependencies.begin();
            std::vector<std::string> filteredKeyColumns;
            std::copy_if(
                keyColumns.begin(), keyColumns.end(),
                std::back_inserter(filteredKeyColumns),
                [&dependencyTable](const std::string &column) {
                    return column.rfind(dependencyTable, 0) == 0;
                }
            );
            
            return std::find_if_not(
                filteredKeyColumns.begin(), filteredKeyColumns.end(),
                isRWSetContains
            ) != keyColumns.end();
        } else {
            // 이 쿼리는 모든 다중 클러스터컬럼들에 대한 키값을 사용해야 한다
            // (구체적으로, INSERT 절에서 이 모든 다중클러스터컬럼들에 대하여 상수값을 집어넣거나, 또는 WHERE절에서 각 클러스터키에 대해 정하는 구문들이 AND로 연속해서 묶여야 한다).
            // 만일 단 한개의 클러스터컬럼이라도 키값을 사용하지 않는다면, 이 쿼리의 키값은 * (wild card)가 된다.
            return usingEntireKeyColumn;
        }
        
        return false;
    }
    
    void StateChanger::expandClusterMap(RowCluster &rowCluster, Transaction &transaction, int flags) {
        std::unordered_map<std::string, std::shared_ptr<StateRange>> clusterMap;
        bool containsUnrelatedQuery = false;
        
        std::function<void(StateItem &)> walkStateItem = [this, flags, &walkStateItem, &clusterMap, &rowCluster](StateItem &stateItem) {
            if (!stateItem.name.empty()) {
                auto &keyColumns = _plan.keyColumns();
                
                auto resolvedName = RowCluster::resolveForeignKey(stateItem.name, _context->foreignKeys);
                auto tmp = stateItem;
                tmp.name = resolvedName;
    
                _clusterMutex3.lock();
                auto resolvedAlias = RowCluster::resolveAlias(tmp, rowCluster.aliasMap());
                _clusterMutex3.unlock();
                
                if (std::find(keyColumns.begin(), keyColumns.end(), resolvedAlias.name) != keyColumns.end()) {
                    assert(resolvedAlias.condition_type == EN_CONDITION_NONE);
                    assert(resolvedAlias.function_type != FUNCTION_NONE);
                    
                    auto stateRange = std::make_shared<StateRange>();
        
                    if (!(flags & CLUSTER_EXPAND_FLAG_DONT_EXPAND)) {
                        switch (resolvedAlias.function_type) {
                            case FUNCTION_EQ:
                                stateRange->SetValue(resolvedAlias.data_list[0], true);
                                break;
                            case FUNCTION_NE:
                                stateRange->SetValue(resolvedAlias.data_list[0], false);
                                break;
                            case FUNCTION_GT:
                                stateRange->SetBegin(resolvedAlias.data_list[0], false);
                                break;
                            case FUNCTION_GE:
                                stateRange->SetBegin(resolvedAlias.data_list[0], true);
                                break;
                            case FUNCTION_LT:
                                stateRange->SetEnd(resolvedAlias.data_list[0], false);
                                break;
                            case FUNCTION_LE:
                                stateRange->SetEnd(resolvedAlias.data_list[0], true);
                                break;
                            case FUNCTION_BETWEEN:
                                stateRange->SetBetween(resolvedAlias.data_list[0], resolvedAlias.data_list[1]);
                                break;
                            default:
                                break;
                        }
                        
                        _logger->trace(
                            "RowCluster: expanding range of {} => (WHERE {})",
                            resolvedAlias.name,
                            stateRange->MakeWhereQuery(resolvedAlias.name)
                        );
                    }

                   
                    if (clusterMap[resolvedAlias.name] == nullptr) {
                        clusterMap[resolvedAlias.name] = std::make_shared<StateRange>();
                    }
                    
                    clusterMap[resolvedAlias.name] = StateRange::OR(*clusterMap[resolvedAlias.name], *stateRange);
                    // FK
                    
                    if (flags & CLUSTER_EXPAND_FLAG_INCLUDE_FK) {
                        auto stateRange2 = std::make_shared<StateRange>();
    
                        if (!(flags & CLUSTER_EXPAND_FLAG_DONT_EXPAND)) {
                            switch (stateItem.function_type) {
                                case FUNCTION_EQ:
                                    stateRange2->SetValue(stateItem.data_list[0], true);
                                    break;
                                case FUNCTION_NE:
                                    stateRange2->SetValue(stateItem.data_list[0], false);
                                    break;
                                case FUNCTION_GT:
                                    stateRange2->SetBegin(stateItem.data_list[0], false);
                                    break;
                                case FUNCTION_GE:
                                    stateRange2->SetBegin(stateItem.data_list[0], true);
                                    break;
                                case FUNCTION_LT:
                                    stateRange2->SetEnd(stateItem.data_list[0], false);
                                    break;
                                case FUNCTION_LE:
                                    stateRange2->SetEnd(stateItem.data_list[0], true);
                                    break;
                                case FUNCTION_BETWEEN:
                                    stateRange2->SetBetween(stateItem.data_list[0], stateItem.data_list[1]);
                                    break;
                                default:
                                    break;
                            }
                        }
    
                        if (clusterMap[stateItem.name] == nullptr) {
                            clusterMap[stateItem.name] = std::make_shared<StateRange>();
                        }
                        
                        clusterMap[stateItem.name] = StateRange::OR(*clusterMap[stateItem.name], *stateRange2);
                    }
                }
            }
            
            for (auto &subStateItem: stateItem.arg_list) {
                walkStateItem(subStateItem);
            }
        };
        
        if (!_plan.keyColumns().empty()) {
            for (auto &query: transaction.queries()) {
                
                for (auto &stateItem: query->whereSet()) {
                    walkStateItem(stateItem);
                }
                
                for (auto &stateItem: query->itemSet()) {
                    walkStateItem(stateItem);
                }
                
                if (true) {
                    // TODO: UPDATE 지원해야 하나?
                    for (auto &aliasPair: _plan.columnAliases()) {
                        auto alias = std::find_if(query->itemSet().begin(), query->itemSet().end(), [&aliasPair](auto &item) {
                            return item.name == aliasPair.first;
                        });
                        
                        if (alias == query->itemSet().end()) {
                            continue;
                        }
                        
                        auto real = std::find_if(query->itemSet().begin(), query->itemSet().end(), [&aliasPair](auto &item) {
                            return item.name == aliasPair.second;
                        });
    
                        if (real == query->itemSet().end()) {
                            continue;
                        }
    
                        _logger->trace("adding alias: {} ({}) => {} ({})", alias->name, alias->MakeRange()->MakeWhereQuery(alias->name), real->name, real->MakeRange()->MakeWhereQuery(real->name));
                        {
                            std::scoped_lock scopedLock(_clusterMutex3);
                            rowCluster.addAlias(aliasPair.first, *alias, *real);
                        }
                    }
                }
    
                {
                    std::scoped_lock scopedLock(_clusterMutex3);
                    if (!isQueryRelatedWithKeyColumns(*query)) {
                        containsUnrelatedQuery = true;
                    }
                }
            }
        } else {
            // FIXME
            return;
        }
    
        const auto &keyColumns = _plan.keyColumns();
        
        if (flags & CLUSTER_EXPAND_FLAG_WILDCARD && containsUnrelatedQuery) {
            for (const auto &keyColumn: keyColumns) {
                rowCluster.setWildcard(keyColumn, true);
            }
        }
        
        if (flags & CLUSTER_EXPAND_FLAG_STRICT) {
            if (clusterMap.size() != keyColumns.size() ||
                std::find_if(clusterMap.begin(), clusterMap.end(), [&keyColumns](const auto &pair) {
                    return std::find(keyColumns.begin(), keyColumns.end(), pair.first) != keyColumns.end();
                }) == clusterMap.end()
            ) {
                return;
            }
        }
        
        
        if ((flags & CLUSTER_EXPAND_FLAG_DONT_EXPAND)) {
            std::scoped_lock scopedLock(_clusterMutex2);
            
            for (auto &pair: clusterMap) {
                rowCluster.addKey(pair.first);
            }
        } else {
            std::scoped_lock scopedLock(_clusterMutex2);
            
            for (auto &pair: clusterMap) {
                rowCluster.addKeyRange(pair.first, pair.second);
            }
        }
    }
    
    bool StateChanger::isTransactionRelatedToPlan(std::shared_ptr<Transaction> transaction) const {
        for (auto &query: transaction->queries()) {
            if (query->database() == _plan.dbName()) {
                return true;
            }
        }
        
        return false;
    }
    
    bool StateChanger::isTransactionRelatedToCluster(std::shared_ptr<Transaction> transaction) const {
        return std::find(_plan.skipGids().begin(), _plan.skipGids().end(), transaction->gid()) == _plan.skipGids().end();
    }
    
    void StateChanger::processDDLTransaction(std::shared_ptr<Transaction> transaction) {
        static const std::vector<int16_t> RENAME_TOKEN = {SQL_RENAME, SQL_TABLE, SQL_IDENTIFIER};
        
        for (auto &query: transaction->queries()) {
            if (query->database() != _plan.dbName()) {
                continue;
            }
            
            auto when = query->timestamp();
            
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
            
            auto namingHistory = _context->findTable(prevTableName, when);
            if (namingHistory != nullptr) {
                namingHistory->addRenameHistory(newTableName, when);
            } else {
                auto history = std::make_shared<NamingHistory>(prevTableName);
                history->addRenameHistory(newTableName, when);
                
                _context->tables.emplace_back(history);
            }
        }
    }
    
    void StateChanger::processNode(uint64_t nodeIdx) {
        _logger->trace("[#{}] thread created", nodeIdx);
        auto node = _stateGraph->getTxnNode(nodeIdx);
    
        using namespace std::chrono_literals;
        
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
    
            {
                std::unique_lock clusterLock(_clusterMutex);
        
                if (node->transaction->gid() > _plan.lowestGidAvailable() && !_isClusterReady) {
                    _clusterCondvar.wait(clusterLock, [this]() { return _isClusterReady; });
                }
        
                clusterLock.unlock();
            }
    
    
    
            { // @with(dbHandleLease);
                _logger->trace("[#{}->#{}] leasing dbHandle", nodeIdx, node->nodeIdx);
                auto dbHandleLease = _dbHandlePool.take();
                auto &dbHandle = dbHandleLease.get();
    
                __node__processTransaction(
                    nodeIdx, node->nodeIdx,
                    node->transaction,
                    dbHandle
                );
            } // @release(dbHandleLease);
            
            NEXT_TRANSACTION:
            _stateGraph->removeTransaction(node->nodeIdx);
            node = node->next();
        }
    
        _logger->trace("[#{}] thread end", nodeIdx);
    }
    
    void StateChanger::__node__processTransaction(
        uint64_t rootNodeId,
        uint64_t nodeId,
        std::shared_ptr<Transaction> transaction,
        mariadb::DBHandle &dbHandle
    ) {
        const bool isTargetTransaction = _plan.isRollbackGid(transaction->gid());
        
        if (_hashWatcher != nullptr && isTargetTransaction) {
            dbHandle.executeQuery("use " + _intermediateDBName);
            dbHandle.executeQuery(fmt::format("/* ULTRAVERSE_HASHWATCHER_START_{} */ CREATE TABLE __ULTRAVERSE_HASHWATCHER_START__( dummy INTEGER )", _intermediateDBName));
    
            _logger->info("starting hashwatcher");
            _hashWatcher->start();
        }
        
        _logger->info("[#{}->#{}] replaying transaction", rootNodeId, nodeId);
        dbHandle.executeQuery("use " + _intermediateDBName);
        dbHandle.executeQuery("BEGIN");
    
        for (auto &query: transaction->queries()) {
            if (query->database() != _plan.dbName()) {
                goto NEXT_QUERY;
            }
           
            {
                const auto readSetHash = std::hash<ColumnSet>{}(query->readSet());
                const auto writeSetHash = std::hash<ColumnSet>{}(query->writeSet());
                
                const bool isDDL = query->flags() & Query::FLAG_IS_DDL;
                const bool isRelatedWithCluster = RowCluster::isQueryRelated(*_keyRanges, *query, _context->foreignKeys, _rowCluster.aliasMap());
                const bool isRelatedWithColumnGraph = std::any_of(_columnSetHashes->begin(), _columnSetHashes->end(), [this, &readSetHash, &writeSetHash](size_t hashA) {
                    return _columnGraph->isRelated(hashA, readSetHash) || _columnGraph->isRelated(hashA, writeSetHash);
                });
                const bool needsForceExecution =
                    (transaction->gid() < _plan.lowestGidAvailable()) ||
                    (transaction->flags() & Transaction::FLAG_FORCE_EXECUTE);
                
                const bool skipQuery =
                    (_mode == OperationMode::NORMAL && isTargetTransaction)   ||
                    !(needsForceExecution                               ||
                    isDDL                                               ||
                    (isRelatedWithCluster && isRelatedWithColumnGraph));
    
    
                if (isRelatedWithColumnGraph && query->type() == Query::INSERT) {
                    auto tableName = StateUserQuery::SplitDBNameAndTableName(*query->writeSet().begin())[0];
                    if (_context->autoIncrements.find(tableName) == _context->autoIncrements.end()) {
                        _context->autoIncrements.insert({ tableName, getAutoIncrement(dbHandle, tableName) });
                    }
                    
                    if (!skipQuery) {
                        int64_t autoIncrement = _context->autoIncrements[tableName];
                        
                        _logger->trace("{}: setting AUTO_INCREMENT to {}", tableName, autoIncrement);
                        setAutoIncrement(dbHandle, tableName, autoIncrement);
                    }
    
                    _context->autoIncrements[tableName]++;
                }
                
                if (skipQuery) {
                    if (!isRelatedWithCluster) {
                        _logger->trace("query skipped: not related with cluster: {}", query->statement());
                    } else if (!isRelatedWithColumnGraph) {
                        _logger->trace("query skipped: not related with column graph: {}", query->statement());
                    }
                
                    goto NEXT_QUERY;
                }
                
                if (!isDDL && _hashWatcher != nullptr) {
                    const std::string tableName = StateUserQuery::SplitDBNameAndTableName(
                        *query->writeSet().begin())[0];

                    if (transaction->gid() <= _plan.lowestGidAvailable()) {
                        if (query->isAfterHashPresent(tableName)) {
                            _hashWatcher->setHash(tableName, query->afterHash(tableName));
                        }
                    } else if (transaction->gid() > _plan.lowestGidAvailable()) {
                        const bool isHashMatched = _hashWatcher->isHashMatched(tableName);
    
                        if (isHashMatched) {
                            _logger->info("hash matched: skipping query\n\n");
                            goto NEXT_QUERY;
                        } else {
                            if (query->isAfterHashPresent(tableName)) {
                                query->afterHash(tableName).hexdump();
                                _hashWatcher->queue(tableName, query->afterHash(tableName));
                            }
                        }
                    }
                }
                
                if (query->flags() & Query::FLAG_IS_CONTINUOUS) {
                    // _logger->trace("query marked as continuous");
                    goto NEXT_QUERY;
                }
                
                __node__replayQuery(rootNodeId, nodeId, query, dbHandle);
                
                if (!isDDL && transaction->gid() > _plan.lowestGidAvailable() || transaction->flags() & Transaction::FLAG_FORCE_EXECUTE) {
                    std::scoped_lock lock(_changedTablesMutex);
                    const std::string tableName = StateUserQuery::SplitDBNameAndTableName(
                        *query->writeSet().begin())[0];
                    
                    _changedTables.insert(tableName);
                }
            }
        
            NEXT_QUERY: ;
        }
        
        // _logger->trace("[#{}->#{}] finalizing transaction", rootNodeId, nodeId);
        dbHandle.executeQuery("COMMIT");
        // _logger->debug("[#{}->#{}] releasing dbHandle", rootNodeId, nodeId);
    
        if (!(_mode == OperationMode::NORMAL && isTargetTransaction) && isTransactionRelatedToCluster(transaction)) {
            expandClusterMap(_rowCluster2, *transaction, CLUSTER_EXPAND_FLAG_INCLUDE_FK | CLUSTER_EXPAND_FLAG_DONT_EXPAND);
        }
        
        if (transaction->flags() & Transaction::FLAG_CONTAINS_DDL) {
            _ddlTxnProcessedId = std::max(_ddlTxnProcessedId, transaction->gid());
        }
    }
    
    void StateChanger::__node__replayQuery(
        uint64_t rootNodeId,
        uint64_t nodeId,
        std::shared_ptr<Query> query,
        mariadb::DBHandle &dbHandle
    ) {
        auto statement = QUERY_TAG_STATECHANGE + query->statement();
        _logger->trace("[#{}->#{}] executing query: (timestamp={}) {}", rootNodeId, nodeId, query->timestamp(), query->statement());
        // TODO: 제거하기로 함
        if (dbHandle.executeQuery("SET foreign_key_checks=0") != 0) {
            _logger->warn("[#{}->#{}] failed to turn off foreign key constraint", rootNodeId, nodeId);
        }
        if (dbHandle.executeQuery(fmt::format("SET TIMESTAMP={}", query->timestamp())) != 0) {
            _logger->warn("[#{}->#{}] failed to set timestamp", rootNodeId, nodeId);
        }
        
        if (dbHandle.executeQuery(statement) != 0) {
            // TODO: 계속 실행해 나가야함
            _logger->error("[#{}->#{}] query execution failed: {}", rootNodeId, nodeId,
                           mysql_error(dbHandle));
            dbHandle.executeQuery("ROLLBACK");
            throw std::runtime_error(mysql_error(dbHandle));
        }
        
        if (query->flags() & Query::FLAG_IS_DDL) {
            _logger->info("[#{}->#{}] updating foreign key..", rootNodeId, nodeId);
            updatePrimaryKeys(dbHandle, query->timestamp());
            updateForeignKeys(dbHandle, query->timestamp());
        }
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
    
    void StateChanger::dropIntermediateDB() {
         _logger->info("dropping intermediate database: {}", _intermediateDBName);
        
        auto query = QUERY_TAG_STATECHANGE + fmt::format("DROP DATABASE IF EXISTS {}", _intermediateDBName);
        auto dbHandleLease = _dbHandlePool.take();
        auto &dbHandle = dbHandleLease.get();
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot drop intermediate database: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
        dbHandle.executeQuery("COMMIT");
    }
    
    void StateChanger::updatePrimaryKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp) {
        std::scoped_lock _lock(_context->contextLock);
    
        // TODO: LOCK
        std::unordered_set<std::string> primaryKeys;
    
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{}' AND CONSTRAINT_NAME = 'PRIMARY'", _intermediateDBName);
    
    
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch foreign key information: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
    
        MYSQL_RES *result = mysql_store_result(dbHandle);
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result)) != nullptr) {
            std::string table(row[0]);
            std::string column(row[1]);
       
            _logger->trace("updatePrimaryKeys(): adding primary key: {}.{}", table, column);
        
            primaryKeys.insert(table + "." + column);
        }
        mysql_free_result(result);
    
        _context->primaryKeys = primaryKeys;
    }
    
    void StateChanger::updateForeignKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp) {
        std::scoped_lock _lock(_context->contextLock);
    
        // TODO: LOCK
        std::vector<ForeignKey> foreignKeys;
        
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{}' AND REFERENCED_TABLE_NAME IS NOT NULL", _intermediateDBName);
        
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch foreign key information: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
        
        MYSQL_RES *result = mysql_store_result(dbHandle);
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result)) != nullptr) {
            std::string fromTable(row[0]);
            std::string fromColumn(row[1]);
            
            std::string toTable(row[2]);
            std::string toColumn(row[3]);
            
            // _logger->trace("updateForeignKeys(): adding foreign key: {}.{} -> {}.{}", fromTable, fromColumn, toTable, toColumn);
            
            ForeignKey foreignKey {
                _context->findTable(fromTable, timestamp), fromColumn,
                _context->findTable(toTable, timestamp), toColumn
            };
            
            foreignKeys.push_back(foreignKey);
        }
        mysql_free_result(result);
        
        _context->foreignKeys = foreignKeys;
    }
    
    int64_t StateChanger::getAutoIncrement(mariadb::DBHandle &dbHandle, std::string table) {
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
                        _intermediateDBName, table);
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch auto increment: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
        
        MYSQL_RES *result = mysql_store_result(dbHandle);
        bool isAvailable = mysql_num_rows(result) != 0;
        
        if (!isAvailable) {
            return -1;
        }
        
        MYSQL_ROW row = mysql_fetch_row(result);
        
        // TODO: support for 64-bit integer
        return std::atoi(row[0]);
    }
    
    void StateChanger::setAutoIncrement(mariadb::DBHandle &dbHandle, std::string table, int64_t value) {
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("ALTER TABLE {} AUTO_INCREMENT = {}", table, value);
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot set auto increment: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
    }
}
