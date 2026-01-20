//
// Created by cheesekun on 6/27/23.
//

#include <algorithm>
#include <atomic>
#include <future>
#include <sstream>

#include <execution>

#include <fmt/color.h>

#include "GIDIndexWriter.hpp"
#include "StateLogWriter.hpp"
#include "cluster/RowCluster.hpp"

#include "cluster/StateCluster.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/StringUtil.hpp"

#include "StateChanger.hpp"

#include "StateChangeReport.hpp"
#include "StateChangeReplayPlan.hpp"


namespace {
    using ultraverse::state::v2::ColumnSet;
    using ultraverse::state::v2::ForeignKey;
    using ultraverse::state::v2::Query;
    using ultraverse::state::v2::StateCluster;
    using ultraverse::state::v2::Transaction;

    struct ColumnRW {
        ColumnSet read;
        ColumnSet write;
    };

    ColumnRW collectColumnRW(const std::shared_ptr<Transaction> &transaction) {
        ColumnRW rw;
        for (const auto &query : transaction->queries()) {
            if (query->flags() & Query::FLAG_IS_DDL) {
                continue;
            }
            rw.read.insert(query->readColumns().begin(), query->readColumns().end());
            rw.write.insert(query->writeColumns().begin(), query->writeColumns().end());
        }
        return rw;
    }

    bool isColumnRelated(const std::string &columnA,
                         const std::string &columnB,
                         const std::vector<ForeignKey> &foreignKeys) {
        const auto resolvedA = ultraverse::state::v2::RowCluster::resolveForeignKey(columnA, foreignKeys);
        const auto resolvedB = ultraverse::state::v2::RowCluster::resolveForeignKey(columnB, foreignKeys);

        const auto vecA = ultraverse::utility::splitTableName(resolvedA);
        const auto vecB = ultraverse::utility::splitTableName(resolvedB);

        const auto &tableA = vecA.first;
        const auto &colA = vecA.second;
        const auto &tableB = vecB.first;
        const auto &colB = vecB.second;

        if (tableA.empty() || tableB.empty()) {
            return resolvedA == resolvedB;
        }

        if (tableA == tableB && (colA == colB || colA == "*" || colB == "*")) {
            return true;
        }

        if (colA == "*" || colB == "*") {
            const auto it = std::find_if(foreignKeys.begin(), foreignKeys.end(), [&tableA, &tableB](const ForeignKey &fk) {
                return (
                    (fk.fromTable->getCurrentName() == tableA && fk.toTable->getCurrentName() == tableB) ||
                    (fk.fromTable->getCurrentName() == tableB && fk.toTable->getCurrentName() == tableA)
                );
            });

            if (it != foreignKeys.end()) {
                if (colA == "*" && colB == "*") {
                    return true;
                }

                if (colA == "*") {
                    return it->fromColumn == colB || it->toColumn == colB;
                }

                if (colB == "*") {
                    return it->fromColumn == colA || it->toColumn == colA;
                }
            }
        }

        return false;
    }

    bool columnSetsRelated(const ColumnSet &taintedWrites,
                           const ColumnSet &candidateColumns,
                           const std::vector<ForeignKey> &foreignKeys) {
        if (taintedWrites.empty() || candidateColumns.empty()) {
            return false;
        }

        for (const auto &tainted : taintedWrites) {
            for (const auto &column : candidateColumns) {
                if (isColumnRelated(tainted, column, foreignKeys)) {
                    return true;
                }
            }
        }

        return false;
    }

    bool hasKeyColumnItems(const std::shared_ptr<Transaction> &transaction,
                           const StateCluster &cluster,
                           const ultraverse::state::v2::RelationshipResolver &resolver) {
        for (const auto &query : transaction->queries()) {
            if (query->flags() & Query::FLAG_IS_DDL) {
                continue;
            }

            for (const auto &item : query->readSet()) {
                if (cluster.isKeyColumnItem(resolver, item)) {
                    return true;
                }
            }

            for (const auto &item : query->writeSet()) {
                if (cluster.isKeyColumnItem(resolver, item)) {
                    return true;
                }
            }
        }

        return false;
    }
}

namespace ultraverse::state::v2 {
    
    void StateChanger::makeCluster() {
        StateChangeReport report(StateChangeReport::MAKE_CLUSTER, _plan);
        
        StateCluster rowCluster(_plan.keyColumns());

        _columnGraph = std::make_unique<ColumnDependencyGraph>();
        _tableGraph = std::make_unique<TableDependencyGraph>();
        
        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 1000);
        
        GIDIndexWriter gidIndexWriter(_plan.stateLogPath(), _plan.stateLogName());
        
        std::mutex graphLock;

        createIntermediateDB();

        if (!_plan.dbDumpPath().empty()) {
            auto load_backup_start = std::chrono::steady_clock::now();
            loadBackup(_intermediateDBName, _plan.dbDumpPath());
            
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle->get(), 0);
            updateForeignKeys(dbHandle->get(), 0);
            auto load_backup_end = std::chrono::steady_clock::now();
            
            std::chrono::duration<double> time = load_backup_end - load_backup_start;
            _logger->info("LOAD BACKUP END: {}s elapsed", time.count());
        } else {
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle->get(), 0, _plan.dbName());
            updateForeignKeys(dbHandle->get(), 0, _plan.dbName());
        }

        _tableGraph->addRelationship(_context->foreignKeys);
        
        _reader->open();
        
        auto phase_main_start = std::chrono::steady_clock::now();
        _logger->info("makeCluster(): building cluster");
        
        const bool useRowAlias = !_plan.columnAliases().empty();
        if (useRowAlias) {
            _logger->info("makeCluster(): row-alias enabled; processing sequentially");
            while (_reader->nextHeader()) {
                auto header = _reader->txnHeader();
                auto pos = _reader->pos() - sizeof(TransactionHeader);

                _reader->nextTransaction();
                auto transaction = _reader->txnBody();

                gidIndexWriter.append(pos);

                if (!transaction->isRelatedToDatabase(_plan.dbName())) {
                    _logger->trace("skipping transaction #{} because it is not related to database {}",
                                   transaction->gid(), _plan.dbName());
                    continue;
                }

                if (relationshipResolver.addTransaction(*transaction)) {
                    cachedResolver.clearCache();
                }

                rowCluster.insert(transaction, cachedResolver);

                for (auto &query: transaction->queries()) {
                    if (query->flags() & Query::FLAG_IS_PROCCALL_QUERY) {
                        // FIXME: 프로시저 쿼리 어케할려고?
                        continue;
                    }
                    if (query->flags() & Query::FLAG_IS_DDL) {
                        _logger->warn(
                            "DDL statement found in transaction #{}, but this version of ultraverse does not support DDL statement yet",
                            transaction->gid());
                        _logger->warn("DDL query will be skipped: {}", query->statement());
                        continue;
                    }

                    bool isColumnGraphChanged = false;
                    if (!query->readColumns().empty()) {
                        isColumnGraphChanged |= _columnGraph->add(query->readColumns(), READ, _context->foreignKeys);
                    }
                    if (!query->writeColumns().empty()) {
                        isColumnGraphChanged |= _columnGraph->add(query->writeColumns(), WRITE, _context->foreignKeys);
                    }

                    bool isTableGraphChanged =
                        _tableGraph->addRelationship(query->readColumns(), query->writeColumns());

                    if (isColumnGraphChanged) {
                        _logger->info("updating column dependency graph");
                    }

                    if (isTableGraphChanged) {
                        _logger->info("updating table dependency graph");
                    }
                }
            }
        } else {
            TaskExecutor taskExecutor(_plan.threadNum());
            std::queue<std::shared_ptr<std::promise<int>>> tasks;

            while (_reader->nextHeader()) {
                auto header = _reader->txnHeader();
                auto pos = _reader->pos() - sizeof(TransactionHeader);

                _reader->nextTransaction();
                auto transaction = _reader->txnBody();

                gidIndexWriter.append(pos);

                auto promise = taskExecutor.post<int>(
                    [this, &graphLock, &rowCluster, &cachedResolver, transaction]() {
                        if (!transaction->isRelatedToDatabase(_plan.dbName())) {
                            _logger->trace("skipping transaction #{} because it is not related to database {}",
                                           transaction->gid(), _plan.dbName());
                            return 0;
                        }

                        rowCluster.insert(transaction, cachedResolver);

                        for (auto &query: transaction->queries()) {
                            if (query->flags() & Query::FLAG_IS_PROCCALL_QUERY) {
                                // FIXME: 프로시저 쿼리 어케할려고?
                                continue;
                            }
                            if (query->flags() & Query::FLAG_IS_DDL) {
                                _logger->warn(
                                    "DDL statement found in transaction #{}, but this version of ultraverse does not support DDL statement yet",
                                    transaction->gid());
                                _logger->warn("DDL query will be skipped: {}", query->statement());
                                continue;
                            }

                            std::scoped_lock _lock(graphLock);

                            bool isColumnGraphChanged = false;
                            if (!query->readColumns().empty()) {
                                isColumnGraphChanged |= _columnGraph->add(query->readColumns(), READ, _context->foreignKeys);
                            }
                            if (!query->writeColumns().empty()) {
                                isColumnGraphChanged |= _columnGraph->add(query->writeColumns(), WRITE, _context->foreignKeys);
                            }

                            bool isTableGraphChanged =
                                _tableGraph->addRelationship(query->readColumns(), query->writeColumns());

                            if (isColumnGraphChanged) {
                                _logger->info("updating column dependency graph");
                            }

                            if (isTableGraphChanged) {
                                _logger->info("updating table dependency graph");
                            }
                        }

                        return 0;
                    });

                tasks.emplace(std::move(promise));
            }

            while (!tasks.empty()) {
                _logger->info("make_cluster(): {} tasks remaining", tasks.size());
                tasks.front()->get_future().wait();
                tasks.pop();
            }

            taskExecutor.shutdown();
        }
        
        rowCluster.merge();
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        _logger->info("make_cluster(): main phase {}s", _phase2Time);
        
        _logger->info("make_cluster(): saving cluster..");
        _clusterStore->save(rowCluster);

        {
            StateLogWriter graphWriter(_plan.stateLogPath(), _plan.stateLogName());
            graphWriter << *_columnGraph;
            graphWriter << *_tableGraph;
        }
        
        if (_plan.dropIntermediateDB()) {
            dropIntermediateDB();
        }
        
        if (!_plan.reportPath().empty()) {
            report.writeToJSON(_plan.reportPath());
        }
    }
    
    void StateChanger::bench_prepareRollback() {
        StateChangeReport report(StateChangeReport::PREPARE_AUTO, _plan);
        
        StateCluster rowCluster(_plan.keyColumns());
        
        std::mutex gidListMutex;
        
        std::atomic<size_t> replayGidCount{0};
        size_t totalCount = 0;
        
        size_t totalQueryCount = 0;
        size_t replayQueryCount = 0;
        
        std::map<gid_t, size_t> queryCounts;
        
        std::set<gid_t> selectedGids;
        std::set<gid_t> replayGids;
        
        
        {
            _logger->info("prepare(): loading cluster");
            _clusterStore->load(rowCluster);
            _logger->info("prepare(): loading cluster end");
        }
        
        /* PHASE 0: walk state log */
        
        _reader->open();
        _reader->seek(0);
        
        while (_reader->nextHeader()) {
            const auto &header = _reader->txnHeader();
            
            _reader->nextTransaction();
            const auto &body = _reader->txnBody();
            
            if (!body->isRelatedToDatabase(_plan.dbName())) {
                continue;
            }
            
            size_t queryCount = body->queries().size();
            totalQueryCount += queryCount;

            auto gid = header->gid;
            
            queryCounts.emplace(gid, queryCount);
            
            totalCount++;
        }
        
        const auto calculateReplayQueryCount = [&]() {
            size_t count = 0;

            for (const auto &pair: queryCounts) {
                if (replayGids.find(pair.first) != replayGids.end()) {
                    continue;
                }

                count += pair.second;
            }

            return count;
        };
        
        gid_t i = 0;
        while (i < totalCount) {
            selectedGids.emplace(i);
            
            for (const auto &cluster: rowCluster.clusters()) {
                if (_plan.keyColumns().find(utility::toLower(cluster.first)) == _plan.keyColumns().end()) {
                    continue;
                }
                
                std::for_each(std::execution::par_unseq, cluster.second.write.begin(), cluster.second.write.end(), [&](const auto &writePair) {
                    const auto &range = writePair.first;
                    const auto &gids = writePair.second;
                    
                    if (gids.find(i) == gids.end()) {
                        return;
                    }
                    
                    
                    const auto &depRangeIt = std::find_if(std::execution::par_unseq, cluster.second.read.begin(), cluster.second.read.end(), [&range](const auto &pair) {
                        return pair.first == range || StateRange::isIntersects(pair.first, range);
                    });
                    
                    if (depRangeIt == cluster.second.read.end()) {
                        return;
                    }
                    
                    {
                        std::scoped_lock<std::mutex> _lock(gidListMutex);
                        replayGids.insert(
                            depRangeIt->second.begin(), depRangeIt->second.end()
                        );
                    }
                });
            }
            
            replayGids.erase(i);
            
            replayQueryCount = calculateReplayQueryCount();

            // _logger->trace("bench_prepareRollback(): #{}: {} / {} transactions will be replayed ({}%)", i, replayGids.size(), totalCount, ((double) replayGids.size() / totalCount) * 100);
            
            if (i % 8 == 0) {
                _logger->trace("bench_prepareRollback(): #{}: {} / {} queries will be replayed ({}%)", i, replayQueryCount, totalQueryCount, ((double) replayQueryCount / totalQueryCount) * 100);
            }
            
            
            if (replayQueryCount < (size_t) (totalQueryCount * (1.0 - _plan.autoRollbackRatio()))) {
                break;
            }
            
            i++;
        }
        

        
        replayGidCount = selectedGids.size();
        
        
        report.bench_setRollbackGids(selectedGids);
        report.setReplayGidCount(replayGidCount);
        report.setTotalCount(totalCount);
        report.setExecutionTime(_phase2Time);
        
        report.bench_setReplayQueryCount(replayQueryCount);
        report.bench_setTotalQueryCount(totalQueryCount);
        
        _logger->info("benchAutoRollback(): {} / {} transactions will be replayed ({}%)", replayGidCount.load(), totalCount, ((double) replayGidCount / totalCount) * 100);
        _logger->info("benchAutoRollback(): {} / {} queries will be replayed ({}%)", replayQueryCount, totalQueryCount, ((double) replayQueryCount / totalQueryCount) * 100);
        // rowCluster.describe();
        
        if (!_plan.reportPath().empty()) {
            report.writeToJSON(_plan.reportPath());
        }
    }
    
    void StateChanger::prepare() {
        StateChangeReport report(StateChangeReport::PREPARE, _plan);
        
        TaskExecutor taskExecutor(_plan.threadNum());
        StateCluster rowCluster(_plan.keyColumns());
        
        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 1000);
        
        std::vector<gid_t> replayGids;
        replayGids.reserve(1024);
        StateChangeReplayPlan replayPlan;
        size_t totalCount = 0;
        
        createIntermediateDB();
        report.setIntermediateDBName(_intermediateDBName);
        
        
        if (!_plan.dbDumpPath().empty()) {
            auto load_backup_start = std::chrono::steady_clock::now();
            loadBackup(_intermediateDBName, _plan.dbDumpPath());
            
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle->get(), 0);
            updateForeignKeys(dbHandle->get(), 0);
            auto load_backup_end = std::chrono::steady_clock::now();
            
            std::chrono::duration<double> time = load_backup_end - load_backup_start;
            _logger->info("LOAD BACKUP END: {}s elapsed", time.count());
            report.setSQLLoadTime(time.count());
        } else {
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle->get(), 0, _plan.dbName());
            updateForeignKeys(dbHandle->get(), 0, _plan.dbName());
        }
        
        {
            _logger->info("prepare(): loading cluster");
            _clusterStore->load(rowCluster);
            _logger->info("prepare(): loading cluster end");
        }
        
        _reader->open();
        _reader->seek(0);
        
        auto phase_main_start = std::chrono::steady_clock::now();
        
        std::vector<std::future<gid_t>> replayTasks;
        replayTasks.reserve(1024);
        constexpr size_t kReplayFutureFlushSize = 10000;

        ColumnSet columnTaint;
        
        auto flushReplayTasks = [&replayTasks, &replayGids]() {
            for (auto &future : replayTasks) {
                gid_t gid = future.get();
                if (gid != UINT64_MAX) {
                    replayGids.push_back(gid);
                }
            }
            replayTasks.clear();
        };
        
        while (_reader->nextHeader()) {
            auto header = _reader->txnHeader();
            gid_t gid = header->gid;
            
            totalCount++;
            
            // _logger->info("read gid {}", gid);

            _reader->nextTransaction();
            auto transaction = _reader->txnBody();

            if (!transaction->isRelatedToDatabase(_plan.dbName())) {
                _logger->trace("skipping transaction #{} because it is not related to database {}",
                               transaction->gid(), _plan.dbName());
                continue;
            }

            if (relationshipResolver.addTransaction(*transaction)) {
                cachedResolver.clearCache();
            }

            ColumnRW txnColumns = collectColumnRW(transaction);
            ColumnSet txnAccess = txnColumns.read;
            txnAccess.insert(txnColumns.write.begin(), txnColumns.write.end());

            if (_plan.isRollbackGid(gid) || _plan.hasUserQuery(gid)) {
                auto nextGid = transaction->gid() + 1;
                bool shouldRevalidate = !_plan.isRollbackGid(nextGid) && !_plan.hasUserQuery(nextGid);

                if (_plan.isRollbackGid(transaction->gid())) {
                    rowCluster.addRollbackTarget(transaction, cachedResolver, shouldRevalidate);
                    columnTaint.insert(txnColumns.write.begin(), txnColumns.write.end());
                }

                if (_plan.hasUserQuery(transaction->gid())) {
                    auto userQuery = std::move(loadUserQuery(_plan.userQueries()[transaction->gid()]));
                    userQuery->setGid(transaction->gid());
                    userQuery->setTimestamp(transaction->timestamp());
                    rowCluster.addPrependTarget(transaction->gid(), userQuery, cachedResolver);
                    replayPlan.userQueries.emplace(transaction->gid(), *userQuery);

                    ColumnRW prependColumns = collectColumnRW(userQuery);
                    columnTaint.insert(prependColumns.write.begin(), prependColumns.write.end());
                }

                if (_plan.performBenchInsert()) {
                    auto promise = taskExecutor.post<gid_t>([gid, &rowCluster]() {
                        if (rowCluster.shouldReplay(gid)) {
                            return gid;
                        }
                        return UINT64_MAX;
                    });
                    replayTasks.emplace_back(promise->get_future());
                    if (replayTasks.size() >= kReplayFutureFlushSize) {
                        flushReplayTasks();
                    }
                }

                continue;
            }

            bool isColumnDependent = columnSetsRelated(columnTaint, txnAccess, _context->foreignKeys);
            if (isColumnDependent) {
                columnTaint.insert(txnColumns.write.begin(), txnColumns.write.end());
            }

            if (!isColumnDependent) {
                continue;
            }

            bool hasKeyColumns = hasKeyColumnItems(transaction, rowCluster, cachedResolver);
            if (!hasKeyColumns) {
                std::promise<gid_t> immediate;
                auto future = immediate.get_future();
                immediate.set_value(gid);
                replayTasks.emplace_back(std::move(future));
                if (replayTasks.size() >= kReplayFutureFlushSize) {
                    flushReplayTasks();
                }
                continue;
            }

            auto promise = taskExecutor.post<gid_t>([gid, &rowCluster]() {
                if (rowCluster.shouldReplay(gid)) {
                    return gid;
                }
                return UINT64_MAX;
            });
            replayTasks.emplace_back(promise->get_future());
            if (replayTasks.size() >= kReplayFutureFlushSize) {
                flushReplayTasks();
            }
        }
        
        
        if (!replayTasks.empty()) {
            flushReplayTasks();
        }

        taskExecutor.shutdown();
        
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        std::sort(replayGids.begin(), replayGids.end());
        replayGids.erase(std::unique(replayGids.begin(), replayGids.end()), replayGids.end());
        replayPlan.gids = replayGids;

        report.setReplayGidCount(replayGids.size());
        report.setTotalCount(totalCount);
        report.setExecutionTime(_phase2Time);
        
        auto replayCountValue = replayGids.size();
        _logger->info("prepare(): {} / {} transactions will be replayed ({}%)", replayCountValue, totalCount, ((double) replayCountValue / totalCount) * 100);
        // rowCluster.describe();
        
        _logger->info("prepare(): main phase {}s", _phase2Time);
        
        if (_plan.dropIntermediateDB()) {
            dropIntermediateDB();
        }
        
        std::string replaceQuery = rowCluster.generateReplaceQuery(_plan.dbName(), "__INTERMEDIATE_DB__", cachedResolver);
        _logger->debug("TODO: execute query: \n{}", replaceQuery);
        report.setReplaceQuery(replaceQuery);

        const std::string replayPlanPath = _plan.stateLogPath() + "/" + _plan.stateLogName() + ".ultreplayplan";
        _logger->info("prepare(): writing replay plan to {}", replayPlanPath);
        replayPlan.save(replayPlanPath);

        if (_closeStandardFds) {
            close(STDIN_FILENO);
            close(STDOUT_FILENO);
            close(STDERR_FILENO);
        }
        
        if (!_plan.reportPath().empty()) {
            report.writeToJSON(_plan.reportPath());
        }
    }

}
