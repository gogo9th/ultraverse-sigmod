//
// Created by cheesekun on 6/27/23.
//

#include <algorithm>
#include <atomic>
#include <cmath>
#include <future>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

#include <execution>

#include <fmt/color.h>

#include "GIDIndexWriter.hpp"
#include "StateLogWriter.hpp"
#include "analysis/TaintAnalyzer.hpp"
#include "cluster/StateCluster.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/StringUtil.hpp"
#include "StateChanger.hpp"

#include "StateChangeReport.hpp"
#include "StateChangeReplayPlan.hpp"


namespace {
    bool isTransactionInScope(const ultraverse::state::v2::StateChangePlan &plan,
                              const std::unordered_set<ultraverse::state::v2::gid_t> &skipGids,
                              ultraverse::state::v2::gid_t gid,
                              const ultraverse::state::v2::Transaction &transaction) {
        if (!transaction.isRelatedToDatabase(plan.dbName())) {
            return false;
        }
        if (plan.hasGidRange()) {
            if (gid < plan.startGid() || gid > plan.endGid()) {
                return false;
            }
        }
        if (!skipGids.empty() && skipGids.find(gid) != skipGids.end()) {
            return false;
        }
        return true;
    }

    std::vector<size_t> buildAutoRollbackIndices(size_t totalCount, double ratio) {
        std::vector<size_t> indices;
        if (totalCount == 0) {
            return indices;
        }

        if (ratio < 0.0) {
            ratio = 0.0;
        } else if (ratio > 1.0) {
            ratio = 1.0;
        }

        if (ratio == 0.0) {
            return indices;
        }

        auto targetCount = static_cast<size_t>(std::llround(static_cast<double>(totalCount) * ratio));
        if (targetCount == 0) {
            targetCount = 1;
        }
        if (targetCount > totalCount) {
            targetCount = totalCount;
        }

        indices.reserve(targetCount);
        for (size_t k = 0; k < targetCount; k++) {
            double centered = (static_cast<double>(k) + 0.5) * static_cast<double>(totalCount) /
                              static_cast<double>(targetCount);
            size_t idx = static_cast<size_t>(std::floor(centered));
            if (!indices.empty() && idx <= indices.back()) {
                idx = indices.back() + 1;
            }
            if (idx >= totalCount) {
                idx = totalCount - 1;
            }
            indices.push_back(idx);
        }

        return indices;
    }
}

namespace ultraverse::state::v2 {

    void StateChanger::makeCluster() {
        StateChangeReport report(StateChangeReport::MAKE_CLUSTER, _plan);

        StateCluster rowCluster(_plan.keyColumns(), _plan.keyColumnGroups());

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
        rowCluster.normalizeWithResolver(relationshipResolver);

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

    StateChanger::ReplayAnalysisResult StateChanger::analyzeReplayPlan(
        StateCluster &rowCluster,
        StateRelationshipResolver &relationshipResolver,
        CachedRelationshipResolver &cachedResolver,
        StateChangeReplayPlan *replayPlan,
        const std::function<bool(gid_t, size_t)> &isRollbackTarget,
        const std::function<std::optional<std::string>(gid_t)> &userQueryPath,
        const std::function<bool(gid_t)> &shouldRevalidateTarget) {
        TaskExecutor taskExecutor(_plan.threadNum());
        std::vector<std::future<gid_t>> replayTasks;
        replayTasks.reserve(1024);
        constexpr size_t kReplayFutureFlushSize = 10000;

        ReplayAnalysisResult result;
        std::unordered_map<gid_t, size_t> queryCounts;
        std::unordered_set<gid_t> skipGids(_plan.skipGids().begin(), _plan.skipGids().end());

        ColumnSet columnTaint;

        _reader->open();
        _reader->seek(0);

        auto flushReplayTasks = [&replayTasks, &result]() {
            for (auto &future : replayTasks) {
                gid_t gid = future.get();
                if (gid != UINT64_MAX) {
                    result.replayGids.push_back(gid);
                }
            }
            replayTasks.clear();
        };

        size_t candidateIndex = 0;
        bool pendingTargetCacheRefresh = false;

        while (_reader->nextHeader()) {
            auto header = _reader->txnHeader();
            gid_t gid = header->gid;

            _reader->nextTransaction();
            auto transaction = _reader->txnBody();

            if (!isTransactionInScope(_plan, skipGids, gid, *transaction)) {
                continue;
            }

            result.totalCount++;
            size_t queryCount = transaction->queries().size();
            result.totalQueryCount += queryCount;
            queryCounts.emplace(gid, queryCount);

            if (relationshipResolver.addTransaction(*transaction)) {
                cachedResolver.clearCache();
            }

            auto txnColumns = analysis::TaintAnalyzer::collectColumnRW(*transaction);
            ColumnSet txnAccess = txnColumns.read;
            txnAccess.insert(txnColumns.write.begin(), txnColumns.write.end());

            bool rollbackTarget = isRollbackTarget(gid, candidateIndex);
            auto userQueryOpt = userQueryPath ? userQueryPath(gid) : std::nullopt;
            candidateIndex++;

            if (rollbackTarget || userQueryOpt.has_value()) {
                if (rollbackTarget) {
                    rowCluster.addRollbackTarget(transaction, cachedResolver, shouldRevalidateTarget(gid));
                    columnTaint.insert(txnColumns.write.begin(), txnColumns.write.end());
                    if (!shouldRevalidateTarget(gid)) {
                        pendingTargetCacheRefresh = true;
                    }
                }

                if (userQueryOpt.has_value()) {
                    auto userQuery = loadUserQuery(userQueryOpt.value());
                    if (!userQuery) {
                        std::ostringstream message;
                        message << "failed to load user query for gid " << gid
                                << " from " << userQueryOpt.value();
                        _logger->error("{}", message.str());
                        throw std::runtime_error(message.str());
                    }
                    userQuery->setGid(gid);
                    userQuery->setTimestamp(transaction->timestamp());
                    rowCluster.addPrependTarget(gid, userQuery, cachedResolver);
                    if (replayPlan != nullptr) {
                        replayPlan->userQueries.emplace(gid, *userQuery);
                    }

                    auto prependColumns = analysis::TaintAnalyzer::collectColumnRW(*userQuery);
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

            if (pendingTargetCacheRefresh) {
                rowCluster.refreshTargetCache(cachedResolver);
                pendingTargetCacheRefresh = false;
            }

            bool isColumnDependent = analysis::TaintAnalyzer::columnSetsRelated(columnTaint, txnAccess, _context->foreignKeys);
            bool hasKeyColumns = analysis::TaintAnalyzer::hasKeyColumnItems(*transaction, rowCluster, cachedResolver);

            if (isColumnDependent) {
                columnTaint.insert(txnColumns.write.begin(), txnColumns.write.end());
            }

            if (!isColumnDependent && !hasKeyColumns) {
                continue;
            }

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

            if (!isColumnDependent) {
                if (!rowCluster.shouldReplay(gid)) {
                    continue;
                }

                columnTaint.insert(txnColumns.write.begin(), txnColumns.write.end());

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

        std::sort(result.replayGids.begin(), result.replayGids.end());
        result.replayGids.erase(std::unique(result.replayGids.begin(), result.replayGids.end()),
                                result.replayGids.end());

        for (gid_t gid : result.replayGids) {
            auto it = queryCounts.find(gid);
            if (it != queryCounts.end()) {
                result.replayQueryCount += it->second;
            }
        }

        if (replayPlan != nullptr) {
            replayPlan->gids = result.replayGids;
        }

        return result;
    }

    void StateChanger::bench_prepareRollback() {
        StateChangeReport report(StateChangeReport::PREPARE_AUTO, _plan);

        StateCluster rowCluster(_plan.keyColumns(), _plan.keyColumnGroups());
        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 1000);

        {
            _logger->info("prepare(): loading cluster");
            _clusterStore->load(rowCluster);
            _logger->info("prepare(): loading cluster end");
        }

        {
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle->get(), 0, _plan.dbName());
            updateForeignKeys(dbHandle->get(), 0, _plan.dbName());
        }
        rowCluster.normalizeWithResolver(relationshipResolver);

        std::unordered_set<gid_t> skipGids(_plan.skipGids().begin(), _plan.skipGids().end());

        _reader->open();
        _reader->seek(0);

        size_t totalCount = 0;
        while (_reader->nextHeader()) {
            auto header = _reader->txnHeader();
            gid_t gid = header->gid;
            _reader->nextTransaction();
            auto transaction = _reader->txnBody();

            if (!isTransactionInScope(_plan, skipGids, gid, *transaction)) {
                continue;
            }

            totalCount++;
        }

        auto rollbackIndices = buildAutoRollbackIndices(totalCount, _plan.autoRollbackRatio());
        struct AutoRollbackSelector {
            const std::vector<size_t> &indices;
            size_t next = 0;
            std::vector<gid_t> selected;

            bool select(gid_t gid, size_t candidateIndex) {
                if (next < indices.size() && candidateIndex == indices[next]) {
                    selected.push_back(gid);
                    next++;
                    return true;
                }
                return false;
            }
        };

        AutoRollbackSelector selector { rollbackIndices };
        auto isRollbackTarget = [&selector](gid_t gid, size_t candidateIndex) {
            return selector.select(gid, candidateIndex);
        };
        auto userQueryPath = [](gid_t) -> std::optional<std::string> {
            return std::nullopt;
        };
        auto shouldRevalidate = [](gid_t) {
            return true;
        };

        auto phase_main_start = std::chrono::steady_clock::now();
        auto analysis = analyzeReplayPlan(
            rowCluster,
            relationshipResolver,
            cachedResolver,
            nullptr,
            isRollbackTarget,
            userQueryPath,
            shouldRevalidate
        );
        auto phase_main_end = std::chrono::steady_clock::now();
        std::chrono::duration<double> time = phase_main_end - phase_main_start;
        _phase2Time = time.count();

        report.bench_setRollbackGids(std::set<gid_t>(selector.selected.begin(), selector.selected.end()));
        report.setReplayGidCount(analysis.replayGids.size());
        report.setTotalCount(analysis.totalCount);
        report.setExecutionTime(_phase2Time);

        report.bench_setReplayQueryCount(analysis.replayQueryCount);
        report.bench_setTotalQueryCount(analysis.totalQueryCount);

        if (analysis.totalCount > 0) {
            _logger->info("benchAutoRollback(): {} / {} transactions will be replayed ({}%)",
                          analysis.replayGids.size(),
                          analysis.totalCount,
                          ((double) analysis.replayGids.size() / analysis.totalCount) * 100);
        }
        if (analysis.totalQueryCount > 0) {
            _logger->info("benchAutoRollback(): {} / {} queries will be replayed ({}%)",
                          analysis.replayQueryCount,
                          analysis.totalQueryCount,
                          ((double) analysis.replayQueryCount / analysis.totalQueryCount) * 100);
        }

        if (!_plan.reportPath().empty()) {
            report.writeToJSON(_plan.reportPath());
        }
    }

    void StateChanger::prepare() {
        StateChangeReport report(StateChangeReport::PREPARE, _plan);

        StateCluster rowCluster(_plan.keyColumns(), _plan.keyColumnGroups());

        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 1000);

        StateChangeReplayPlan replayPlan;

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
        rowCluster.normalizeWithResolver(relationshipResolver);

        auto phase_main_start = std::chrono::steady_clock::now();

        auto isRollbackTarget = [this](gid_t gid, size_t) {
            return _plan.isRollbackGid(gid);
        };
        auto userQueryPath = [this](gid_t gid) -> std::optional<std::string> {
            auto it = _plan.userQueries().find(gid);
            if (it == _plan.userQueries().end()) {
                return std::nullopt;
            }
            return it->second;
        };
        auto shouldRevalidate = [this](gid_t gid) {
            gid_t nextGid = gid + 1;
            return !_plan.isRollbackGid(nextGid) && !_plan.hasUserQuery(nextGid);
        };

        auto analysis = analyzeReplayPlan(
            rowCluster,
            relationshipResolver,
            cachedResolver,
            &replayPlan,
            isRollbackTarget,
            userQueryPath,
            shouldRevalidate
        );

        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }

        replayPlan.rollbackGids = _plan.rollbackGids();
        std::sort(replayPlan.rollbackGids.begin(), replayPlan.rollbackGids.end());
        replayPlan.rollbackGids.erase(std::unique(replayPlan.rollbackGids.begin(), replayPlan.rollbackGids.end()),
                                      replayPlan.rollbackGids.end());

        report.setReplayGidCount(analysis.replayGids.size());
        report.setTotalCount(analysis.totalCount);
        report.setExecutionTime(_phase2Time);

        auto replayCountValue = analysis.replayGids.size();
        if (analysis.totalCount > 0) {
            _logger->info("prepare(): {} / {} transactions will be replayed ({}%)",
                          replayCountValue,
                          analysis.totalCount,
                          ((double) replayCountValue / analysis.totalCount) * 100);
        }
        // rowCluster.describe();

        _logger->info("prepare(): main phase {}s", _phase2Time);

        if (_plan.dropIntermediateDB()) {
            dropIntermediateDB();
        }

        auto replaceQueries = rowCluster.generateReplaceQuery(
            _plan.dbName(),
            "__INTERMEDIATE_DB__",
            cachedResolver,
            _context->foreignKeys
        );
        _plan.setReplaceQueries(replaceQueries);
        replayPlan.replaceQueries = replaceQueries;
        _logger->debug("prepare(): generated replace queries (use __INTERMEDIATE_DB__ placeholder)");

        std::ostringstream replaceQueryStream;
        for (const auto &statement : replaceQueries) {
            if (statement.empty()) {
                continue;
            }
            replaceQueryStream << statement << ";\n";
        }
        report.setReplaceQuery(replaceQueryStream.str());

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
