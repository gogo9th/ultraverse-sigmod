//
// Created by cheesekun on 6/27/23.
//

#include <algorithm>
#include <sstream>

#include <execution>

#include <fmt/color.h>

#include "GIDIndexWriter.hpp"
#include "cluster/RowCluster.hpp"

#include "cluster/StateCluster.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/StringUtil.hpp"

#include "StateChanger.hpp"
#include "StateClusterWriter.hpp"
#include "GIDIndexReader.hpp"

#include "StateChangeReport.hpp"


namespace ultraverse::state::v2 {
    
    void StateChanger::makeCluster() {
        StateChangeReport report(StateChangeReport::MAKE_CLUSTER, _plan);
        
        TaskExecutor taskExecutor(_plan.threadNum());
        StateCluster rowCluster(_plan.keyColumns());
        
        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 1000);
        
        StateClusterWriter clusterWriter(_plan.stateLogPath(), _plan.stateLogName());
        GIDIndexWriter gidIndexWriter(_plan.stateLogPath(), _plan.stateLogName());
        
        std::mutex graphLock;
        
        createIntermediateDB();
        
        if (!_plan.dbDumpPath().empty()) {
            auto load_backup_start = std::chrono::steady_clock::now();
            loadBackup(_intermediateDBName, _plan.dbDumpPath());
            
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle.get(), 0);
            updateForeignKeys(dbHandle.get(), 0);
            auto load_backup_end = std::chrono::steady_clock::now();
            
            std::chrono::duration<double> time = load_backup_end - load_backup_start;
            _logger->info("LOAD BACKUP END: {}s elapsed", time.count());
        }
        
        _reader.open();
        
        auto phase_main_start = std::chrono::steady_clock::now();
        _logger->info("makeCluster(): building cluster");
        
        std::queue<std::shared_ptr<std::promise<int>>> tasks;
        std::set<gid_t> replayGids;
        
        while (_reader.nextHeader()) {
            auto header = _reader.txnHeader();
            auto pos = _reader.pos() - sizeof(TransactionHeader);
            
            _reader.nextTransaction();
            auto transaction = _reader.txnBody();
            
            gidIndexWriter.append(pos);
            
            auto promise = taskExecutor.post<int>(
                [this, &graphLock, &rowCluster, &cachedResolver, &clusterWriter, transaction]() {
                    if (!transaction->isRelatedToDatabase(_plan.dbName())) {
                        _logger->trace("skipping transaction #{} because it is not related to database {}",
                                       transaction->gid(), _plan.dbName());
                        return 0;
                    }
                    
                    if (transaction->flags() & Transaction::FLAG_CONTAINS_DDL) {
                        _logger->warn(
                            "DDL statement found in transaction #{}, but this version of ultraverse does not support DDL statement yet",
                            transaction->gid());
                        _logger->warn("transaction #{} will be skipped", transaction->gid());
                        return 0;
                    }
                    
                    rowCluster.insert(transaction, cachedResolver);
                    
                    for (auto &query: transaction->queries()) {
                        if (query->flags() & Query::FLAG_IS_PROCCALL_QUERY) {
                            // FIXME: 프로시저 쿼리 어케할려고?
                            continue;
                        }
                        
                        /*
                        std::scoped_lock _lock(graphLock);
                        
                        bool isColumnGraphChanged =
                            _columnGraph->add(query->readSet(), READ, _context->foreignKeys) ||
                            _columnGraph->add(query->writeSet(), WRITE, _context->foreignKeys);
                        
                        bool isTableGraphChanged =
                            _tableGraph->addRelationship(query->readSet(), query->writeSet());
                        
                        if (isColumnGraphChanged) {
                            _logger->info("updating column dependency graph");
                            clusterWriter << *_columnGraph;
                        }
                        
                        if (isTableGraphChanged) {
                            _logger->info("updating table dependency graph");
                            clusterWriter << *_tableGraph;
                        }
                         */
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
        
        rowCluster.merge();
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        _logger->info("make_cluster(): main phase {}s", _phase2Time);
        
        _logger->info("make_cluster(): saving cluster..");
        clusterWriter << rowCluster;
        
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
        StateClusterWriter clusterWriter(_plan.stateLogPath(), _plan.stateLogName());
        
        std::mutex gidListMutex;
        
        size_t replayGidCount = 0;
        size_t totalCount = 0;
        
        size_t totalQueryCount = 0;
        size_t replayQueryCount = 0;
        
        std::map<gid_t, size_t> queryCounts;
        
        std::set<gid_t> selectedGids;
        std::set<gid_t> replayGids;
        
        
        {
            _logger->info("prepare(): loading cluster");
            clusterWriter >> rowCluster;
            _logger->info("prepare(): loading cluster end");
        }
        
        /* PHASE 0: walk state log */
        
        _reader.open();
        _reader.seek(0);
        
        while (_reader.nextHeader()) {
            const auto &header = _reader.txnHeader();
            
            _reader.nextTransaction();
            const auto &body = _reader.txnBody();
            
            if (!body->isRelatedToDatabase(_plan.dbName())) {
                continue;
            }
            
            size_t queryCount = body->queries().size();
            totalQueryCount += queryCount;
            
            queryCounts.emplace(header->gid, queryCount);
            
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
            
            
            if (replayQueryCount < (size_t) (totalQueryCount * _plan.autoRollbackRatio())) {
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
        
        _logger->info("benchAutoRollback(): {} / {} transactions will be replayed ({}%)", replayGidCount, totalCount, ((double) replayGidCount / totalCount) * 100);
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
        
        StateClusterWriter clusterWriter(_plan.stateLogPath(), _plan.stateLogName());
        
        std::mutex graphLock;
        std::mutex stdoutLock;
        
        size_t replayGidCount = 0;
        size_t totalCount = 0;
        
        createIntermediateDB();
        report.setIntermediateDBName(_intermediateDBName);
        
        
        if (!_plan.dbDumpPath().empty()) {
            auto load_backup_start = std::chrono::steady_clock::now();
            loadBackup(_intermediateDBName, _plan.dbDumpPath());
            
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle.get(), 0);
            updateForeignKeys(dbHandle.get(), 0);
            auto load_backup_end = std::chrono::steady_clock::now();
            
            std::chrono::duration<double> time = load_backup_end - load_backup_start;
            _logger->info("LOAD BACKUP END: {}s elapsed", time.count());
            report.setSQLLoadTime(time.count());
        }
        
        {
            _logger->info("prepare(): loading cluster");
            clusterWriter >> rowCluster;
            _logger->info("prepare(): loading cluster end");
        }
        
        _reader.open();
        _reader.seek(0);
        
        auto phase_main_start = std::chrono::steady_clock::now();
        
        std::atomic_bool isRunning = true;
        
        std::mutex tasksMutex;
        std::queue<std::shared_ptr<std::promise<gid_t>>> tasks;
        std::set<gid_t> replayGids;
        
        std::thread promiseConsumerThread([&isRunning, &tasks, &tasksMutex, &replayGidCount]() {
            while (isRunning || !tasks.empty()) {
                if (tasks.empty()) {
                    continue;
                }
                
                tasksMutex.lock();
                auto promise = std::move(tasks.front());
                tasks.pop();
                tasksMutex.unlock();
                
                gid_t gid = promise->get_future().get();
                if (gid != UINT64_MAX) {
                    std::cout << gid << std::endl;
                    
                    replayGidCount++;
                }
            }
        });
        
        while (_reader.nextHeader()) {
            auto header = _reader.txnHeader();
            gid_t gid = header->gid;
            
            totalCount++;
            
            // _logger->info("read gid {}", gid);
            
            if (_plan.isRollbackGid(gid) || _plan.hasUserQuery(gid)) {
                _reader.nextTransaction();
                auto transaction = _reader.txnBody();
                
                
                if (!transaction->isRelatedToDatabase(_plan.dbName())) {
                    _logger->trace("skipping transaction #{} because it is not related to database {}",
                                   transaction->gid(), _plan.dbName());
                    continue;
                }
                
                if (transaction->flags() & Transaction::FLAG_CONTAINS_DDL) {
                    _logger->warn(
                        "DDL statement found in transaction #{}, but this version of ultraverse does not support DDL statement yet",
                        transaction->gid());
                    _logger->warn("transaction #{} will be skipped", transaction->gid());
                    continue;
                }
                
                auto nextGid = transaction->gid() + 1;
                bool shouldRevalidate = !_plan.isRollbackGid(nextGid) && !_plan.hasUserQuery(nextGid);
                
                if (_plan.isRollbackGid(transaction->gid())) {
                    rowCluster.addRollbackTarget(transaction, cachedResolver, shouldRevalidate);
                }
                
                if (_plan.hasUserQuery(transaction->gid())) {
                    auto userQuery = std::move(loadUserQuery(_plan.userQueries()[transaction->gid()]));
                    
                    rowCluster.addPrependTarget(transaction->gid(), userQuery, cachedResolver);
                }
                
               
                continue;
            } else {
                std::scoped_lock _lock(tasksMutex);
                tasks.push(taskExecutor.post<gid_t>([gid, &rowCluster, &cachedResolver]() {
                    if (rowCluster.shouldReplay(gid)) {
                        return gid;
                    }
                    return UINT64_MAX;
                }));
            }
            
            _reader.skipTransaction();
        }
        
        
        isRunning = false;
        
        if (promiseConsumerThread.joinable()) {
            promiseConsumerThread.join();
        }

        
        taskExecutor.shutdown();
        
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        report.setReplayGidCount(replayGidCount);
        report.setTotalCount(totalCount);
        report.setExecutionTime(_phase2Time);
        
        _logger->info("prepare(): {} / {} transactions will be replayed ({}%)", replayGidCount, totalCount, ((double) replayGidCount / totalCount) * 100);
        // rowCluster.describe();
        
        _logger->info("prepare(): main phase {}s", _phase2Time);
        
        if (_plan.dropIntermediateDB()) {
            dropIntermediateDB();
        }
        
        std::cout.flush();
        
        std::string replaceQuery = rowCluster.generateReplaceQuery(_plan.dbName(), "__INTERMEDIATE_DB__", cachedResolver);
        _logger->debug("TODO: execute query: \n{}", replaceQuery);
        report.setReplaceQuery(replaceQuery);
        
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        
        if (!_plan.reportPath().empty()) {
            report.writeToJSON(_plan.reportPath());
        }
    }

}
