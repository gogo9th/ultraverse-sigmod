//
// Created by cheesekun on 6/27/23.
//

#include <algorithm>
#include <sstream>

#include <fmt/color.h>

#include "GIDIndexWriter.hpp"
#include "cluster/RowCluster.hpp"

#include "cluster/StateCluster.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/StringUtil.hpp"

#include "StateChanger.hpp"
#include "StateClusterWriter.hpp"
#include "GIDIndexReader.hpp"


namespace ultraverse::state::v2 {
    
    void StateChanger::makeCluster() {
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
            tasks.front()->get_future().wait();
            tasks.pop();
        }
        
        taskExecutor.shutdown();
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        _logger->info("make_cluster(): main phase {}s", _phase2Time);
        
        _logger->info("make_cluster(): saving cluster..");
        clusterWriter << rowCluster;
        
        dropIntermediateDB();
    }
    
    void StateChanger::prepare() {
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
                
                if (_plan.isRollbackGid(transaction->gid())) {
                    rowCluster.addRollbackTarget(transaction, cachedResolver);
                }
                
                if (_plan.hasUserQuery(transaction->gid())) {
                    auto userQuery = std::move(loadUserQuery(_plan.userQueries()[transaction->gid()]));
                    
                    rowCluster.addPrependTarget(transaction->gid(), userQuery, cachedResolver);
                }
                
                continue;
            } else {
                std::scoped_lock _lock(tasksMutex);
                tasks.push(taskExecutor.post<gid_t>([gid, &rowCluster]() {
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
        
        _logger->info("prepare(): {} / {} transactions will be replayed ({}%)", replayGidCount, totalCount, ((double) replayGidCount / totalCount) * 100);
        // rowCluster.describe();
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        _logger->info("prepare(): main phase {}s", _phase2Time);
        
        dropIntermediateDB();
        
        std::cout.flush();
        
        
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }

}
