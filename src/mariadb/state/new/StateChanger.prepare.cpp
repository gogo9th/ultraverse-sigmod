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
        
        std::queue<std::shared_ptr<std::promise<int>>> tasks;
        std::set<gid_t> replayGids;
        
        while (_reader.nextHeader()) {
            auto header = _reader.txnHeader();
            gid_t gid = header->gid;
            
            // _logger->info("read gid {}", gid);
            
            if (_plan.isRollbackGid(gid) || _plan.hasUserQuery(gid)) {
                _reader.nextTransaction();
                auto transaction = _reader.txnBody();
                
                auto promise = taskExecutor.post<int>(
                    [this, &graphLock, &stdoutLock, &rowCluster, &cachedResolver, &replayGids, transaction]() {
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
                        
                        if (_plan.isRollbackGid(transaction->gid())) {
                            rowCluster.addRollbackTarget(transaction, cachedResolver);
                        }
                        
                        if (_plan.hasUserQuery(transaction->gid())) {
                            auto userQuery = std::move(loadUserQuery(_plan.userQueries()[transaction->gid()]));
                            
                            rowCluster.addPrependTarget(transaction->gid(), userQuery, cachedResolver);
                        }
                        
                        return 0;
                });
                
                tasks.emplace(std::move(promise));
                continue;
            } else if (rowCluster.shouldReplay(gid)) {
                // _logger->info("shouldReplay({}) returned true", gid);
                // replayGids.emplace(transaction->gid());
                {
                    // std::scoped_lock<std::mutex> _lock(stdoutLock);
                    std::cout << gid<< std::endl;
                    // std::cout.flush();
                    
                }
            }
            
            _reader.skipTransaction();
        }
        
        while (!tasks.empty()) {
            tasks.front()->get_future().wait();
            tasks.pop();
        }
        
        taskExecutor.shutdown();
        
        _logger->info("prepare(): {} transactions will be replayed", replayGids.size());
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
