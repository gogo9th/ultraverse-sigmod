//
// Created by cheesekun on 7/12/23.
//

#include <algorithm>
#include <sstream>

#include <fmt/color.h>

#include "GIDIndexWriter.hpp"
#include "cluster/RowCluster.hpp"

#include "cluster/StateCluster.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/StringUtil.hpp"

#include "graph/RowGraph.hpp"

#include "StateChanger.hpp"
#include "GIDIndexReader.hpp"

namespace ultraverse::state::v2 {
    void StateChanger::replay() {
        createIntermediateDB();
        
        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 1000);
        
        RowGraph rowGraph(_plan.keyColumns(), cachedResolver);
        GIDIndexReader gidIndexReader(_plan.stateLogPath(), _plan.stateLogName());
        
        std::atomic_bool isEOF = false;
        
        std::mutex replayTargetsLock;
        std::queue<gid_t> replayTargets;
        
        this->_isRunning = true;
        this->_replayedTxns = 0;
        
        
        for (int i = 0; i < _dbHandlePool.poolSize(); i++) {
            auto dbHandle = _dbHandlePool.take();
            
            dbHandle.get().executeQuery(fmt::format("USE {}", _intermediateDBName));
        }
        
        std::thread replayThread([&]() {
            std::set<gid_t> replayedGids;
            int i = 0;
            
            _reader.open();
            
            
            while (!isEOF || !replayTargets.empty()) {
                if (replayTargets.empty()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 60));
                    
                    goto NEXT_LOOP;
                }
                {
                    std::vector<gid_t> gids;
                    replayTargetsLock.lock();
                    while (!replayTargets.empty()) {
                        gids.push_back(replayTargets.front());
                        replayTargets.pop();
                    }
                    replayTargetsLock.unlock();
                    
                    for (gid_t gid: gids) {
                        while (i - _replayedTxns > 4000) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 60));
                        }
                        
                        auto offset = gidIndexReader.offsetOf(gid);
                        _reader.seek(offset);
                        
                        _reader.nextHeader();
                        _reader.nextTransaction();
                        
                        const auto header = _reader.txnHeader();
                        const auto transaction = _reader.txnBody();
                        
                        auto nodeId = rowGraph.addNode(transaction);
                        
                        // replayedGids.emplace(gid);
                        
                        if (i++ % 1000 == 0) {
                            _logger->info("replay(): transaction #{} added as node #{}; {} / {} executed", gid, nodeId, _replayedTxns, i);
                        }
                    }
                    
                }
                
                NEXT_LOOP:
                continue;
            }
        });
        
        std::thread gcThread([&]() {
            while (_isRunning) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2500));
                
                // _logger->info("replay(): GC thread running...");
                rowGraph.gc();
            }
        });
        
        std::vector<std::thread> workerThreads;
        
        for (int i = 0; i < _plan.threadNum(); i++) {
            workerThreads.emplace_back(&StateChanger::replayThreadMain, this, i, std::ref(rowGraph));
        }
        
        /*
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
         */
        
        auto phase_main_start = std::chrono::steady_clock::now();
        _logger->info("replay(): waiting for replay targets via STDIN...");
        
        
        std::string line;
        while (std::getline(std::cin, line)) {
            uint64_t gid = std::stoull(line);
            // _logger->info("replay(): transaction #{} enqueued", gid);
            
            {
                std::scoped_lock<std::mutex> _lock(replayTargetsLock);
                replayTargets.emplace(gid);
            }
        }
        
        isEOF = true;
        
        if (replayThread.joinable()) {
            replayThread.join();
        }
        
        while (!rowGraph.isFinalized()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        _isRunning = false;
        
        for (auto &thread: workerThreads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        
        if (gcThread.joinable()) {
            gcThread.join();
        }
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        _logger->info("replay(): main phase {}s", _phase2Time);
        
        dropIntermediateDB();
    }
    
    void StateChanger::replayThreadMain(int workerId, RowGraph &rowGraph) {
        auto logger = createLogger(fmt::format("ReplayThread #{}", workerId));
        logger->info("thread started");
        
        while (_isRunning) {
            auto nodeId = rowGraph.entrypoint(workerId);
            
            if (nodeId == nullptr) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 60));
                continue;
            }
            
            {
                auto node = std::move(rowGraph.nodeFor(nodeId));
                const auto transaction = node->transaction;
                
                if (node == nullptr || node->finalized) {
                    goto NEXT_LOOP;
                }
                
                // logger->info("processing node #{}, gid #{}", nodeId, transaction->gid());
                
                /*
                while (true) {
                    // 이 부분 크래시나는데 일조함
                    if (std::all_of(dependencies.begin(), dependencies.end(), [&](auto dependencyId) {
                        auto dependency = rowGraph.nodeFor(dependencyId);
                        return (bool) dependency->finalized;
                    })) {
                        break;
                    }
                    
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 30));
                }
                 */
                {
                    auto dbHandle = _dbHandlePool.take();
                    
                    bool isProcedureCall = transaction->flags() & Transaction::FLAG_IS_PROCEDURE_CALL;
                    
                    // logger->info("replaying transaction #{}", transaction->gid());
                    
                    dbHandle.get().executeQuery("BEGIN");
                    
                    try {
                        for (const auto &query: transaction->queries()) {
                            bool isProcedureCallQuery = query->flags() & Query::FLAG_IS_PROCCALL_QUERY;
                            if (isProcedureCall && !isProcedureCallQuery) {
                                goto NEXT_QUERY;
                            }
                            
                            if (dbHandle.get().executeQuery(query->statement()) != 0) {
                                logger->error("query execution failed: {}", mysql_error(dbHandle.get()));
                            }
                            
                            // 프로시저에서 반환한 result를 소모하지 않으면 commands out of sync 오류가 난다
                            do {
                                auto result = mysql_store_result(dbHandle.get());
                                if (result != nullptr) {
                                    mysql_free_result(result);
                                }
                            } while (mysql_next_result(dbHandle.get()) == 0);
                            
                            NEXT_QUERY:
                            continue;
                        }
                        dbHandle.get().executeQuery("COMMIT");
                    } catch (std::exception &e) {
                        logger->error("exception occurred while replaying transaction #{}: {}", transaction->gid(),
                                       e.what());
                        dbHandle.get().executeQuery("ROLLBACK");
                    }
                }
                
                _replayedTxns++;
                
                /*
                auto it = std::find_if(dependents.begin(), dependents.end(), [&](auto &dependent) {
                    auto node = rowGraph.nodeFor(dependent);
                    return (node->finalized && node->processedBy == -1);
                });
                
                if (it == dependents.end()) {
                    node->finalized = true;
                    node->transaction = nullptr;
                    goto NEXT_LOOP;
                }
                
                auto child = rowGraph.nodeFor(*it);
                child->processedBy = workerId;
                node->finalized = true;
                node->transaction = nullptr;
                
                nodeId = *it;
                 */
                
                
                node->finalized = true;
                node->transaction = nullptr;
            }
            
            NEXT_LOOP:
            continue;
        }
    
    }
}
