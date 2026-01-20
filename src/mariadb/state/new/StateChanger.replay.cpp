//
// Created by cheesekun on 7/12/23.
//

#include <fmt/color.h>

#include "graph/RowGraph.hpp"

#include "StateChanger.hpp"
#include "StateChangeReport.hpp"
#include "StateChangeReplayPlan.hpp"

namespace ultraverse::state::v2 {
    void StateChanger::replay() {
        StateChangeReport report(StateChangeReport::EXECUTE, _plan);
        
        createIntermediateDB();
        report.setIntermediateDBName(_intermediateDBName);
        
        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 8000);
        
        RowGraph rowGraph(_plan.keyColumns(), cachedResolver);
        rowGraph.setRangeComparisonMethod(_plan.rangeComparisonMethod());

        const std::string replayPlanPath = _plan.stateLogPath() + "/" + _plan.stateLogName() + ".ultreplayplan";
        auto replayPlan = StateChangeReplayPlan::load(replayPlanPath);
        _logger->info("replay(): loaded replay plan from {} ({} gids, {} user queries)",
                      replayPlanPath, replayPlan.gids.size(), replayPlan.userQueries.size());
        
        this->_isRunning = true;
        this->_replayedTxns = 0;
        
        
        for (int i = 0; i < _dbHandlePool.poolSize(); i++) {
            auto dbHandle = _dbHandlePool.take();
            auto &handle = dbHandle->get();
            
            handle.executeQuery(fmt::format("USE {}", _intermediateDBName));
        }
        
        std::thread replayThread([&]() {
            int i = 0;
            
            _reader->open();

            auto userIt = replayPlan.userQueries.begin();
            auto userEnd = replayPlan.userQueries.end();

            auto addUserQueryNode = [&](gid_t userGid, const Transaction &userTxn) -> RowGraphId {
                auto txnPtr = std::make_shared<Transaction>(userTxn);
                txnPtr->setGid(userGid);
                if (relationshipResolver.addTransaction(*txnPtr)) {
                    cachedResolver.clearCache();
                }
                auto nodeId = rowGraph.addNode(txnPtr);
                if (i++ % 1000 == 0) {
                    _logger->info("replay(): user query for gid #{} added as node #{}; {} / {} executed",
                                  userGid, nodeId, (int) _replayedTxns, i);
                }
                return nodeId;
            };

            for (gid_t gid : replayPlan.gids) {
                while (userIt != userEnd && userIt->first < gid) {
                    while (i - _replayedTxns > 4000) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 60));
                    }
                    addUserQueryNode(userIt->first, userIt->second);
                    ++userIt;
                }

                RowGraphId prependNodeId = nullptr;
                if (userIt != userEnd && userIt->first == gid) {
                    while (i - _replayedTxns > 4000) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 60));
                    }
                    prependNodeId = addUserQueryNode(userIt->first, userIt->second);
                    ++userIt;
                }

                while (i - _replayedTxns > 4000) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 60));
                }

                if (!_reader->seekGid(gid)) {
                    _logger->warn("replay(): gid #{} not found in state log", gid);
                    continue;
                }

                _reader->nextHeader();
                _reader->nextTransaction();

                const auto transaction = _reader->txnBody();

                if (relationshipResolver.addTransaction(*transaction)) {
                    cachedResolver.clearCache();
                }

                const bool holdTarget = (prependNodeId != nullptr);
                auto nodeId = rowGraph.addNode(transaction, holdTarget);
                if (prependNodeId != nullptr) {
                    rowGraph.addEdge(prependNodeId, nodeId);
                    rowGraph.releaseNode(nodeId);
                }

                if (i++ % 1000 == 0) {
                    _logger->info("replay(): transaction #{} added as node #{}; {} / {} executed",
                                  gid, nodeId, (int) _replayedTxns, i);
                }
            }

            while (userIt != userEnd) {
                while (i - _replayedTxns > 4000) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 60));
                }
                addUserQueryNode(userIt->first, userIt->second);
                ++userIt;
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
        
        auto phase_main_start = std::chrono::steady_clock::now();
        _logger->info("replay(): executing replay plan...");
        
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
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        _logger->info("replay(): main phase {}s", _phase2Time);
        report.setExecutionTime(_phase2Time);
        
        if (gcThread.joinable()) {
            gcThread.join();
        }
        
        if (!_plan.reportPath().empty()) {
            report.writeToJSON(_plan.reportPath());
        }
        
        if (_plan.dropIntermediateDB()) {
            dropIntermediateDB();
        }
        
        // dropIntermediateDB();
    }
    
    void StateChanger::replayThreadMain(int workerId, RowGraph &rowGraph) {
        auto logger = createLogger(fmt::format("ReplayThread #{}", workerId));
        logger->info("thread started");
        
        while (_isRunning) {
            auto nodeId = rowGraph.entrypoint(workerId);
            
            if (nodeId == nullptr) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
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
                    auto &handle = dbHandle->get();

                    bool isProcedureCall = transaction->flags() & Transaction::FLAG_IS_PROCEDURE_CALL;
                    
                    // logger->info("replaying transaction #{}", transaction->gid());
                    
                    handle.executeQuery("SET autocommit=0");
                    handle.executeQuery("START TRANSACTION");
                    
                    try {
                        for (const auto &query: transaction->queries()) {
                            bool isProcedureCallQuery = query->flags() & Query::FLAG_IS_PROCCALL_QUERY;
                            if (isProcedureCall && !isProcedureCallQuery) {
                                goto NEXT_QUERY;
                            }
                            
                            if (handle.executeQuery(query->statement()) != 0) {
                                logger->error("query execution failed: {} / {}", handle.lastError(), query->statement());
                            }
                            
                            // 프로시저에서 반환한 result를 소모하지 않으면 commands out of sync 오류가 난다
                            handle.consumeResults();
                            
                            NEXT_QUERY:
                            continue;
                        }
                        handle.executeQuery("COMMIT");
                    } catch (std::exception &e) {
                        logger->error("exception occurred while replaying transaction #{}: {}", transaction->gid(),
                                       e.what());
                        handle.executeQuery("ROLLBACK");
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
                node->transaction.reset();
            }
            
            NEXT_LOOP:
            continue;
        }
    
    }
}
