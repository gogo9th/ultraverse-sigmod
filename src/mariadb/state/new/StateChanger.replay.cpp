//
// Created by cheesekun on 7/12/23.
//

#include <algorithm>
#include <sstream>

#include <fmt/color.h>

#include "graph/RowGraph.hpp"

#include "StateChanger.hpp"
#include "StateChangeReport.hpp"
#include "StateChangeReplayPlan.hpp"
#include "utils/StringUtil.hpp"

namespace ultraverse::state::v2 {
    void StateChanger::replay() {
        StateChangeReport report(StateChangeReport::EXECUTE, _plan);
        
        createIntermediateDB();
        report.setIntermediateDBName(_intermediateDBName);

        const std::string replayPlanPath = _plan.stateLogPath() + "/" + _plan.stateLogName() + ".ultreplayplan";
        auto replayPlan = StateChangeReplayPlan::load(replayPlanPath);
        _plan.setReplaceQueries(replayPlan.replaceQueries);
        _logger->info("replay(): loaded replay plan from {} ({} gids, {} user queries)",
                      replayPlanPath, replayPlan.gids.size(), replayPlan.userQueries.size());

        gid_t firstTargetGid = 0;
        bool hasTargetGid = false;
        if (!replayPlan.rollbackGids.empty()) {
            firstTargetGid = *std::min_element(replayPlan.rollbackGids.begin(), replayPlan.rollbackGids.end());
            hasTargetGid = true;
        }
        if (!replayPlan.userQueries.empty()) {
            gid_t userGid = replayPlan.userQueries.begin()->first;
            if (!hasTargetGid || userGid < firstTargetGid) {
                firstTargetGid = userGid;
                hasTargetGid = true;
            }
        }
        if (!hasTargetGid && !replayPlan.gids.empty()) {
            firstTargetGid = replayPlan.gids.front();
            hasTargetGid = true;
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

        for (int i = 0; i < _dbHandlePool.poolSize(); i++) {
            auto dbHandle = _dbHandlePool.take();
            auto &handle = dbHandle->get();
            
            handle.executeQuery(fmt::format("USE {}", _intermediateDBName));
        }

        auto runPreReplay = [&](gid_t startGid, gid_t endGid) {
            if (startGid > endGid) {
                return;
            }

            _logger->info("replay(): pre-replay range {}..{}", startGid, endGid);

            StateRelationshipResolver preResolver(_plan, *_context);
            CachedRelationshipResolver preCachedResolver(preResolver, 8000);

            RowGraph preGraph(_plan.keyColumns(), preCachedResolver, _plan.keyColumnGroups());
            preGraph.setRangeComparisonMethod(_plan.rangeComparisonMethod());

            std::atomic_bool preRunning = true;
            std::atomic_uint64_t preReplayedTxns = 0;

            _reader->open();
            if (!_reader->seekGid(startGid)) {
                _logger->warn("replay(): pre-replay start gid #{} not found in state log", startGid);
                _reader->close();
                return;
            }

            std::thread feeder([&]() {
                uint64_t added = 0;
                while (_reader->nextHeader()) {
                    auto header = _reader->txnHeader();
                    if (!header) {
                        break;
                    }
                    if (header->gid < startGid) {
                        _reader->skipTransaction();
                        continue;
                    }
                    if (header->gid > endGid) {
                        break;
                    }

                    while (added - preReplayedTxns.load() > 4000) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 60));
                    }

                    _reader->nextTransaction();
                    const auto transaction = _reader->txnBody();

                    if (!transaction || !transaction->isRelatedToDatabase(_plan.dbName())) {
                        continue;
                    }

                    if (preResolver.addTransaction(*transaction)) {
                        preCachedResolver.clearCache();
                    }

                    auto nodeId = preGraph.addNode(transaction);
                    if (++added % 1000 == 0) {
                        auto gid = header->gid;
                        _logger->info("replay(): pre-replay transaction #{} added as node #{}; {} / {} executed",
                                      gid, nodeId, (int) preReplayedTxns.load(), added);
                    }
                }
            });

            std::thread gcThread([&]() {
                while (preRunning) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
                    preGraph.gc();
                }
            });

            std::vector<std::thread> workerThreads;
            for (int i = 0; i < _plan.threadNum(); i++) {
                workerThreads.emplace_back(&StateChanger::replayThreadMain, this, i, std::ref(preGraph),
                                           std::ref(preRunning), std::ref(preReplayedTxns));
            }

            if (feeder.joinable()) {
                feeder.join();
            }

            _reader->close();

            while (!preGraph.isFinalized()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            preRunning = false;

            for (auto &thread : workerThreads) {
                if (thread.joinable()) {
                    thread.join();
                }
            }

            if (gcThread.joinable()) {
                gcThread.join();
            }

            _logger->info("replay(): pre-replay finished ({} transactions)", (uint64_t) preReplayedTxns.load());
        };

        if (_plan.hasReplayFromGid()) {
            const gid_t replayFromGid = _plan.replayFromGid();
            if (!hasTargetGid) {
                _logger->warn("replay(): --replay-from specified but target gid is unknown; skipping pre-replay");
            } else if (replayFromGid >= firstTargetGid) {
                _logger->warn("replay(): --replay-from {} is not before target gid {}; skipping pre-replay",
                              replayFromGid, firstTargetGid);
            } else {
                runPreReplay(replayFromGid, firstTargetGid - 1);
            }
        }

        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 8000);

        RowGraph rowGraph(_plan.keyColumns(), cachedResolver, _plan.keyColumnGroups());
        rowGraph.setRangeComparisonMethod(_plan.rangeComparisonMethod());

        this->_isRunning = true;
        this->_replayedTxns = 0;

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
                std::this_thread::sleep_for(std::chrono::milliseconds(10000));
                
                // _logger->info("replay(): GC thread running...");
                rowGraph.gc();
            }
        });
        
        std::vector<std::thread> workerThreads;
        
        auto phase_main_start = std::chrono::steady_clock::now();
        _logger->info("replay(): executing replay plan...");

        for (int i = 0; i < _plan.threadNum(); i++) {
            workerThreads.emplace_back(&StateChanger::replayThreadMain, this, i, std::ref(rowGraph),
                                       std::ref(_isRunning), std::ref(_replayedTxns));
        }
        
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

        const auto &replaceQueries = _plan.replaceQueries();
        if (replaceQueries.empty()) {
            _logger->warn("replay(): replace query list is empty; skipping state update");
        } else if (!_plan.executeReplaceQuery()) {
            std::ostringstream script;
            for (const auto &statement : replaceQueries) {
                if (statement.empty()) {
                    continue;
                }
                auto substituted = utility::replaceAll(statement, "__INTERMEDIATE_DB__", _intermediateDBName);
                script << substituted << ";\n";
            }
            _logger->warn("replay(): manual replace query mode enabled; skipping execution");
            _logger->info("replay(): execute the following queries manually on '{}':\n{}",
                          _plan.dbName(), script.str());
        } else {
            _logger->info("replay(): executing replace queries...");
            auto dbHandle = _dbHandlePool.take();
            auto &handle = dbHandle->get();
            size_t executed = 0;
            size_t failed = 0;

            handle.executeQuery("SET autocommit = 0");
            handle.executeQuery("START TRANSACTION");

            for (const auto &statement : replaceQueries) {
                if (statement.empty()) {
                    continue;
                }
                auto substituted = utility::replaceAll(statement, "__INTERMEDIATE_DB__", _intermediateDBName);

                _logger->debug("replay(): executing replace query: {}", substituted);
                if (handle.executeQuery(substituted) != 0) {
                    _logger->error("replay(): replace query execution failed: {} / {}", handle.lastError(), substituted);
                    ++failed;
                }
                handle.consumeResults();
                ++executed;
            }
            if (failed > 0) {
                _logger->warn("replay(): replace queries completed with failures ({}/{})", failed, executed);
            } else {
                _logger->info("replay(): replace queries executed ({})", executed);
            }

            handle.executeQuery("COMMIT");
        }
        
        if (!_plan.reportPath().empty()) {
            report.writeToJSON(_plan.reportPath());
        }
        
        if (_plan.dropIntermediateDB()) {
            if (!_plan.executeReplaceQuery()) {
                _logger->warn("replay(): keeping intermediate database '{}' (manual replace query mode)",
                              _intermediateDBName);
            } else {
                dropIntermediateDB();
            }
        }
        
        // dropIntermediateDB();
    }
    
    void StateChanger::replayThreadMain(int workerId,
                                        RowGraph &rowGraph,
                                        std::atomic_bool &running,
                                        std::atomic_uint64_t &replayedTxns) {
        auto logger = createLogger(fmt::format("ReplayThread #{}", workerId));
        logger->info("thread started");
        
        while (running) {
            auto nodeId = rowGraph.entrypoint(workerId);
            
            if (nodeId == nullptr) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
            
            {
                auto node = rowGraph.nodeFor(nodeId);
                if (node == nullptr || node->finalized) {
                    goto NEXT_LOOP;
                }

                const auto transaction = std::atomic_load(&node->transaction);
                if (!transaction) {
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
                    
                    logger->info("replaying transaction #{}", transaction->gid());
                    
                    handle.executeQuery("SET autocommit=0");
                    handle.executeQuery("START TRANSACTION");
                    
                    try {
                        for (const auto &query: transaction->queries()) {
                            bool isProcedureCallQuery = query->flags() & Query::FLAG_IS_PROCCALL_QUERY;
                            if (isProcedureCall && !isProcedureCallQuery) {
                                goto NEXT_QUERY;
                            }
                            
                            applyStatementContext(handle, *query);

                            // logger->debug("[#{}] executing query: {}", transaction->gid(), query->statement());
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
                
                replayedTxns++;
                
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
                std::atomic_store(&node->transaction, std::shared_ptr<Transaction>{});
            }
            
            NEXT_LOOP:
            continue;
        }
    
    }
}
