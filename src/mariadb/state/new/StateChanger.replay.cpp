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

namespace ultraverse::state::v2 {
    void StateChanger::replay() {
        createIntermediateDB();
        
        std::atomic_bool isEOF = false;
        
        std::mutex replayTargetsLock;
        std::set<gid_t> replayTargets;
        
        
        std::thread replayThread([&]() {
            std::set<gid_t> replayedGids;
            
            _reader.open();
            
            
            while (!isEOF) {
                {
                    replayTargetsLock.lock();
                    while (replayTargets.empty()) {
                        replayTargetsLock.unlock();
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                    
                    replayTargetsLock.unlock();
                }
                
                _reader.seek(0);
                
                while (_reader.nextHeader()) {
                    const auto header = _reader.txnHeader();
                    
                    {
                        std::scoped_lock<std::mutex> _lock(replayTargetsLock);
                        
                        if (replayTargets.empty()) {
                            goto NEXT_WHILE;
                        }
                        
                        auto it = replayTargets.find(header->gid);
                        if (it == replayTargets.end()) {
                            _reader.seek(header->nextPos);
                            goto NEXT;
                        }
                        
                        
                        replayTargets.erase(it);
                    }
                    
                    {
                        _reader.nextTransaction();
                        auto transaction = _reader.txnBody();
                        auto dbHandle = _dbHandlePool.take();
                        
                        _logger->info("replaying transaction #{}", transaction->gid());
                        
                        dbHandle.get().executeQuery("USE " + _intermediateDBName);
                        dbHandle.get().executeQuery("BEGIN");
                        
                        try {
                            for (const auto &query: transaction->queries()) {
                                dbHandle.get().executeQuery(query->statement());
                            }
                        } catch (std::exception &e) {
                            _logger->error("exception occurred while replaying transaction #{}: {}", transaction->gid(),
                                           e.what());
                            dbHandle.get().executeQuery("ROLLBACK");
                            continue;
                        }
                        
                        dbHandle.get().executeQuery("COMMIT");
                    }
                    
                    NEXT:
                    continue;
                }
                
                NEXT_WHILE:
                continue;
            }
        });
        
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
        
        auto phase_main_start = std::chrono::steady_clock::now();
        _logger->info("replay(): waiting for replay targets via STDIN...");
        
        std::string line;
        while (std::getline(std::cin, line)) {
            uint64_t gid = std::stoull(line);
            _logger->info("replay(): transaction #{} enqueued", gid);
            
            {
                std::scoped_lock<std::mutex> _lock(replayTargetsLock);
                replayTargets.emplace(gid);
            }
        }
        
        isEOF = true;
        
        if (replayThread.joinable()) {
            replayThread.join();
        }
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }
        
        _logger->info("replay(): main phase {}s", _phase2Time);
        
        dropIntermediateDB();
    }
}
