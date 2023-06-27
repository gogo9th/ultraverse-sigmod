//
// Created by cheesekun on 6/27/23.
//

#include <algorithm>
#include <sstream>

#include <fmt/color.h>

#include <bison_parser.h>
#include <SQLParser.h>
#include <SQLParserResult.h>

#include "StateLogWriter.hpp"
#include "GIDIndexWriter.hpp"
#include "GIDIndexReader.hpp"
#include "cluster/RowCluster.hpp"

#include "cluster/StateCluster.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/StringUtil.hpp"


#include "StateChanger.hpp"


namespace ultraverse::state::v2 {
    
    void StateChanger::prepare() {
        TaskExecutor taskExecutor(_plan.threadNum());
        StateCluster rowCluster(_plan.keyColumns());
        
        StateRelationshipResolver relationshipResolver(_plan, *_context);
        CachedRelationshipResolver cachedResolver(relationshipResolver, 1000);
        
        GIDIndexWriter gidIndexWriter(_plan.stateLogPath(), _plan.stateLogName());
        
        std::mutex graphLock;
        
        createIntermediateDB();
        
        if (!_plan.dbDumpPath().empty()) {
            loadBackup(_intermediateDBName, _plan.dbDumpPath());
            
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle.get(), 0);
            updateForeignKeys(dbHandle.get(), 0);
        }
        
        _reader.open();
        
        _logger->info("prepare(): building cluster");
        
        std::queue<std::shared_ptr<std::promise<int>>> tasks;
        std::set<gid_t> replayGids;
        
        while (_reader.nextHeader()) {
            auto header = _reader.txnHeader();
            auto pos = _reader.pos() - sizeof(TransactionHeader);
            
            _reader.nextTransaction();
            auto transaction = _reader.txnBody();
            
            gidIndexWriter.append(pos);
            
            auto promise = taskExecutor.post<int>([this, &graphLock, &rowCluster, &cachedResolver, &replayGids, transaction]() {
                if (!transaction->isRelatedToDatabase(_plan.dbName())) {
                    _logger->trace("skipping transaction #{} because it is not related to database {}",
                                   transaction->gid(), _plan.dbName());
                    return 0;
                }
                
                if (transaction->flags() & Transaction::FLAG_CONTAINS_DDL) {
                    _logger->warn("DDL statement found in transaction #{}, but this version of ultraverse does not support DDL statement yet",
                                  transaction->gid());
                    _logger->warn("transaction #{} will be skipped", transaction->gid());
                    return 0;
                }
                
                rowCluster.insert(transaction, cachedResolver);
                
                if (_plan.isRollbackGid(transaction->gid())) {
                    rowCluster.addRollbackTarget(transaction, cachedResolver);
                }
                
                if (_plan.hasUserQuery(transaction->gid())) {
                    auto userQuery = std::move(loadUserQuery(_plan.userQueries()[transaction->gid()]));
                    
                    rowCluster.addPrependTarget(transaction->gid(), userQuery, cachedResolver);
                }
                
                for (auto &query: transaction->queries()) {
                    if (query->flags() & Query::FLAG_IS_PROCCALL_QUERY) {
                        // FIXME: 프로시저 쿼리 어케할려고?
                        continue;
                    }
                    
                    std::scoped_lock _lock(graphLock);
                    
                    bool isColumnGraphChanged =
                        _columnGraph->add(query->readSet(), READ, _context->foreignKeys) ||
                        _columnGraph->add(query->writeSet(), WRITE, _context->foreignKeys);
                    
                    bool isTableGraphChanged =
                        _tableGraph->addRelationship(query->readSet(), query->writeSet());
                    
                    if (isColumnGraphChanged) {
                        _logger->info("updating column dependency graph");
                        // stateLogWriter << *_columnGraph;
                    }
                    
                    if (isTableGraphChanged) {
                        _logger->info("updating table dependency graph");
                        // stateLogWriter << *_tableGraph;
                    }
                }
                
                if (rowCluster.shouldReplay(transaction->gid())) {
                    _logger->info("shouldReplay({}) returned true", transaction->gid());
                    replayGids.emplace(transaction->gid());
                }
                
                return 0;
            });
            
            tasks.emplace(promise);
        }
        
        while (tasks.empty()) {
            tasks.front()->get_future().wait();
            tasks.pop();
        }
        
        taskExecutor.shutdown();
        
        // rowCluster.describe();
        
        dropIntermediateDB();
    }
}
