//
// Created by cheesekun on 6/20/23.
//

#include <sstream>

#include <execution>
#include <utility>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "utils/StringUtil.hpp"
#include "StateCluster.hpp"

namespace ultraverse::state::v2 {
    
    StateCluster::Cluster::Cluster(): read(), write() {
    
    }
    
    StateCluster::Cluster::Cluster(const StateCluster::Cluster &other):
        read(other.read),
        write(other.write),
        pendingRead(other.pendingRead),
        pendingWrite(other.pendingWrite)
    {
    
    }
    
    decltype(StateCluster::Cluster::read.begin()) StateCluster::Cluster::findByRange(StateCluster::ClusterType type, const StateRange &range) {
        auto &cluster = type == READ ? read : write;
        auto &mutex = type == READ ? readLock : writeLock;
        
        std::scoped_lock _lock(mutex);
        
        return std::find_if(cluster.begin(), cluster.end(), [&range](const auto &pair) {
            return pair.first == range || StateRange::isIntersects(pair.first, range);
        });
    }
    
    decltype(StateCluster::Cluster::pendingRead.begin()) StateCluster::Cluster::pending_findByRange(StateCluster::ClusterType type, const StateRange &range) {
        auto &cluster = type == READ ? pendingRead : pendingWrite;
        auto &mutex = type == READ ? readLock : writeLock;
        
        return std::find_if(std::execution::par_unseq, cluster.begin(), cluster.end(), [&range](const auto &pair) {
            return pair.first == range || StateRange::isIntersects(pair.first, range);
        });
    }
    
    void StateCluster::Cluster::merge(StateCluster::ClusterType type) {
        auto &cluster = type == READ ? pendingRead : pendingWrite;
        auto &mutex = type == READ ? readLock : writeLock;
        
        std::scoped_lock _lock(mutex);
        
        std::vector<std::pair<StateRange, std::unordered_set<gid_t>>> merged;
        
        for (auto &it : cluster) {
            auto &range = it.first;
            auto &gids = it.second;
            
            auto it2 = std::find_if(std::execution::par_unseq, merged.begin(), merged.end(), [&range](const auto &pair) {
                return pair.first == range || StateRange::isIntersects(pair.first, range);
            });
            
            if (it2 == merged.end()) {
                merged.emplace_back(range, gids);
            } else {
                it2->first.OR_FAST(range);
                it2->second.insert(gids.begin(), gids.end());
            }
        }
        
        cluster.clear();
        
        for (auto &pair : merged) {
            cluster.emplace_back(std::move(pair));
        }
        
        merged.clear();
    }
    
    void StateCluster::merge() {
        for (auto &pair : _clusters) {
            auto &cluster = pair.second;
            
            _logger->info("performing merge for {}", pair.first);
            
            cluster.merge(READ);
            cluster.merge(WRITE);
            
            _logger->info("finalizing {}", pair.first);
            
            cluster.finalize();
        }
    }
    
    void StateCluster::Cluster::finalize() {
        for (const auto &pair: pendingRead) {
            read.emplace(pair);
        }
        
        pendingRead.clear();
        
        for (const auto &pair: pendingWrite) {
            write.emplace(pair);
        }
        
        pendingWrite.clear();
    }
    
    
    std::optional<StateRange> StateCluster::Cluster::match(StateCluster::ClusterType type,
                                                           const std::string &columnName,
                                                           const ClusterMap &cluster,
                                                           const std::vector<StateItem> &items,
                                                           const RelationshipResolver &resolver) {
        
        auto it = std::find_if(cluster.begin(), cluster.end(), [type, &resolver, &columnName, &items](const auto &pair) {
            const StateRange &range = pair.first;
            return std::any_of(std::execution::par, items.begin(), items.end(), [type, &resolver, &columnName, &range](const StateItem &item) {
                // FIXME: 이거 개느릴거같은데;;
                
                // Q: 이거 std::move 해야 하지 않나?
                // A: 아니야. 그냥 const & 로 해야해. 그래야 더 빠르거든.
                // Q: 왜?
                // A: std::move 는 rvalue 로 바꿔주는거야. 그래서 이동 생성자를 호출하거든.
                
                // WRITE가 FK에 대해 직접 write하는 경우는 없으므로 chain resolve는 READ에 대해서만 수행한다.

                const auto &real = (type == READ) ?
                    resolver.resolveRowChain(item) :
                    resolver.resolveRowAlias(item);
                
                if (real != nullptr) {
                    return real->name == columnName && StateRange::isIntersects(real->MakeRange2(), range);
                } else {
                    const auto &realColumn = (type == READ) ?
                                             resolver.resolveChain(item.name) :
                                             resolver.resolveColumnAlias(item.name);
                    
                    if (!realColumn.empty()) {
                        return realColumn == columnName && StateRange::isIntersects(item.MakeRange2(), range);
                    } else {
                        return item.name == columnName && StateRange::isIntersects(item.MakeRange2(), range);
                    }
                }
            });
        });
        
        if (it == cluster.end()) {
            return std::nullopt;
        }
        
        return it->first;
    }
    
    std::map<std::string, std::set<std::string>>
    StateCluster::buildKeyColumnsMap(const std::set<std::string> &keyColumns) {
        std::map<std::string, std::set<std::string>> keyColumnsMap;
        
        for (const auto &keyColumn: keyColumns) {
            const auto pair = utility::splitTableName(keyColumn);
            const auto &tableName = pair.first;
            const auto &columnName = pair.second;
            
            keyColumnsMap[tableName].insert(keyColumn);
        }
        
        return std::move(keyColumnsMap);
    }
    
    StateCluster::StateCluster(const std::set<std::string> &keyColumns):
        _logger(createLogger("StateCluster")),
        _keyColumns(keyColumns),
        _keyColumnsMap(std::move(buildKeyColumnsMap(keyColumns))),
        _clusters()
    {
    
    }
    
    const std::set<std::string> &StateCluster::keyColumns() const {
        return _keyColumns;
    }
    
    const std::unordered_map<std::string, StateCluster::Cluster> &StateCluster::clusters() const {
        return _clusters;
    }
    
    bool StateCluster::isKeyColumnItem(const RelationshipResolver &resolver, const StateItem &item) const {
        return std::any_of(
        _keyColumns.begin(), _keyColumns.end(),
            [&resolver, &item](const auto &keyColumn) {
                // 과연 이게 맞는가? alias + foreign key를 고려해야 하지 않을까?
                auto realColumn = resolver.resolveChain(item.name);
                
                return (item.name == keyColumn) ||
                       (!realColumn.empty() && realColumn == keyColumn);
            }
        );
    }
    
    void StateCluster::insert2(StateCluster::ClusterType type, const std::string &columnName, const StateRange &range, gid_t gid) {
        auto &clusterContainer = _clusters[columnName];
        auto &cluster = type == READ ? clusterContainer.pendingRead : clusterContainer.pendingWrite;
        auto &mutex = type == READ ? clusterContainer.readLock : clusterContainer.writeLock;
        
        std::scoped_lock _lock(mutex);
        
        auto it = clusterContainer.pending_findByRange(type, range);
        
        if (it != cluster.end()) {
            StateRange dstRange(it->first);
            dstRange.OR_FAST(range);
            
            if (it->first != dstRange) {
                it->first = dstRange;
            }
            it->second.emplace(gid);
        } else {
            cluster.emplace_back(std::make_pair(range, std::unordered_set<gid_t> { gid }));
        }
    }
    
    std::pair<std::vector<StateItem>, std::vector<StateItem>>
    StateCluster::extractItems(Transaction &transaction, const RelationshipResolver &resolver) const {
        
        std::map<std::string, StateItem> readKeyItems;
        std::map<std::string, StateItem> writeKeyItems;
        
        std::vector<StateItem> _readKeyItems;
        std::vector<StateItem> _writeKeyItems;
        
        auto processFn = [this, &resolver, &readKeyItems, &writeKeyItems](bool isWrite) {
            return [this, &resolver, &readKeyItems, &writeKeyItems, isWrite](const StateItem &item) {
                std::string itemName = utility::toLower(item.name);
                const auto &real = resolver.resolveRowChain(item);
                
                if (real != nullptr) {
                    std::string realName = utility::toLower(real->name);
                    auto &_item = isWrite ? writeKeyItems[realName] : readKeyItems[realName];
                    
                    if (_item.name.empty()) {
                        _item.name = realName;
                        _item.function_type = FUNCTION_IN_INTERNAL;
                    }
                    
                    _item.data_list.insert(
                        _item.data_list.end(),
                        real->data_list.begin(), real->data_list.end()
                        );
                } else {
                    const auto &realColumn = utility::toLower(resolver.resolveChain(item.name));
                    
                    if (!realColumn.empty()) {
                        // real row를 해결하지 못했지만, realColumn은 해결한 경우 => 즉, foreignKey인 경우
                        auto &_item = isWrite ? writeKeyItems[itemName] : readKeyItems[realColumn];
                        
                        if (_item.name.empty()) {
                            _item.name = realColumn;
                            _item.function_type = FUNCTION_IN_INTERNAL;
                        }
                        
                        _item.data_list.insert(
                            _item.data_list.end(),
                            item.data_list.begin(), item.data_list.end()
                        );
                    } else if (_keyColumns.find(itemName) != _keyColumns.end()) {
                        // real row도 해결하지 못하고, realColumn도 해결하지 못한 경우 => keyColumn인 경우
                        auto &_item = isWrite ? writeKeyItems[itemName] : readKeyItems[itemName];
                        
                        if (_item.name.empty()) {
                            _item.name = itemName;
                            _item.function_type = FUNCTION_IN_INTERNAL;
                        }
                        
                        _item.data_list.insert(
                            _item.data_list.end(),
                            item.data_list.begin(), item.data_list.end()
                        );
                    }
                }
            };
        };
        
        {
            auto it = transaction.readSet_begin();
            const auto &end = it.end();
            
            std::for_each(it, end, processFn(false));
        }
        
        {
            auto it = transaction.writeSet_begin();
            const auto &end = it.end();
            
            std::for_each(it, end, processFn(true));
        }
        
        
        for (const auto &pair: _keyColumnsMap) {
            const auto &table = pair.first;
            const auto &keyColumns = pair.second;
            
            {
                std::set<std::string> foundReadColumns;
                
                for (const auto &keyColumn: keyColumns) {
                    if (readKeyItems.find(keyColumn) != readKeyItems.end()) {
                        foundReadColumns.insert(keyColumn);
                    }
                }
                
                if (foundReadColumns.empty() || foundReadColumns.size() == keyColumns.size()) {
                    continue;
                }
                
                for (const auto &keyColumn: keyColumns) {
                    if (foundReadColumns.find(keyColumn) != foundReadColumns.end()) {
                        continue;
                    }
                    
                    readKeyItems[keyColumn] = StateItem::Wildcard(keyColumn);
                }
            }
            {
                std::set<std::string> foundWriteColumns;
                
                for (const auto &keyColumn: keyColumns) {
                    if (writeKeyItems.find(keyColumn) != writeKeyItems.end()) {
                        foundWriteColumns.insert(keyColumn);
                    }
                }
                
                if (foundWriteColumns.empty() || foundWriteColumns.size() == keyColumns.size()) {
                    continue;
                }
                
                for (const auto &keyColumn: keyColumns) {
                    if (foundWriteColumns.find(keyColumn) != foundWriteColumns.end()) {
                        continue;
                    }
                    
                    writeKeyItems[keyColumn] = StateItem::Wildcard(keyColumn);
                }
            }
        }
        
        // insert all values to vector
        std::transform(
            readKeyItems.begin(), readKeyItems.end(),
            std::back_inserter(_readKeyItems),
            [](const auto &pair) {
                return pair.second;
            }
        );
        
        std::transform(
            writeKeyItems.begin(), writeKeyItems.end(),
            std::back_inserter(_writeKeyItems),
            [](const auto &pair) {
                return pair.second;
            }
        );
        
        return std::make_pair(
            std::move(_readKeyItems),
            std::move(_writeKeyItems)
        );
    }
    
    
    void StateCluster::insert(StateCluster::ClusterType type, const std::vector<StateItem> &items, gid_t gid) {
        std::for_each(items.begin(), items.end(), [this, type, gid](const auto &item) {
            insert2(type, item.name, item.MakeRange2(), gid);
        });
    }
    
    void StateCluster::insert(const std::shared_ptr<Transaction>& transaction, const RelationshipResolver &resolver) {
        const auto &rwItemsPair = extractItems(*transaction, resolver);
        
        insert(READ, rwItemsPair.first, transaction->gid());
        insert(WRITE, rwItemsPair.second, transaction->gid());
    }
    
    std::optional<StateRange> StateCluster::match(StateCluster::ClusterType type, const std::string &columnName,
                                                  const std::shared_ptr<Transaction> &transaction,
                                                  const RelationshipResolver &resolver) const {
        
        if (_clusters.find(columnName) == _clusters.end()) {
            return std::nullopt;
        }
        
        const auto &cluster = _clusters.at(columnName);
        const auto &rwItemsPair = extractItems(*transaction, resolver);
        
        if (type == READ) {
            return std::move(StateCluster::Cluster::match(READ, columnName, cluster.read, rwItemsPair.first, resolver));
        }
        
        if (type == WRITE) {
            return std::move(StateCluster::Cluster::match(WRITE, columnName, cluster.write, rwItemsPair.second, resolver));
        }
        
        return std::nullopt;
    }
    
    void StateCluster::describe() {
        std::cerr << "StateCluster::describe()" << std::endl;
        
        for (const auto &pair : _clusters) {
            std::cerr << "[" << pair.first << "]" << std::endl;
            std::cerr << "  READ" << std::endl;
            for (const auto &pair2 : pair.second.read) {
                std::cerr << "    " << pair2.first.MakeWhereQuery(pair.first) << " => ";
                for (const auto &gid : pair2.second) {
                    std::cerr << gid << ", ";
                }
                std::cerr << std::endl;
            }
            
            std::cerr << "  WRITE" << std::endl;
            for (const auto &pair2 : pair.second.read) {
                std::cerr << "    " << pair2.first.MakeWhereQuery(pair.first) << " => ";
                for (const auto &gid : pair2.second) {
                    std::cerr << gid << ", ";
                }
                std::cerr << std::endl;
            }
            std::cerr << std::endl;
        }
    }
    
    void StateCluster::addRollbackTarget(const std::shared_ptr<Transaction> &transaction,
                                         const RelationshipResolver &resolver,
                                         bool revalidate) {
        std::scoped_lock lock(_targetCacheLock);
        
        _rollbackTargets.insert(std::make_pair(transaction->gid(), TargetTransactionCache {
            transaction,
            {}, {}
        }));
        
        if (revalidate) {
            invalidateTargetCache(_rollbackTargets, resolver);
        }
    }
    
    void StateCluster::addPrependTarget(gid_t gid,
                                        const std::shared_ptr<Transaction> &transaction,
                                        const RelationshipResolver &resolver) {
        std::scoped_lock lock(_targetCacheLock);
        
        _prependTargets.insert(std::make_pair(gid, TargetTransactionCache {
            transaction,
            {}, {}
        }));
        
        invalidateTargetCache(_prependTargets, resolver);
    }
    
    void StateCluster::invalidateTargetCache(std::unordered_map<gid_t, TargetTransactionCache> &targets, const RelationshipResolver &resolver) {
    
#ifdef STATECLUSTER_USE_NEW_APPROACH
        _logger->debug("invalidateTargetCache() called");
        _replayTargets.clear();
        
        for (auto &pair: targets) {
            gid_t gid = pair.first;
            
            for (const auto &cluster: _clusters) {
                if (keyColumns().find(cluster.first) == keyColumns().end()) {
                    continue;
                }
                
                const auto &read = cluster.second.read;
                const auto &write = cluster.second.write;
                
                for (const auto &writePair: write) {
                    const auto &range = writePair.first;
                    const auto &gids = writePair.second;
                    
                    if (gids.find(gid) == gids.end()) {
                        continue;
                    }
                    
                    const auto &depRangeIt = std::find_if(read.begin(), read.end(), [&range](const auto &pair) {
                        return pair.first == range || StateRange::isIntersects(pair.first, range);
                    });
                    
                    
                    if (depRangeIt == read.end()) {
                        continue;
                    }
                    
                    pair.second.read[cluster.first] = depRangeIt->first;
                    
                    const auto &depGids = depRangeIt->second;
                    _replayTargets.insert(depGids.begin(), depGids.end());
                }
            }
        }
        
        for (auto &pair: targets) {
            gid_t gid = pair.first;
            _replayTargets.erase(gid);
        }
        
        _logger->debug("invalidateTargetCache() end");
#else
        _targetCache.clear();
        
        for (auto &pair: targets) {
            auto &cache = pair.second;
            
            for (const auto &keyColumn: _keyColumns) {
                std::string column = resolver.resolveChain(keyColumn);
                
                if (column.empty()) {
                    column = keyColumn;
                }
                
                // auto readRange = match(READ, keyColumn, cache.transaction, resolver);
                auto writeRange = match(WRITE, column, cache.transaction, resolver);
                
                /*
                if (readRange.has_value()) {
                    cache.read[keyColumn] = readRange.value();
                }
                 */
                
                if (writeRange.has_value()) {
                    cache.write[column] = writeRange.value();
                    std::unordered_map<StateRange, std::reference_wrapper<const std::unordered_set<gid_t>>>
                        &cacheMap = _targetCache[column];
                    if (cacheMap.find(writeRange.value()) == cacheMap.end()) {
                        auto range = writeRange.value();
                        const auto &cluster = _clusters.at(column);
                        auto it = std::find_if(std::execution::par_unseq, cluster.read.begin(), cluster.read.end(),
                                               [this, &range](const auto &pair) {
                                                   return pair.first == range ||
                                                          StateRange::isIntersects(pair.first, range);
                                               });
                        
                        if (it != cluster.read.end()) {
                            const std::unordered_set<gid_t> &gids = it->second;
                            cacheMap.emplace(range, std::ref(gids));
                            
                            cache.read[column] = it->first;
                        }
                    }
                }
            }
        }
#endif
    }
    
    bool StateCluster::shouldReplay(gid_t gid) {
        if (_rollbackTargets.find(gid) != _rollbackTargets.end()) {
            // 롤백 타겟 자신은 재실행되어선 안된다
            return false;
        }

#ifdef STATECLUSTER_USE_NEW_APPROACH
        return _replayTargets.find(gid) != _replayTargets.end();
#else
        size_t matched = 0;
        
        for (const auto &pair: _keyColumnsMap) {
            const auto &keyColumns = pair.second;
            
            size_t count = 0;
            
            for (const auto &keyColumn: keyColumns) {
                if (_targetCache.find(keyColumn) == _targetCache.end()) {
                    continue;
                }
                
                const auto &ranges = _targetCache[keyColumn];
                
                if (std::any_of(ranges.begin(), ranges.end(), [gid](const auto &pair) {
                    const auto &gids = pair.second.get();
                    return gids.find(gid) != gids.end();
                })) {
                    count++;
                }
            }
            
            if (count == 0) {
                continue;
            } else if (count == keyColumns.size()) {
                matched++;
            } else {
                return false;
            }
        }
        
        return matched > 0;
#endif
    }
    
    std::string StateCluster::generateReplaceQuery(const std::string &targetDB, const std::string &intermediateDB, const RelationshipResolver &resolver) {
        std::string query = fmt::format("use {};\nSET FOREIGN_KEY_CHECKS=0;\n", targetDB);
        
        for (const auto &pair: _keyColumnsMap) {
            const auto &tableName = pair.first;
            const auto &keyColumns = pair.second;
            
            size_t i = 0;
            bool changed = false;
            bool isWildcard = false;
            
            std::vector<std::string> where;
            
            for (const auto &keyColumn: keyColumns) {
                std::string resolvedColumn = resolver.resolveChain(keyColumn);
                std::vector<std::string> conds;
                
                if (resolvedColumn.empty()) {
                    resolvedColumn = keyColumn;
                }
                
                {
                    size_t j = 0;
                    
                    auto it = _rollbackTargets.begin();
                    
                    while (true) {
                        it = std::find_if(
                            it, _rollbackTargets.end(),
                            [&resolvedColumn](const auto &pair) {
                                return pair.second.read.find(resolvedColumn) != pair.second.read.end();
                            }
                       );
                        
                        if (it == _rollbackTargets.end()) {
                            break;
                        }
                        
                        const auto &range = it->second.read.at(resolvedColumn);
                        
                        if (range.wildcard()) {
                            isWildcard = true;
                            goto NEXT_COLUMN;
                        } else {
                            changed = true;
                        }
                        
                        conds.push_back(fmt::format("({})", range.MakeWhereQuery(keyColumn)));
                        
                        it++;
                    }
                    
                }
                
                if (!conds.empty()) {
                    where.push_back(fmt::format("{}", fmt::join(conds, " OR ")));
                }
                
                NEXT_COLUMN:
                i++;
            }
            

            
            if (isWildcard) {
                query += fmt::format("TRUNCATE {};\n", tableName);
                query += fmt::format("REPLACE INTO {} SELECT * FROM {}.{};\n", tableName, intermediateDB, tableName);
            } else if (changed) {
                std::string _where = fmt::format("{}", fmt::join(where, " AND "));
                
                query += fmt::format("DELETE FROM {} WHERE {};\n", tableName, _where);
                query += fmt::format("REPLACE INTO {} SELECT * FROM {}.{} WHERE {};\n", tableName, intermediateDB, tableName, _where);
            }

            query += "\n";
        }
        
        query += "\nSET FOREIGN_KEY_CHECKS=1;\n";
        
        return std::move(query);
    }
    
    bool StateCluster::shouldReplay(gid_t gid, const StateCluster::TargetTransactionCache &cache) {
        const auto &read = cache.read;
        const auto &write = cache.write;
        
        
        const std::function<bool(ClusterType, const std::pair<std::string, StateRange> &)> containsGid = [this, gid](ClusterType type, const auto &pair) {
            const auto &columnName = pair.first;
            const auto &range = pair.second;
            
            const auto &cluster = _clusters.at(columnName);
            auto it = std::find_if(std::execution::par_unseq, cluster.read.begin(), cluster.read.end(), [this, &range](const auto &pair) {
                return pair.first == range || StateRange::isIntersects(pair.first, range);
            });
            
            if (it != cluster.read.end()){
                // std::scoped_lock _lock(_clusterInsertionLock);
                const auto &gids = it->second;
                
                return gids.find(gid) != gids.end();
                /*
                if (gids.find(gid) != gids.end()) {
                    // FIXME: 속도 졸라느려지므로 아래 로그 제거해야 함
                    // _logger->debug("shouldReplay({}): matched with {} ({} - READ)", gid, range.MakeWhereQuery(columnName), type == READ ? "READ" : "WRITE");
                    return true;
                }
                 */
            }
            
            /*
            if (cluster.write.find(range) != cluster.write.end()) {
                const auto &gids = _clusters.at(columnName).write.at(range);
                if (gids.find(gid) != gids.end()) {
                    // FIXME: 속도 졸라느려지므로 아래 로그 제거해야 함
                    _logger->debug("shouldReplay({}): matched with {} ({} - WRITE)", gid, range.MakeWhereQuery(columnName), type == READ ? "READ" : "WRITE");
                    return true;
                }
            }
            */
            
            return false;
        };
        
        return (
            // std::any_of(read.begin(), read.end(), [&containsGid](const auto &pair) { return containsGid(READ, pair); }) ||
            std::any_of(write.begin(), write.end(), [&containsGid](const auto &pair) { return containsGid(WRITE, pair); })
        );
    }
}
