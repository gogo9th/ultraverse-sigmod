//
// Created by cheesekun on 6/20/23.
//

#include <sstream>

#include <execution>
#include <utility>

#include <fmt/format.h>

#include "utils/StringUtil.hpp"
#include "StateCluster.hpp"

namespace ultraverse::state::v2 {
    
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
    
    void StateCluster::insert(StateCluster::ClusterType type, const std::string &columnName, const StateRange &range, gid_t gid) {
        auto &cluster = type == READ ?
            _clusters[columnName].read :
            _clusters[columnName].write;
        
        std::scoped_lock lock(_clusterInsertionLock);
        auto it = std::find_if(std::execution::par_unseq, cluster.begin(), cluster.end(), [&range](const auto &pair) {
            return pair.first == range || StateRange::isIntersects(pair.first, range);
        });
        
        if (it != cluster.end()) {
            auto dstRange = it->first;
            dstRange.OR_FAST(range);
            
            if (dstRange == range || it->first == dstRange) {
                it->second.emplace(gid);
            } else {
                // replace key using std::extract (see https://en.cppreference.com/w/cpp/container/map/extract)
                auto range1 = it->first;
                
                // _logger->trace("merging range: {} and {}", range1.MakeWhereQuery(columnName), range.MakeWhereQuery(columnName));
                
                auto it2 = cluster.begin();
                while (true) {
                    it2 = std::find_if(std::execution::par_unseq, it2, cluster.end(), [&range1, &range](const auto &pair) {
                        return pair.first != range1 && StateRange::isIntersects(pair.first, range);
                    });
                    
                    if (it2 == cluster.end()) {
                        break;
                    }
                    
                    // _logger->trace("merging range: {} and {}", range1.MakeWhereQuery(columnName), it2->first.MakeWhereQuery(columnName));
                    
                    cluster[range1].insert(it2->second.begin(), it2->second.end());
                    dstRange.OR_FAST(it2->first);
                    
                    it2->second.clear();
                    it2 = cluster.erase(it2);
                }
                
                auto node = cluster.extract(range1);
                
                {
                    node.key() = dstRange;
                    node.mapped().emplace(gid);
                    
                    cluster.insert(std::move(node));
                }
            }
        } else {
            auto &_cluster = cluster[range];
            
            // _cluster.reserve(16384);
            _cluster.emplace(gid);
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
                const auto &real = resolver.resolveRowChain(item);
                
                if (real != nullptr) {
                    auto &_item = readKeyItems[real->name];
                    
                    if (_item.name.empty()) {
                        _item.name = real->name;
                        _item.function_type = FUNCTION_IN_INTERNAL;
                    }
                    
                    _item.data_list.insert(
                        _item.data_list.end(),
                        item.data_list.begin(), item.data_list.end()
                        );
                } else {
                    const auto &realColumn = resolver.resolveChain(item.name);
                    
                    if (!realColumn.empty()) {
                        // real row를 해결하지 못했지만, realColumn은 해결한 경우 => 즉, foreignKey인 경우
                        auto &_item = readKeyItems[realColumn];
                        
                        if (_item.name.empty()) {
                            _item.name = realColumn;
                            _item.function_type = FUNCTION_IN_INTERNAL;
                        }
                        
                        _item.data_list.insert(
                            _item.data_list.end(),
                            item.data_list.begin(), item.data_list.end()
                        );
                    } else if (_keyColumns.find(item.name) != _keyColumns.end()) {
                        // real row도 해결하지 못하고, realColumn도 해결하지 못한 경우 => keyColumn인 경우
                        auto &_item = isWrite ? writeKeyItems[item.name] : readKeyItems[item.name];
                        
                        if (_item.name.empty()) {
                            _item.name = item.name;
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
            insert(type, item.name, item.MakeRange2(), gid);
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
        _targetCache.clear();
        
        for (auto &pair: targets) {
            auto &cache = pair.second;
            
            for (const auto &keyColumn: _keyColumns) {
                // auto readRange = match(READ, keyColumn, cache.transaction, resolver);
                auto writeRange = match(WRITE, keyColumn, cache.transaction, resolver);
                
                /*
                if (readRange.has_value()) {
                    cache.read[keyColumn] = readRange.value();
                }
                 */
                
                if (writeRange.has_value()) {
                    cache.write[keyColumn] = writeRange.value();
                    std::unordered_map<StateRange, std::reference_wrapper<const std::unordered_set<gid_t>>>
                        &cacheMap = _targetCache[keyColumn];
                    if (cacheMap.find(writeRange.value()) == cacheMap.end()) {
                        auto range = writeRange.value();
                        const auto &cluster = _clusters.at(keyColumn);
                        auto it = std::find_if(std::execution::par_unseq, cluster.read.begin(), cluster.read.end(), [this, &range](const auto &pair) {
                            return pair.first == range || StateRange::isIntersects(pair.first, range);
                        });
                        
                        const std::unordered_set<gid_t> &gids = it->second;
                        cacheMap.emplace(range, std::ref(gids));
                    }
                }
            }
        }
    }
    
    bool StateCluster::shouldReplay(gid_t gid) {
        if (_rollbackTargets.find(gid) != _rollbackTargets.end()) {
            // 롤백 타겟 자신은 재실행되어선 안된다
            return false;
        }
        
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
    }
    
    std::string StateCluster::generateReplaceQuery(const std::string &targetDB, const std::string &intermediateDB) {
        std::string query = fmt::format("use {};\nSET FOREIGN_KEY_CHECKS=0;\n", targetDB);
        
        for (const auto &pair: _keyColumnsMap) {
            const auto &tableName = pair.first;
            const auto &keyColumns = pair.second;
            std::stringstream sstream;
            
            sstream << fmt::format("REPLACE INTO {} SELECT * FROM {}.{} WHERE ", tableName, intermediateDB, tableName);
            
            size_t i = 0;
            bool changed = false;
            
            for (const auto &keyColumn: keyColumns) {
                if (_targetCache.find(keyColumn) == _targetCache.end()) {
                    goto NEXT_COLUMN;
                }
                
                {
                    const auto &ranges = _targetCache[keyColumn];
                    
                    if (ranges.empty()) {
                        goto NEXT_COLUMN;
                    }
                    
                    changed = true;
                    
                    if (ranges.size() == 1 && ranges.begin()->first.wildcard()) {
                        // TODO: mark as wildcard
                        goto NEXT_COLUMN;
                    }
                    
                    sstream << "(";
                    
                    size_t j = 0;
                    
                    for (const auto &range: ranges) {
                        sstream << "(";
                        sstream << range.first.MakeWhereQuery(keyColumn);
                        sstream << ")";
                        
                        if (j++ != ranges.size() - 1) {
                            sstream << " OR ";
                        }
                    }
                    
                    sstream << ")";
                    
                    if (i++ != keyColumns.size() - 1) {
                        sstream << " AND ";
                    }
                }
                
                NEXT_COLUMN:
                i++;
            }
            
            sstream << ";\n";
            
            if (changed) {
                query += sstream.str();
            }
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
