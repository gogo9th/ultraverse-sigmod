//
// Created by cheesekun on 6/20/23.
//

#include <algorithm>
#include <sstream>

#include <execution>
#include <utility>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "../StateChangeContext.hpp"
#include "utils/StringUtil.hpp"
#include "StateCluster.hpp"

#include "ultraverse_state.pb.h"

namespace ultraverse::state::v2 {

    namespace {
        constexpr size_t kParallelMinItems = 256;

        std::set<std::string> normalizeKeyColumns(const std::set<std::string> &keyColumns) {
            std::set<std::string> normalized;
            for (const auto &keyColumn : keyColumns) {
                normalized.insert(utility::toLower(keyColumn));
            }
            return normalized;
        }

        std::vector<std::vector<std::string>> normalizeKeyColumnGroups(
            const std::set<std::string> &keyColumns,
            const std::vector<std::vector<std::string>> &keyColumnGroups) {
            std::vector<std::vector<std::string>> normalizedGroups;
            std::unordered_set<std::string> usedColumns;

            auto appendGroup = [&](const std::vector<std::string> &group) {
                std::vector<std::string> normalizedGroup;
                for (const auto &column : group) {
                    auto normalized = utility::toLower(column);
                    if (normalized.empty()) {
                        continue;
                    }
                    if (!usedColumns.insert(normalized).second) {
                        continue;
                    }
                    normalizedGroup.push_back(std::move(normalized));
                }
                if (!normalizedGroup.empty()) {
                    normalizedGroups.push_back(std::move(normalizedGroup));
                }
            };

            for (const auto &group : keyColumnGroups) {
                appendGroup(group);
            }

            for (const auto &column : keyColumns) {
                auto normalized = utility::toLower(column);
                if (normalized.empty()) {
                    continue;
                }
                if (usedColumns.insert(normalized).second) {
                    normalizedGroups.push_back({normalized});
                }
            }

            return normalizedGroups;
        }

        std::vector<bool> buildGroupCompositeFlags(const std::vector<std::vector<std::string>> &groups) {
            std::vector<bool> flags;
            flags.reserve(groups.size());

            for (const auto &group : groups) {
                if (group.size() <= 1) {
                    flags.push_back(false);
                    continue;
                }

                std::string tableName;
                bool sameTable = true;
                for (const auto &column : group) {
                    const auto pair = utility::splitTableName(column);
                    if (pair.first.empty()) {
                        sameTable = false;
                        break;
                    }
                    if (tableName.empty()) {
                        tableName = pair.first;
                    } else if (tableName != pair.first) {
                        sameTable = false;
                        break;
                    }
                }

                flags.push_back(sameTable && !tableName.empty());
            }

            return flags;
        }

        std::unordered_map<std::string, std::vector<StateCluster::GroupProjection>> buildKeyColumnGroupsByTable(
            const std::vector<std::vector<std::string>> &keyColumnGroups) {
            std::unordered_map<std::string, std::vector<StateCluster::GroupProjection>> mapping;

            for (size_t index = 0; index < keyColumnGroups.size(); index++) {
                const auto &group = keyColumnGroups[index];
                if (group.empty()) {
                    continue;
                }

                std::unordered_map<std::string, std::vector<std::string>> projections;
                for (const auto &column : group) {
                    const auto pair = utility::splitTableName(column);
                    if (pair.first.empty()) {
                        continue;
                    }
                    projections[pair.first].push_back(column);
                }

                for (auto &pair : projections) {
                    if (pair.second.empty()) {
                        continue;
                    }
                    StateCluster::GroupProjection projection;
                    projection.groupIndex = index;
                    projection.columns = std::move(pair.second);
                    mapping[pair.first].push_back(std::move(projection));
                }
            }

            return mapping;
        }

        std::string normalizeColumnName(const RelationshipResolver &resolver, const std::string &column) {
            std::string resolved = resolver.resolveChain(column);
            if (resolved.empty()) {
                resolved = column;
            }
            return utility::toLower(resolved);
        }
    }
    
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
        return std::find_if(cluster.begin(), cluster.end(), [&range](const auto &pair) {
            return pair.first == range || StateRange::isIntersects(pair.first, range);
        });
    }
    
    void StateCluster::Cluster::merge(StateCluster::ClusterType type) {
        auto &cluster = type == READ ? pendingRead : pendingWrite;
        auto &mutex = type == READ ? readLock : writeLock;
        
        std::scoped_lock _lock(mutex);
        
        std::vector<std::pair<StateRange, std::unordered_set<gid_t>>> merged;
        merged.reserve(cluster.size());
        
        for (auto &it : cluster) {
            auto &range = it.first;
            auto &gids = it.second;
            
            auto it2 = std::find_if(merged.begin(), merged.end(), [&range](const auto &pair) {
                return pair.first == range || StateRange::isIntersects(pair.first, range);
            });
            
            if (it2 == merged.end()) {
                merged.emplace_back(range, gids);
            } else {
                it2->first.OR_FAST(range);
                it2->second.insert(gids.begin(), gids.end());
            }
        }
        
        cluster = std::move(merged);
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
        for (auto &pair: pendingRead) {
            read.emplace(std::move(pair.first), std::move(pair.second));
        }
        
        pendingRead.clear();
        
        for (auto &pair: pendingWrite) {
            write.emplace(std::move(pair.first), std::move(pair.second));
        }
        
        pendingWrite.clear();
    }
    
    
    std::optional<StateRange> StateCluster::Cluster::match(StateCluster::ClusterType type,
                                                           const std::string &columnName,
                                                           const ClusterMap &cluster,
                                                           const std::vector<StateItem> &items,
                                                           const RelationshipResolver &resolver) {
        const bool useParallel = items.size() >= kParallelMinItems;
        auto it = std::find_if(cluster.begin(), cluster.end(), [type, &resolver, &columnName, &items, useParallel](const auto &pair) {
            const StateRange &range = pair.first;
            if (useParallel) {
                return std::any_of(std::execution::par, items.begin(), items.end(), [type, &resolver, &columnName, &range](const StateItem &item) {
                    // FIXME: 이거 개느릴거같은데;;
                    
                    // Q: 이거 std::move 해야 하지 않나?
                    // A: 아니야. 그냥 const & 로 해야해. 그래야 더 빠르거든.
                    // Q: 왜?
                    // A: std::move 는 rvalue 로 바꿔주는거야. 그래서 이동 생성자를 호출하거든.
                    
                    const auto &real = resolver.resolveRowChain(item);
                    
                    if (real != nullptr) {
                        return real->name == columnName && StateRange::isIntersects(real->MakeRange2(), range);
                    } else {
                        const auto &realColumn = resolver.resolveChain(item.name);
                        
                        if (!realColumn.empty()) {
                            return realColumn == columnName && StateRange::isIntersects(item.MakeRange2(), range);
                        } else {
                            return item.name == columnName && StateRange::isIntersects(item.MakeRange2(), range);
                        }
                    }
                });
            }

            return std::any_of(items.begin(), items.end(), [type, &resolver, &columnName, &range](const StateItem &item) {
                // FIXME: 이거 개느릴거같은데;;
                
                // Q: 이거 std::move 해야 하지 않나?
                // A: 아니야. 그냥 const & 로 해야해. 그래야 더 빠르거든.
                // Q: 왜?
                // A: std::move 는 rvalue 로 바꿔주는거야. 그래서 이동 생성자를 호출하거든.
                
                const auto &real = resolver.resolveRowChain(item);
                
                if (real != nullptr) {
                    return real->name == columnName && StateRange::isIntersects(real->MakeRange2(), range);
                } else {
                    const auto &realColumn = resolver.resolveChain(item.name);
                    
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
    
    StateCluster::StateCluster(const std::set<std::string> &keyColumns,
                               const std::vector<std::vector<std::string>> &keyColumnGroups):
        _logger(createLogger("StateCluster")),
        _keyColumns(normalizeKeyColumns(keyColumns)),
        _keyColumnGroups(normalizeKeyColumnGroups(keyColumns, keyColumnGroups)),
        _groupIsComposite(buildGroupCompositeFlags(_keyColumnGroups)),
        _keyColumnGroupsByTable(buildKeyColumnGroupsByTable(_keyColumnGroups)),
        _clusters()
    {
        _keyColumns.clear();
        for (const auto &group : _keyColumnGroups) {
            for (const auto &column : group) {
                _keyColumns.insert(column);
            }
        }

        _clusters.reserve(_keyColumns.size() * 2);
        for (const auto &keyColumn : _keyColumns) {
            _clusters.emplace(keyColumn, Cluster{});
        }
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
        auto it = _clusters.find(columnName);
        if (it == _clusters.end()) {
            return;
        }
        auto &clusterContainer = it->second;
        auto &cluster = type == READ ? clusterContainer.pendingRead : clusterContainer.pendingWrite;
        auto &mutex = type == READ ? clusterContainer.readLock : clusterContainer.writeLock;
        
        std::scoped_lock _lock(mutex);
        
        auto it2 = clusterContainer.pending_findByRange(type, range);
        
        if (it2 != cluster.end()) {
            StateRange dstRange(it2->first);
            dstRange.OR_FAST(range);
            
            if (it2->first != dstRange) {
                it2->first = dstRange;
            }
            it2->second.emplace(gid);
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
        
        auto mergeItem = [&readKeyItems, &writeKeyItems](bool isWrite, StateItem candidate) {
            auto &target = isWrite ? writeKeyItems : readKeyItems;
            std::string key = utility::toLower(candidate.name);
            candidate.name = key;

            auto it = target.find(key);
            if (it == target.end()) {
                target.emplace(key, std::move(candidate));
                return;
            }

            auto isWildcard = [](const StateItem &item) {
                return item.function_type == FUNCTION_WILDCARD;
            };

            if (isWildcard(it->second)) {
                return;
            }
            if (isWildcard(candidate)) {
                it->second = std::move(candidate);
                return;
            }

            StateItem merged;
            merged.name = key;
            merged.condition_type = EN_CONDITION_OR;
            merged.function_type = FUNCTION_NONE;

            auto appendArgs = [&merged](const StateItem &item) {
                if (item.condition_type == EN_CONDITION_OR) {
                    merged.arg_list.insert(merged.arg_list.end(), item.arg_list.begin(), item.arg_list.end());
                } else {
                    merged.arg_list.push_back(item);
                }
            };

            appendArgs(it->second);
            appendArgs(candidate);

            it->second = std::move(merged);
        };

        auto processFn = [this, &resolver, &mergeItem](bool isWrite) {
            return [this, &resolver, &mergeItem, isWrite](const StateItem &item) {
                std::string itemName = utility::toLower(item.name);
                const auto &real = resolver.resolveRowChain(item);

                if (real != nullptr) {
                    StateItem resolved = *real;
                    resolved.name = utility::toLower(real->name);
                    mergeItem(isWrite, std::move(resolved));
                } else {
                    const auto &realColumn = utility::toLower(resolver.resolveChain(item.name));

                    if (!realColumn.empty()) {
                        // real row를 해결하지 못했지만, realColumn은 해결한 경우 => 즉, foreignKey인 경우
                        StateItem resolved = item;
                        resolved.name = realColumn;
                        mergeItem(isWrite, std::move(resolved));
                    } else if (_keyColumns.find(itemName) != _keyColumns.end()) {
                        // real row도 해결하지 못하고, realColumn도 해결하지 못한 경우 => keyColumn인 경우
                        StateItem resolved = item;
                        resolved.name = itemName;
                        mergeItem(isWrite, std::move(resolved));
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
        
        
        for (size_t groupIndex = 0; groupIndex < _keyColumnGroups.size(); groupIndex++) {
            if (groupIndex >= _groupIsComposite.size() || !_groupIsComposite[groupIndex]) {
                continue;
            }
            const auto &group = _keyColumnGroups[groupIndex];
            if (group.empty()) {
                continue;
            }

            {
                std::set<std::string> foundReadColumns;
                for (const auto &keyColumn : group) {
                    if (readKeyItems.find(keyColumn) != readKeyItems.end()) {
                        foundReadColumns.insert(keyColumn);
                    }
                }

                if (!foundReadColumns.empty() && foundReadColumns.size() != group.size()) {
                    for (const auto &keyColumn : group) {
                        if (foundReadColumns.find(keyColumn) != foundReadColumns.end()) {
                            continue;
                        }
                        readKeyItems[keyColumn] = StateItem::Wildcard(keyColumn);
                    }
                }
            }

            {
                std::set<std::string> foundWriteColumns;
                for (const auto &keyColumn : group) {
                    if (writeKeyItems.find(keyColumn) != writeKeyItems.end()) {
                        foundWriteColumns.insert(keyColumn);
                    }
                }

                if (!foundWriteColumns.empty() && foundWriteColumns.size() != group.size()) {
                    for (const auto &keyColumn : group) {
                        if (foundWriteColumns.find(keyColumn) != foundWriteColumns.end()) {
                            continue;
                        }
                        writeKeyItems[keyColumn] = StateItem::Wildcard(keyColumn);
                    }
                }
            }
        }
        
        // insert all values to vector
        _readKeyItems.reserve(readKeyItems.size());
        std::transform(
            readKeyItems.begin(), readKeyItems.end(),
            std::back_inserter(_readKeyItems),
            [](const auto &pair) {
                return pair.second;
            }
        );
        
        _writeKeyItems.reserve(writeKeyItems.size());
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
        std::unique_lock<std::shared_mutex> lock(_targetCacheLock);
        
        _rollbackTargets.insert(std::make_pair(transaction->gid(), TargetTransactionCache {
            transaction,
            {}, {}
        }));
        
        if (revalidate) {
            invalidateTargetCache(resolver);
        }
    }
    
    void StateCluster::addPrependTarget(gid_t gid,
                                        const std::shared_ptr<Transaction> &transaction,
                                        const RelationshipResolver &resolver) {
        std::unique_lock<std::shared_mutex> lock(_targetCacheLock);
        
        _prependTargets.insert(std::make_pair(gid, TargetTransactionCache {
            transaction,
            {}, {}
        }));
        
        invalidateTargetCache(resolver);
    }

    void StateCluster::normalizeWithResolver(const RelationshipResolver &resolver) {
        std::vector<std::vector<std::string>> normalizedGroups;
        normalizedGroups.reserve(_keyColumnGroups.size());
        std::unordered_set<std::string> usedColumns;

        for (const auto &group : _keyColumnGroups) {
            if (group.empty()) {
                continue;
            }

            std::vector<std::string> normalizedGroup;
            normalizedGroup.reserve(group.size());
            for (const auto &column : group) {
                auto normalized = normalizeColumnName(resolver, column);
                if (normalized.empty()) {
                    continue;
                }
                if (!usedColumns.insert(normalized).second) {
                    continue;
                }
                normalizedGroup.push_back(std::move(normalized));
            }

            if (!normalizedGroup.empty()) {
                normalizedGroups.push_back(std::move(normalizedGroup));
            }
        }

        _keyColumnGroups = std::move(normalizedGroups);
        _groupIsComposite = buildGroupCompositeFlags(_keyColumnGroups);
        _keyColumnGroupsByTable = buildKeyColumnGroupsByTable(_keyColumnGroups);

        _keyColumns.clear();
        for (const auto &group : _keyColumnGroups) {
            for (const auto &column : group) {
                _keyColumns.insert(column);
            }
        }

        std::unordered_map<std::string, Cluster> normalizedClusters;
        normalizedClusters.reserve(std::max(_clusters.size(), _keyColumns.size()));

        auto appendCluster = [&normalizedClusters](const std::string &key, const Cluster &cluster) {
            auto &target = normalizedClusters[key];
            for (const auto &pair : cluster.read) {
                target.pendingRead.emplace_back(pair.first, pair.second);
            }
            for (const auto &pair : cluster.write) {
                target.pendingWrite.emplace_back(pair.first, pair.second);
            }
            for (const auto &pair : cluster.pendingRead) {
                target.pendingRead.emplace_back(pair.first, pair.second);
            }
            for (const auto &pair : cluster.pendingWrite) {
                target.pendingWrite.emplace_back(pair.first, pair.second);
            }
        };

        for (const auto &pair : _clusters) {
            auto normalized = normalizeColumnName(resolver, pair.first);
            if (normalized.empty()) {
                continue;
            }
            appendCluster(normalized, pair.second);
        }

        for (auto &pair : normalizedClusters) {
            pair.second.merge(READ);
            pair.second.merge(WRITE);
            pair.second.finalize();
        }

        for (const auto &keyColumn : _keyColumns) {
            if (normalizedClusters.find(keyColumn) == normalizedClusters.end()) {
                normalizedClusters.emplace(keyColumn, Cluster{});
            }
        }

        _clusters = std::move(normalizedClusters);
        _resolvedKeyColumnGroups = _keyColumnGroups;
        _resolvedGroupIsComposite = _groupIsComposite;
    }

    void StateCluster::rebuildResolvedKeyColumnGroups(const RelationshipResolver &resolver) {
        (void)resolver;
        _resolvedKeyColumnGroups = _keyColumnGroups;
        _resolvedGroupIsComposite = _groupIsComposite;
    }

    void StateCluster::invalidateTargetCache(const RelationshipResolver &resolver) {
        _targetCache.clear();
        rebuildResolvedKeyColumnGroups(resolver);

        auto rebuildTargets = [&](auto &targets) {
            for (auto &pair: targets) {
                auto &cache = pair.second;
                cache.read.clear();
                cache.write.clear();

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
                        const auto &range = writeRange.value();
                        cache.write[column] = range;

                        std::unordered_map<StateRange, TargetGidSetRef> &cacheMap = _targetCache[column];
                        TargetGidSetRef &entry = cacheMap[range];

                        const auto &cluster = _clusters.at(column);

                        if (entry.read == nullptr) {
                            auto itRead = std::find_if(cluster.read.begin(), cluster.read.end(),
                                                       [this, &range](const auto &pair) {
                                                           return pair.first == range ||
                                                                  StateRange::isIntersects(pair.first, range);
                                                       });

                            if (itRead != cluster.read.end()) {
                                entry.read = &itRead->second;
                                cache.read[column] = itRead->first;
                            }
                        }

                        if (entry.write == nullptr) {
                            auto itWrite = cluster.write.find(range);
                            if (itWrite != cluster.write.end()) {
                                entry.write = &itWrite->second;
                                cache.write[column] = itWrite->first;
                            }
                        }
                    }
                }
            }
        };

        rebuildTargets(_rollbackTargets);
        rebuildTargets(_prependTargets);
    }

    void StateCluster::refreshTargetCache(const RelationshipResolver &resolver) {
        std::unique_lock<std::shared_mutex> lock(_targetCacheLock);
        invalidateTargetCache(resolver);
    }
    
    bool StateCluster::shouldReplay(gid_t gid) {
        std::shared_lock<std::shared_mutex> lock(_targetCacheLock);
        if (_rollbackTargets.find(gid) != _rollbackTargets.end()) {
            // 롤백 타겟 자신은 재실행되어선 안된다
            return false;
        }
        size_t matched = 0;

        const auto &groups = _resolvedKeyColumnGroups.empty() ? _keyColumnGroups : _resolvedKeyColumnGroups;
        const auto &groupIsComposite = _resolvedKeyColumnGroups.empty() ? _groupIsComposite : _resolvedGroupIsComposite;
        
        for (size_t groupIndex = 0; groupIndex < groups.size(); groupIndex++) {
            const auto &group = groups[groupIndex];
            if (group.empty()) {
                continue;
            }

            size_t count = 0;

            for (const auto &keyColumn : group) {
                if (_targetCache.find(keyColumn) == _targetCache.end()) {
                    continue;
                }

                const auto &ranges = _targetCache[keyColumn];

                if (std::any_of(ranges.begin(), ranges.end(), [gid](const auto &pair) {
                    return pair.second.contains(gid);
                })) {
                    count++;
                }
            }

            if (groupIndex < groupIsComposite.size() && groupIsComposite[groupIndex]) {
                if (count == 0) {
                    continue;
                } else if (count == group.size()) {
                    matched++;
                } else {
                    return false;
                }
                continue;
            }

            if (count > 0) {
                matched++;
            }
        }
        
        return matched > 0;
    }
    
std::vector<std::string> StateCluster::generateReplaceQuery(const std::string &targetDB,
                                                            const std::string &intermediateDB,
                                                            const RelationshipResolver &resolver,
                                                            const std::vector<ForeignKey> &foreignKeys) {
        std::vector<std::string> queries;
        queries.emplace_back(fmt::format("USE {}", targetDB));
        queries.emplace_back("SET FOREIGN_KEY_CHECKS=0");

        auto tableProjections = _keyColumnGroupsByTable;

        std::unordered_map<std::string, size_t> keyColumnGroupIndex;
        keyColumnGroupIndex.reserve(_keyColumns.size());
        for (size_t groupIndex = 0; groupIndex < _keyColumnGroups.size(); groupIndex++) {
            const auto &group = _keyColumnGroups[groupIndex];
            for (const auto &column : group) {
                const auto normalized = utility::toLower(column);
                if (!normalized.empty()) {
                    keyColumnGroupIndex.emplace(normalized, groupIndex);
                }
            }
        }

        std::unordered_map<std::string, size_t> resolvedKeyColumnGroupIndex = keyColumnGroupIndex;
        for (const auto &pair : keyColumnGroupIndex) {
            auto resolved = utility::toLower(resolver.resolveChain(pair.first));
            if (!resolved.empty()) {
                resolvedKeyColumnGroupIndex.emplace(std::move(resolved), pair.second);
            }
        }

        auto addProjectionColumn = [&tableProjections](const std::string &tableName,
                                                       size_t groupIndex,
                                                       const std::string &columnName) {
            if (tableName.empty() || columnName.empty()) {
                return;
            }

            auto &projections = tableProjections[tableName];
            auto it = std::find_if(
                projections.begin(), projections.end(),
                [groupIndex](const GroupProjection &projection) {
                    return projection.groupIndex == groupIndex;
                }
            );

            if (it == projections.end()) {
                GroupProjection projection;
                projection.groupIndex = groupIndex;
                projection.columns.push_back(columnName);
                projections.push_back(std::move(projection));
                return;
            }

            if (std::find(it->columns.begin(), it->columns.end(), columnName) == it->columns.end()) {
                it->columns.push_back(columnName);
            }
        };

        if (!foreignKeys.empty()) {
            for (const auto &foreignKey : foreignKeys) {
                if (!foreignKey.fromTable || !foreignKey.toTable) {
                    continue;
                }

                const auto fromTable = utility::toLower(foreignKey.fromTable->getCurrentName());
                const auto toTable = utility::toLower(foreignKey.toTable->getCurrentName());
                const auto fromColumn = utility::toLower(foreignKey.fromColumn);
                const auto toColumn = utility::toLower(foreignKey.toColumn);

                if (fromTable.empty() || toTable.empty() || fromColumn.empty() || toColumn.empty()) {
                    continue;
                }

                const std::string fromExpr = fromTable + "." + fromColumn;
                const std::string toExpr = toTable + "." + toColumn;

                auto resolvedTo = utility::toLower(resolver.resolveChain(toExpr));
                if (resolvedTo.empty()) {
                    resolvedTo = toExpr;
                }

                auto it = resolvedKeyColumnGroupIndex.find(resolvedTo);
                if (it == resolvedKeyColumnGroupIndex.end()) {
                    continue;
                }

                addProjectionColumn(fromTable, it->second, fromExpr);
            }
        }

        for (const auto &pair : tableProjections) {
            const auto &tableName = pair.first;
            const auto &projections = pair.second;

            bool changed = false;
            bool isWildcard = false;
            std::vector<std::string> whereGroups;

            for (const auto &projection : projections) {
                if (projection.groupIndex >= _keyColumnGroups.size()) {
                    continue;
                }
                if (projection.columns.empty()) {
                    continue;
                }

                bool groupWildcard = false;
                std::vector<std::string> whereColumns;

                for (const auto &keyColumn : projection.columns) {
                    std::string resolvedColumn = resolver.resolveChain(keyColumn);
                    std::vector<std::string> conds;

                    if (resolvedColumn.empty()) {
                        resolvedColumn = keyColumn;
                    }

                    auto appendRange = [&conds, &groupWildcard, &changed, &keyColumn](const StateRange &range) {
                        if (range.wildcard()) {
                            groupWildcard = true;
                            return;
                        }
                        changed = true;
                        conds.push_back(fmt::format("({})", range.MakeWhereQuery(keyColumn)));
                    };

                    auto appendTargets = [&](const auto &targets) {
                        for (const auto &targetPair : targets) {
                            const auto &targetCache = targetPair.second;

                            auto itRead = targetCache.read.find(resolvedColumn);
                            if (itRead != targetCache.read.end()) {
                                appendRange(itRead->second);
                                if (groupWildcard) {
                                    return;
                                }
                            }

                            auto itWrite = targetCache.write.find(resolvedColumn);
                            if (itWrite != targetCache.write.end()) {
                                appendRange(itWrite->second);
                                if (groupWildcard) {
                                    return;
                                }
                            }
                        }
                    };

                    appendTargets(_rollbackTargets);
                    if (!groupWildcard) {
                        appendTargets(_prependTargets);
                    }

                    if (groupWildcard) {
                        break;
                    }

                    if (!conds.empty()) {
                        whereColumns.push_back(fmt::format("{}", fmt::join(conds, " OR ")));
                    }
                }

                if (groupWildcard) {
                    isWildcard = true;
                    break;
                }

                if (!whereColumns.empty()) {
                    const bool isComposite =
                        projection.groupIndex < _groupIsComposite.size() && _groupIsComposite[projection.groupIndex];
                    whereGroups.push_back(fmt::format("({})", fmt::join(whereColumns, isComposite ? " AND " : " OR ")));
                }
            }

            if (isWildcard) {
                queries.emplace_back(fmt::format("TRUNCATE {}", tableName));
                queries.emplace_back(fmt::format("REPLACE INTO {} SELECT * FROM {}.{}", tableName, intermediateDB, tableName));
            } else if (changed && !whereGroups.empty()) {
                std::string _where = fmt::format("{}", fmt::join(whereGroups, " OR "));

                queries.emplace_back(fmt::format("DELETE FROM {} WHERE {}", tableName, _where));
                queries.emplace_back(fmt::format("REPLACE INTO {} SELECT * FROM {}.{} WHERE {}", tableName, intermediateDB, tableName, _where));
            }
        }

        queries.emplace_back("SET FOREIGN_KEY_CHECKS=1");

        return queries;
    }
    
    bool StateCluster::shouldReplay(gid_t gid, const StateCluster::TargetTransactionCache &cache) {
        const auto &read = cache.read;
        const auto &write = cache.write;
        
        
        const std::function<bool(ClusterType, const std::pair<std::string, StateRange> &)> containsGid = [this, gid](ClusterType type, const auto &pair) {
            const auto &columnName = pair.first;
            const auto &range = pair.second;
            
            const auto &cluster = _clusters.at(columnName);
            auto it = std::find_if(cluster.read.begin(), cluster.read.end(), [this, &range](const auto &pair) {
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
            
            auto itWrite = std::find_if(cluster.write.begin(), cluster.write.end(), [this, &range](const auto &pair) {
                return pair.first == range || StateRange::isIntersects(pair.first, range);
            });
            
            if (itWrite != cluster.write.end()) {
                const auto &gids = itWrite->second;
                if (gids.find(gid) != gids.end()) {
                    // FIXME: 속도 졸라느려지므로 아래 로그 제거해야 함
                    // _logger->debug("shouldReplay({}): matched with {} ({} - WRITE)", gid, range.MakeWhereQuery(columnName), type == READ ? "READ" : "WRITE");
                    return true;
                }
            }
            
            return false;
        };
        
        return (
            // std::any_of(read.begin(), read.end(), [&containsGid](const auto &pair) { return containsGid(READ, pair); }) ||
            std::any_of(write.begin(), write.end(), [&containsGid](const auto &pair) { return containsGid(WRITE, pair); })
        );
    }

    void StateCluster::Cluster::toProtobuf(ultraverse::state::v2::proto::StateClusterCluster *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();

        for (const auto &pair : read) {
            auto *entry = out->add_read();
            pair.first.toProtobuf(entry->mutable_range());
            for (const auto gid : pair.second) {
                entry->add_gids(gid);
            }
        }

        for (const auto &pair : write) {
            auto *entry = out->add_write();
            pair.first.toProtobuf(entry->mutable_range());
            for (const auto gid : pair.second) {
                entry->add_gids(gid);
            }
        }
    }

    void StateCluster::Cluster::fromProtobuf(const ultraverse::state::v2::proto::StateClusterCluster &msg) {
        read.clear();
        write.clear();

        for (const auto &entry : msg.read()) {
            StateRange range;
            range.fromProtobuf(entry.range());
            std::unordered_set<gid_t> gids;
            gids.reserve(static_cast<size_t>(entry.gids_size()));
            for (const auto gid : entry.gids()) {
                gids.insert(gid);
            }
            read.emplace(std::move(range), std::move(gids));
        }

        for (const auto &entry : msg.write()) {
            StateRange range;
            range.fromProtobuf(entry.range());
            std::unordered_set<gid_t> gids;
            gids.reserve(static_cast<size_t>(entry.gids_size()));
            for (const auto gid : entry.gids()) {
                gids.insert(gid);
            }
            write.emplace(std::move(range), std::move(gids));
        }
    }

    void StateCluster::toProtobuf(ultraverse::state::v2::proto::StateCluster *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        auto *clusters = out->mutable_clusters();
        clusters->clear();

        for (const auto &pair : _clusters) {
            auto &clusterMsg = (*clusters)[pair.first];
            pair.second.toProtobuf(&clusterMsg);
        }
    }

    void StateCluster::fromProtobuf(const ultraverse::state::v2::proto::StateCluster &msg) {
        _clusters.clear();
        for (const auto &pair : msg.clusters()) {
            Cluster cluster;
            cluster.fromProtobuf(pair.second);
            _clusters.emplace(pair.first, std::move(cluster));
        }
    }
}
