//
// Created by cheesekun on 6/20/23.
//

#include "StateCluster.hpp"

#include <utility>

namespace ultraverse::state::v2 {
    
    std::optional<StateRange> StateCluster::Cluster::match(StateCluster::ClusterType type,
                                                           const std::string &columnName,
                                                           const ClusterMap &cluster,
                                                           CombinedIterator<StateItem> begin,
                                                           CombinedIterator<StateItem> end,
                                                           const RelationshipResolver &resolver) {
        
        auto it = std::find_if(cluster.begin(), cluster.end(), [type, &resolver, &columnName, &begin, &end](const auto &pair) {
            const StateRange &range = pair.first;
            return std::find_if(begin, end, [type, &resolver, &columnName, &range](const StateItem &item) {
                // FIXME: 이거 개느릴거같은데;;
                
                // Q: 이거 std::move 해야 하지 않나?
                // A: 아니야. 그냥 const & 로 해야해. 그래야 더 빠르거든.
                // Q: 왜?
                // A: std::move 는 rvalue 로 바꿔주는거야. 그래서 이동 생성자를 호출하거든.
                
                // WRITE가 FK에 대해 직접 write하는 경우는 없으므로 chain resolve는 READ에 대해서만 수행한다.
                const auto &realColumn = (type == READ) ?
                    resolver.resolveChain(item.name) :
                    resolver.resolveColumnAlias(item.name);
                const auto &real = (type == READ) ?
                    resolver.resolveRowChain(item) :
                    resolver.resolveRowAlias(item);
                
                if (real != nullptr) {
                    return real->name == columnName && StateRange::isIntersects(real->MakeRange2(), range);
                } else if (!realColumn.empty()) {
                    return realColumn == columnName && StateRange::isIntersects(item.MakeRange2(), range);
                } else {
                    return item.name == columnName && StateRange::isIntersects(item.MakeRange2(), range);
                }
            }) != end;
        });
        
        if (it == cluster.end()) {
            return std::nullopt;
        }
        
        return it->first;
    }
    
    
    StateCluster::StateCluster(const std::set<std::string> &keyColumns):
        _logger(createLogger("StateCluster")),
        _keyColumns(keyColumns)
    {
    
    }
    
    const std::set<std::string> &StateCluster::keyColumns() const {
        return _keyColumns;
    }
    
    const std::map<std::string, StateCluster::Cluster> &StateCluster::clusters() const {
        return _clusters;
    }
    
    bool StateCluster::isKeyColumnItem(const RelationshipResolver &resolver, const StateItem &item) const {
        return std::find_if(
            _keyColumns.begin(), _keyColumns.end(),
            [&resolver, &item](const auto &keyColumn) {
                // 과연 이게 맞는가? alias + foreign key를 고려해야 하지 않을까?
                auto realColumn = resolver.resolveChain(item.name);
                
                return (item.name == keyColumn) ||
                       (!realColumn.empty() && realColumn == keyColumn);
            }
        ) != _keyColumns.end();
    }
    
    void StateCluster::insert(StateCluster::ClusterType type, const std::string &columnName, const StateRange &range, gid_t gid) {
        std::scoped_lock lock(_clusterInsertionLock);
        
        auto &cluster = type == READ ?
            _clusters[columnName].read :
            _clusters[columnName].write;
        
        auto it = std::find_if(cluster.begin(), cluster.end(), [&range](const auto &pair) {
            return pair.first == range || StateRange::isIntersects(pair.first, range);
        });
        
        if (it != cluster.end()) {
            auto dstRange = it->first;
            dstRange.OR_FAST(range);
            
            if (dstRange == range || it->first == dstRange) {
                it->second.emplace(gid);
            } else {
                // replace key using std::extract (see https://en.cppreference.com/w/cpp/container/map/extract)
                auto node = cluster.extract(it->first);
                
                _logger->trace("merging range: {} and {}", node.key().MakeWhereQuery(columnName),
                               range.MakeWhereQuery(columnName));
                
                node.key() = dstRange;
                node.mapped().emplace(gid);
                
                cluster.insert(std::move(node));
            }
        } else {
            cluster[range].emplace(gid);
        }
    }
    
    void StateCluster::insert(StateCluster::ClusterType type, CombinedIterator<StateItem> begin, CombinedIterator<StateItem> end, gid_t gid, const RelationshipResolver &resolver) {
        const auto isKeyColumnItem = [&resolver, this](const StateItem &item) {
            return this->isKeyColumnItem(resolver, item);
        };
        
        auto it = std::move(begin);
        
        while (true) {
            it = std::find_if(it, end, isKeyColumnItem);
            
            if (it == end) {
                break;
            }
            
            auto &item = *it;
            auto &columnName = item.name;
            
            const auto &realColumn = resolver.resolveChain(item.name);
            const auto &real = resolver.resolveRowChain(item);
            
            if (real != nullptr) {
                // fk / alias를 해결한 경우에는 type 상관없이 강제로 READ 관계로 넣는다
                insert(READ, real->name, real->MakeRange2(), gid);
            } else if (!realColumn.empty()) {
                // real row를 해결하지 못했지만, realColumn은 해결한 경우 => 즉, foreignKey인 경우
                // 이 경우에도 FK를 해결한 경우이므로 type 상꽌없이 강제로 READ 관계로 넣는다
                insert(READ, realColumn, item.MakeRange2(), gid);
            } else {
                // real row도 해결하지 못하고, realColumn도 해결하지 못한 경우 => keyColumn인 경우
                insert(type, columnName, item.MakeRange2(), gid);
            }
           
            ++it;
        }
    }
    
    void StateCluster::insert(const std::shared_ptr<Transaction>& transaction, const RelationshipResolver &resolver) {
        insert(READ, transaction->whereSet_begin(), transaction->whereSet_end(), transaction->gid(), resolver);
        insert(WRITE, transaction->itemSet_begin(), transaction->itemSet_end(), transaction->gid(), resolver);
    }
    
    std::optional<StateRange> StateCluster::match(StateCluster::ClusterType type, const std::string &columnName,
                                                  const std::shared_ptr<Transaction> &transaction,
                                                  const RelationshipResolver &resolver) const {
        
        if (_clusters.find(columnName) == _clusters.end()) {
            return std::nullopt;
        }
        
        const auto &cluster = _clusters.at(columnName);
        
        if (type == READ) {
            return StateCluster::Cluster::match(READ, columnName, cluster.read, transaction->whereSet_begin(), transaction->whereSet_end(), resolver);
        }
        
        if (type == WRITE) {
            return StateCluster::Cluster::match(WRITE, columnName, cluster.write, transaction->itemSet_begin(), transaction->itemSet_end(), resolver);
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
                                         const RelationshipResolver &resolver) {
        std::scoped_lock lock(_targetCacheLock);
        
        _rollbackTargets.insert(std::make_pair(transaction->gid(), TargetTransactionCache {
            transaction,
            {}, {}
        }));
        
        invalidateTargetCache(_rollbackTargets, resolver);
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
        for (auto &pair: targets) {
            auto &cache = pair.second;
            
            for (const auto &keyColumn: _keyColumns) {
                auto readRange = match(READ, keyColumn, cache.transaction, resolver);
                auto writeRange = match(READ, keyColumn, cache.transaction, resolver);
                
                if (readRange.has_value()) {
                    cache.read[keyColumn] = readRange.value();
                }
                if (writeRange.has_value()) {
                    cache.write[keyColumn] = writeRange.value();
                }
            }
        }
    }
    
    bool StateCluster::shouldReplay(gid_t gid) {
        if (_rollbackTargets.find(gid) != _rollbackTargets.end()) {
            // 롤백 타겟 자신은 재실행되어선 안된다
            return false;
        }
        
        return std::any_of(
            _rollbackTargets.begin(), _rollbackTargets.end(),
            [this, gid](const auto &pair) {
                if (shouldReplay(gid, pair.second)) {
                    _logger->debug("shouldReplay({}): related with rollback target {}", gid, pair.first);
                    return true;
                }
                
                return false;
            }
        );
    }
    
    bool StateCluster::shouldReplay(gid_t gid, const StateCluster::TargetTransactionCache &cache) {
        const auto &read = cache.read;
        const auto &write = cache.write;
        
        const std::function<bool(ClusterType, const std::pair<std::string, StateRange> &)> containsGid = [this, gid](ClusterType type, const auto &pair) {
            std::scoped_lock _lock(_clusterInsertionLock);
            
            const auto &columnName = pair.first;
            const auto &range = pair.second;
            
            const auto &cluster = _clusters.at(columnName);
            
            if (cluster.read.find(range) != cluster.read.end()){
                const auto &gids = _clusters.at(columnName).read.at(range);
                if (gids.find(gid) != gids.end()) {
                    // FIXME: 속도 졸라느려지므로 아래 로그 제거해야 함
                    _logger->debug("shouldReplay({}): matched with {} ({} - READ)", gid, range.MakeWhereQuery(columnName), type == READ ? "READ" : "WRITE");
                    return true;
                }
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