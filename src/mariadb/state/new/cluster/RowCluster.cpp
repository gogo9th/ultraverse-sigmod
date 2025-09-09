#include <queue>

#include <boost/graph/adjacency_list.hpp>
#include <fmt/format.h>

#include "RowCluster.hpp"
#include "utils/StringUtil.hpp"

#include "base/TaskExecutor.hpp"

namespace ultraverse::state::v2 {
    RowCluster::RowCluster():
        _logger(createLogger("RowCluster"))
    {
    
    }
    
    bool RowCluster::hasKey(const std::string &columnName) const {
        return _clusterMap.find(columnName) != _clusterMap.end();
    }
    
    void RowCluster::addKey(const std::string &columnName) {
        if (hasKey(columnName)) {
            return;
        }
        
        _clusterMap.insert({ columnName, std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>() });
    }
    
    void RowCluster::addKeyRange(const std::string &columnName, std::shared_ptr<StateRange> range, gid_t gid) {
        auto &cluster = _clusterMap[columnName];
        auto &graph = _clusterGraph[columnName];
        
        cluster.emplace_back(std::make_pair(range, std::vector<gid_t> { gid }));
        auto size = cluster.size();
        auto nodeIdx = add_vertex({ size - 1, false }, graph);
    
        /*
    
        boost::graph_traits<ClusterGraph>::vertex_iterator vi, viEnd, next;
        boost::tie(vi, viEnd) = vertices(graph);
        
        for (next = vi; vi != viEnd; vi = next) {
            ++next;
            
            const auto &pair = graph[*vi];
            int index = pair.first;
            if (StateRange::AND_FAST(*range, *(cluster[index].first))) {
                add_edge(*vi, nodeIdx, graph);
                break;
            }
        }
         */
    }
    
    void RowCluster::setWildcard(const std::string &columnName, bool wildcard) {
        _wildcardMap[columnName] = wildcard;
    }
    
    void RowCluster::addAlias(const std::string &elementName, const StateItem &alias, const StateItem &real) {
        _aliases[alias.name].insert(std::make_pair(alias.data_list[0], RowAlias { alias, real }));
        
    }
    
    const StateItem &RowCluster::resolveAlias(const StateItem &alias, const AliasMap &aliasMap) {
        auto container = aliasMap.find(alias.name);
        if (container == aliasMap.end()) {
            return alias;
        }
        
        auto real = container->second.find(alias.data_list[0]);
        if (real == container->second.end()) {
            return alias;
        }
       
        return real->second.real;
    }
    
    std::vector<std::unique_ptr<std::pair<std::string, std::shared_ptr<StateRange>>>>
    RowCluster::resolveInvertedAliasRange(const std::vector<RowAlias> &aliases, std::string alias, std::shared_ptr<StateRange> range) {
        std::vector<std::unique_ptr<std::pair<std::string, std::shared_ptr<StateRange>>>> ranges;
        auto it = std::find_if(aliases.begin(), aliases.end(), [&alias, &range](auto item) {
            auto range2 = item.alias.MakeRange();
            auto range3 = StateRange::AND(
                *range, *range2
            );
            
            return item.alias.name == alias && range3->GetRange()->empty();
        });
        
        while (it != aliases.end()) {
            auto item = it->alias;
            ranges.emplace_back(std::make_unique<std::pair<std::string, std::shared_ptr<StateRange>>>(
                it->alias.name, item.MakeRange()
            ));
            
            it++;
        }
        
        return std::move(ranges);
    }
    
    
    std::string RowCluster::resolveAliasName(const AliasMap &aliases, std::string alias) {
        if (aliases.find(alias) == aliases.end() || aliases.at(alias).empty()) {
            return alias;
        }
        
        return aliases.at(alias).begin()->second.real.name;
    }
    
    const RowCluster::AliasMap &RowCluster::aliasMap() const {
        return _aliases;
    }
    
    void RowCluster::mergeCluster(const std::string &columnName) {
        if (_wildcardMap.find(columnName) != _wildcardMap.end()) {
            mergeClusterAll(columnName);
        } else {
            mergeClusterUsingGraph(columnName);
        }
    }
    
    void RowCluster::mergeClusterUsingGraph(const std::string &columnName) {
        using VertexIterator = boost::graph_traits<ClusterGraph>::vertex_descriptor;
        auto &cluster = _clusterMap[columnName];
        std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>> newCluster;
        
        std::function<void (VertexIterator, std::shared_ptr<StateRange> &, std::vector<gid_t> &)> visitNode = [this, &columnName, &visitNode](VertexIterator vi, std::shared_ptr<StateRange> &range, std::vector<gid_t> &gidList) {
            auto &pair1 = _clusterGraph[columnName][vi];
            
            if (pair1.second) {
                return;
            }
            _logger->trace("visiting node {}", pair1.first);
            
            pair1.second = true;
            
            boost::graph_traits<ClusterGraph>::adjacency_iterator ai, aiEnd, aiNext;
            boost::tie(ai, aiEnd) = boost::adjacent_vertices(vi, _clusterGraph[columnName]);
            
            for (aiNext = ai; ai != aiEnd; ai = aiNext) {
                aiNext++;
                
                if (*ai == vi) {
                    continue;
                }
                
                auto &pair2 = _clusterGraph[columnName][*ai];
                
                if (pair2.second) {
                    continue;
                }
                
                auto &pair = _clusterMap[columnName][pair2.first];
                
                range->OR_FAST(*pair.first);
                gidList.insert(
                    gidList.end(),
                    pair.second.begin(), pair.second.end()
                );
                
                visitNode(*ai, range, gidList);
            }
        };
    
        
        boost::graph_traits<ClusterGraph>::vertex_iterator vi, viEnd, viNext;
        boost::tie(vi, viEnd) = boost::vertices(_clusterGraph[columnName]);
        
        for (viNext = vi; vi != viEnd; vi = viNext) {
            viNext++;
            
            auto &pair = _clusterGraph[columnName][*vi];
            if (pair.second) {
                continue;
            }
            
            std::shared_ptr<StateRange> range = std::make_shared<StateRange>();
            std::vector<gid_t> gidList;
            
            range->OR_FAST(*_clusterMap[columnName][pair.first].first);
            gidList.insert(
                gidList.end(),
                _clusterMap[columnName][pair.first].second.begin(),
                _clusterMap[columnName][pair.first].second.end()
            );
            
            visitNode(*vi, range, gidList);
            newCluster.emplace_back(range, std::move(gidList));
        }

        for (int i = 0; i < newCluster.size(); i++) {
            _logger->trace("performing OR_ARRANGE.. {} / {}", i, newCluster.size());
            newCluster[i].first->arrangeSelf();
        }
        
        cluster = newCluster;
        _clusterGraph[columnName].clear();
    
        bool rerun = false;
    
        {
            auto &graph = _clusterGraph[columnName];
            std::mutex mutex;
            TaskExecutor taskExecutor(8);
            std::queue<std::shared_ptr<std::promise<int>>> taskQueue;
        
            for (int i = 0; i < cluster.size(); i++) {
                add_vertex({i, false}, graph);
            }
    
            for (int i = 0; i < cluster.size(); i++) {
                auto task = taskExecutor.post<int>([this, &mutex, &graph, &cluster, &rerun, i]() {
                    _logger->trace("reconstructing graph.. {} / {}", i, cluster.size());
                
                    boost::graph_traits<ClusterGraph>::vertex_iterator vi, viEnd, next;
                    boost::tie(vi, viEnd) = vertices(graph);
                
                    for (next = vi; vi != viEnd; vi = next) {
                        ++next;
                    
                    
                        const auto &pair = graph[*vi];
                        int index = pair.first;
                    
                        if (i == index) {
                            continue;
                        }
                    
                        if (StateRange::isIntersects(*cluster[i].first, *(cluster[index].first))) {
                            std::scoped_lock<std::mutex> lock(mutex);
                            rerun = true;
                            add_edge(*vi, i, graph);
                            break;
                        }
                    }
                    
                    return 0;
                });
                
                taskQueue.emplace(std::move(task));
            }
            
            while (!taskQueue.empty()) {
                auto task = std::move(taskQueue.front());
                auto future = task->get_future();
                future.wait();
                taskQueue.pop();
            }
        }
        
        if (rerun) {
            mergeClusterUsingGraph(columnName);
        }
        
    }
    
    void RowCluster::mergeClusterAll(const std::string &columnName) {
        auto &cluster = _clusterMap[columnName];
        if (cluster.size() < 2) {
            return;
        }
        
        auto it = cluster.begin();
        auto first = *it++;
        
        while (it != cluster.end()) {
            first.first->OR_FAST(*(*it++).first);
        }

        first.first->arrangeSelf();

        cluster.clear();
        cluster.push_back(first);
        
        _clusterGraph[columnName].clear();
    }
   
    std::unordered_map<std::string, std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>> &RowCluster::keyMap() {
        return _clusterMap;
    }
    
    std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>
    RowCluster::getKeyRangeOf(Transaction &transaction, const std::string &keyColumn,
                              const std::vector<ForeignKey> &foreignKeys) {
        std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>> keyRanges;
        
        for (auto &query: transaction.queries()) {
            for (auto &range: _clusterMap.at(keyColumn)) {
                if (isQueryRelated(keyColumn, range.first, *query, foreignKeys, _aliases)) {
                    keyRanges.push_back(range);
                }
            }
        }
        
        return keyRanges;
    }
    
    std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>> RowCluster::getKeyRangeOf2(Transaction &transaction, const std::string &keyColumn, const std::vector<ForeignKey> &foreignKeys) {
        std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>> keyRanges;

        if (_clusterMap.find(keyColumn) != _clusterMap.end()) {
            for (auto &range: _clusterMap.at(keyColumn)) {
                if (isTransactionRelated(transaction.gid(), range.second)) {
                    keyRanges.push_back(range);
                }
            }
        }
        
        return std::move(keyRanges);
    }
    
    bool RowCluster::isQueryRelated(std::map<std::string, std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>> &keyRanges, Query &query,
                                    const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases) {
        // 각 keyRange에 대해 하나만 매칭되어도 재실행 대상이 된다.
        for (auto &pair: keyRanges) {
            for (auto &keyRange: pair.second) {
                if (isQueryRelated(pair.first, keyRange.first, query, foreignKeys, aliases)) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    bool RowCluster::isTransactionRelated(Transaction &transaction, const std::map<std::string, std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>> &keyRanges) {
         // 각 keyRange에 대해 하나만 매칭되어도 재실행 대상이 된다.
         
         for (auto &pair: keyRanges) {
             for (auto &keyRange: pair.second) {
                 if (isTransactionRelated(transaction.gid(), keyRange.second)) {
                     return true;
                 }
             }
         }
        
         return false;
    }
    
    bool RowCluster::isTransactionRelated(gid_t gid, const std::vector<gid_t> &gidList) {
        return std::find(gidList.begin(), gidList.end(), gid) != gidList.end();
    }
    
    bool RowCluster::isQueryRelated(std::string keyColumn, std::shared_ptr<StateRange> range, Query &query, const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases) {
        for (auto &expr: query.readSet()) {
            if (isExprRelated(keyColumn, *range, expr, foreignKeys, aliases)) {
                return true;
            }
        }
        
        for (auto &expr: query.writeSet()) {
            if (isExprRelated(keyColumn, *range, expr, foreignKeys, aliases)) {
                return true;
            }
        }
        
        return false;
    }
    
    bool RowCluster::isExprRelated(std::string keyColumn, StateRange &keyRange, StateItem &expr, const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases) {
        if (!expr.name.empty()) {
            expr.name = resolveForeignKey(expr.name, foreignKeys);
            auto alias = resolveAlias(expr, aliases);
            if (alias.name != expr.name) {
                return isExprRelated(keyColumn, keyRange, alias, foreignKeys, aliases);
            }
            
            if (keyColumn == expr.name) {
                auto range = StateItem::MakeRange(expr);
                if (StateRange::isIntersects(*range, keyRange)) {
                    return true;
                }
            }
        }
        
        for (auto &subExpr: expr.arg_list) {
            if (isExprRelated(keyColumn, keyRange, subExpr, foreignKeys, aliases)) {
                return true;
            }
        }
        
        return false;
    }
    
    std::string RowCluster::resolveForeignKey(std::string exprName, const std::vector<ForeignKey> &foreignKeys) {
        auto vec = utility::splitTableName(exprName);
        auto tableName  = std::move(utility::toLower(vec.first));
        auto columnName = std::move(utility::toLower(vec.second));
    
        auto it = std::find_if(foreignKeys.cbegin(), foreignKeys.cend(), [&tableName, &columnName](auto &foreignKey) {
            if (foreignKey.fromTable->getCurrentName() == tableName && columnName == foreignKey.fromColumn) {
                return true;
            }
            return false;
        });
        
        if (it == foreignKeys.end()) {
            return std::move(utility::toLower(exprName));
        } else {
            return resolveForeignKey(it->toTable->getCurrentName() + "." + it->toColumn, foreignKeys);
        }
    }
    
    RowCluster RowCluster::operator&(const RowCluster &other) const {
        RowCluster dst = *this;
        
        std::unordered_set<std::string> keys;
        for (auto &it: this->_clusterMap) {
            keys.insert(it.first);
        }
        
        for (auto &it: other._clusterMap) {
            keys.insert(it.first);
        }
        
        for (auto &key: keys) {
            if (!other.hasKey(key) || !this->hasKey(key)) {
                continue;
            }
            // dst._clusterMap[key] = StateRange::AND(this->_clusterMap.at(key), other._clusterMap.at(key));
        }
        
        return std::move(dst);
    }
    
    RowCluster RowCluster::operator|(const RowCluster &other) const {
        RowCluster dst = *this;
        
        for (auto &it: this->_clusterMap) {
            if (!other.hasKey(it.first)) {
                dst._clusterMap[it.first] = it.second;
            } else {
                // dst._clusterMap[it.first] = StateRange::OR(this->_clusterMap.at(it.first), other._clusterMap.at(it.first));
            }
        }
        
        for (auto &it: other._clusterMap) {
            if (dst.hasKey(it.first)) {
                continue;
            } else if (!this->hasKey(it.first)) {
                dst._clusterMap[it.first] = it.second;
            }
        }
        
        return std::move(dst);
    }
}