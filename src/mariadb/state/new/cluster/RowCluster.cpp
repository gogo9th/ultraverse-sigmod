#include <algorithm>
#include <cctype>
#include <queue>

#include <boost/graph/adjacency_list.hpp>
#include <fmt/format.h>

#include "RowCluster.hpp"
#include "utils/StringUtil.hpp"

#include "base/TaskExecutor.hpp"

#include "ultraverse_state.pb.h"

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

    std::optional<StateItem> RowCluster::resolveAliasWithCoercion(const StateItem &alias, const AliasMap &aliasMap) {
        auto container = aliasMap.find(alias.name);
        if (container == aliasMap.end() || container->second.empty()) {
            return std::nullopt;
        }

        if (!alias.data_list.empty()) {
            auto real = container->second.find(alias.data_list[0]);
            if (real != container->second.end()) {
                return real->second.real;
            }
        }

        if (container->second.begin()->second.real.data_list.empty()) {
            return std::nullopt;
        }

        int64_t signedSample = 0;
        uint64_t unsignedSample = 0;
        if (!container->second.begin()->second.real.data_list[0].Get(signedSample) &&
            !container->second.begin()->second.real.data_list[0].Get(unsignedSample)) {
            return std::nullopt;
        }

        std::vector<StateData> converted;
        converted.reserve(alias.data_list.size());

        for (const auto &data : alias.data_list) {
            std::string raw;
            if (!data.Get(raw)) {
                return std::nullopt;
            }

            if (raw.empty() ||
                !std::all_of(raw.begin(), raw.end(), [](unsigned char ch) { return std::isdigit(ch) != 0; })) {
                return std::nullopt;
            }

            try {
                converted.emplace_back(StateData{ (int64_t) std::stoll(raw) });
            } catch (const std::exception &) {
                return std::nullopt;
            }
        }

        StateItem real;
        real.condition_type = alias.condition_type;
        real.function_type = alias.function_type;
        real.name = container->second.begin()->second.real.name;
        real.data_list = std::move(converted);
        real.arg_list = alias.arg_list;
        real.sub_query_list = alias.sub_query_list;
        real._isRangeCacheBuilt = false;

        return real;
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

    std::unordered_map<std::string, std::vector<std::pair<RowCluster::CompositeRange, std::vector<gid_t>>>> &RowCluster::compositeKeyMap() {
        return _compositeClusterMap;
    }

    const std::unordered_map<std::string, std::vector<std::pair<RowCluster::CompositeRange, std::vector<gid_t>>>> &RowCluster::compositeKeyMap() const {
        return _compositeClusterMap;
    }

    void RowCluster::addCompositeKey(const std::vector<std::string> &columnNames) {
        const auto keyId = normalizeCompositeKeyId(columnNames);
        if (keyId.empty()) {
            return;
        }
        if (_compositeClusterMap.find(keyId) == _compositeClusterMap.end()) {
            _compositeClusterMap.emplace(keyId, std::vector<std::pair<CompositeRange, std::vector<gid_t>>>());
        }
    }

    void RowCluster::addCompositeKeyRange(const std::vector<std::string> &columnNames, CompositeRange ranges, gid_t gid) {
        if (columnNames.size() != ranges.ranges.size() || columnNames.empty()) {
            return;
        }

        const auto normalized = normalizeCompositeInput(columnNames, ranges);
        if (normalized.first.empty()) {
            return;
        }

        auto &cluster = _compositeClusterMap[normalized.first];
        cluster.emplace_back(std::make_pair(normalized.second, std::vector<gid_t>{gid}));
    }

    void RowCluster::mergeCompositeCluster(const std::vector<std::string> &columnNames) {
        const auto keyId = normalizeCompositeKeyId(columnNames);
        if (keyId.empty()) {
            return;
        }

        auto it = _compositeClusterMap.find(keyId);
        if (it == _compositeClusterMap.end()) {
            return;
        }

        auto &cluster = it->second;
        if (cluster.size() < 2) {
            return;
        }

        bool merged = true;
        while (merged) {
            merged = false;
            for (size_t i = 0; i < cluster.size() && !merged; i++) {
                for (size_t j = i + 1; j < cluster.size(); j++) {
                    if (!compositeIntersects(cluster[i].first, cluster[j].first)) {
                        continue;
                    }

                    compositeMerge(cluster[i].first, cluster[j].first);
                    cluster[i].second.insert(cluster[i].second.end(), cluster[j].second.begin(), cluster[j].second.end());
                    cluster.erase(cluster.begin() + static_cast<long>(j));
                    merged = true;
                    break;
                }
            }
        }
    }
    
    std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>>
    RowCluster::getKeyRangeOf(Transaction &transaction, const std::string &keyColumn,
                              const std::vector<ForeignKey> &foreignKeys) {
        std::vector<std::pair<std::shared_ptr<StateRange>, std::vector<gid_t>>> keyRanges;
        
        for (auto &query: transaction.queries()) {
            for (auto &range: _clusterMap.at(keyColumn)) {
                if (isQueryRelated(keyColumn, *range.first, *query, foreignKeys, _aliases)) {
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
                                    const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases, const std::unordered_set<std::string> *implicitTables) {
        // 각 keyRange에 대해 하나만 매칭되어도 재실행 대상이 된다.
        for (auto &pair: keyRanges) {
            for (auto &keyRange: pair.second) {
                if (isQueryRelated(pair.first, *keyRange.first, query, foreignKeys, aliases, implicitTables)) {
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
    
    bool RowCluster::isQueryRelated(std::string keyColumn, const StateRange &range, Query &query, const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases, const std::unordered_set<std::string> *implicitTables) {
        for (auto &expr: query.readSet()) {
            if (isExprRelated(keyColumn, range, expr, foreignKeys, aliases, implicitTables)) {
                return true;
            }
        }
        
        for (auto &expr: query.writeSet()) {
            if (isExprRelated(keyColumn, range, expr, foreignKeys, aliases, implicitTables)) {
                return true;
            }
        }
        
        return false;
    }
    
    bool RowCluster::isExprRelated(const std::string &keyColumn, const StateRange &keyRange, StateItem &expr, const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases, const std::unordered_set<std::string> *implicitTables) {
        if (!expr.name.empty()) {
            expr.name = resolveForeignKey(expr.name, foreignKeys, implicitTables);
            auto resolved = resolveAliasWithCoercion(expr, aliases);
            if (resolved.has_value()) {
                return isExprRelated(keyColumn, keyRange, *resolved, foreignKeys, aliases, implicitTables);
            }
            
            if (keyColumn == expr.name) {
                const auto &range = expr.MakeRange2();
                if (StateRange::isIntersects(range, keyRange)) {
                    return true;
                }
            }
        }
        
        for (auto &subExpr: expr.arg_list) {
            if (isExprRelated(keyColumn, keyRange, subExpr, foreignKeys, aliases, implicitTables)) {
                return true;
            }
        }

        for (auto &subExpr: expr.sub_query_list) {
            if (isExprRelated(keyColumn, keyRange, subExpr, foreignKeys, aliases, implicitTables)) {
                return true;
            }
        }
        
        return false;
    }

    bool RowCluster::isQueryRelatedComposite(const std::vector<std::string> &keyColumns, const CompositeRange &keyRanges, Query &query, const std::vector<ForeignKey> &foreignKeys, const AliasMap &aliases, const std::unordered_set<std::string> *implicitTables) {
        if (keyColumns.size() != keyRanges.ranges.size()) {
            return false;
        }

        for (size_t i = 0; i < keyColumns.size(); i++) {
            if (!isQueryRelated(keyColumns[i], keyRanges.ranges[i], query, foreignKeys, aliases, implicitTables)) {
                return false;
            }
        }

        return true;
    }
    
    std::string RowCluster::resolveForeignKey(std::string exprName, const std::vector<ForeignKey> &foreignKeys) {
        return resolveForeignKey(std::move(exprName), foreignKeys, nullptr);
    }

    std::string RowCluster::resolveForeignKey(std::string exprName, const std::vector<ForeignKey> &foreignKeys,
                                              const std::unordered_set<std::string> *implicitTables) {
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
            if (implicitTables != nullptr && !columnName.empty()) {
                const std::string suffix = "_id";
                if (columnName.size() > suffix.size() &&
                    columnName.compare(columnName.size() - suffix.size(), suffix.size(), suffix) == 0) {
                    const std::string base = columnName.substr(0, columnName.size() - suffix.size());
                    const std::vector<std::string> candidates = {
                        base,
                        base + "s",
                        base + "es"
                    };
                    for (const auto &candidate : candidates) {
                        if (implicitTables->find(candidate) != implicitTables->end()) {
                            return candidate + ".id";
                        }
                    }
                }
            }
            return std::move(utility::toLower(exprName));
        } else {
            return resolveForeignKey(it->toTable->getCurrentName() + "." + it->toColumn, foreignKeys, implicitTables);
        }
    }

    bool RowCluster::compositeIntersects(const CompositeRange &lhs, const CompositeRange &rhs) {
        if (lhs.ranges.size() != rhs.ranges.size() || lhs.ranges.empty()) {
            return false;
        }

        for (size_t i = 0; i < lhs.ranges.size(); i++) {
            if (!StateRange::isIntersects(lhs.ranges[i], rhs.ranges[i])) {
                return false;
            }
        }

        return true;
    }

    void RowCluster::compositeMerge(CompositeRange &dst, const CompositeRange &src) {
        if (dst.ranges.size() != src.ranges.size()) {
            return;
        }

        for (size_t i = 0; i < dst.ranges.size(); i++) {
            dst.ranges[i].OR_FAST(src.ranges[i]);
            dst.ranges[i].arrangeSelf();
        }
    }

    std::pair<std::string, RowCluster::CompositeRange>
    RowCluster::normalizeCompositeInput(const std::vector<std::string> &columns, const CompositeRange &ranges) {
        if (columns.size() != ranges.ranges.size() || columns.empty()) {
            return {"", CompositeRange{}};
        }

        std::vector<std::pair<std::string, std::shared_ptr<StateRange>>> pairs;
        pairs.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); i++) {
            pairs.emplace_back(utility::toLower(columns[i]), std::make_shared<StateRange>(ranges.ranges[i]));
        }

        std::sort(pairs.begin(), pairs.end(), [](const auto &lhs, const auto &rhs) {
            return lhs.first < rhs.first;
        });

        CompositeRange normalizedRanges;
        normalizedRanges.ranges.reserve(pairs.size());

        std::string keyId;
        for (size_t i = 0; i < pairs.size(); i++) {
            if (i > 0) {
                keyId.append("|");
            }
            keyId.append(pairs[i].first);
            normalizedRanges.ranges.push_back(*pairs[i].second);
        }

        return {keyId, normalizedRanges};
    }

    std::string RowCluster::normalizeCompositeKeyId(const std::vector<std::string> &columns) {
        if (columns.empty()) {
            return std::string();
        }

        std::vector<std::string> normalized;
        normalized.reserve(columns.size());
        for (const auto &column : columns) {
            normalized.push_back(utility::toLower(column));
        }

        std::sort(normalized.begin(), normalized.end());

        std::string keyId;
        for (size_t i = 0; i < normalized.size(); i++) {
            if (i > 0) {
                keyId.append("|");
            }
            keyId.append(normalized[i]);
        }

        return keyId;
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

    void RowCluster::toProtobuf(ultraverse::state::v2::proto::RowCluster *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        auto *clusterMap = out->mutable_cluster_map();
        clusterMap->clear();

        for (const auto &pair : _clusterMap) {
            auto &rangeContainer = (*clusterMap)[pair.first];
            for (const auto &rangePair : pair.second) {
                auto *entry = rangeContainer.add_entries();
                if (rangePair.first) {
                    rangePair.first->toProtobuf(entry->mutable_range());
                }
                for (const auto gid : rangePair.second) {
                    entry->add_gids(gid);
                }
            }
        }

        out->clear_aliases();
        for (const auto &aliasPair : _aliases) {
            const auto &column = aliasPair.first;
            for (const auto &entry : aliasPair.second) {
                auto *aliasMsg = out->add_aliases();
                aliasMsg->set_column(column);
                entry.first.toProtobuf(aliasMsg->mutable_key());
                entry.second.toProtobuf(aliasMsg->mutable_alias());
            }
        }
    }

    void RowCluster::fromProtobuf(const ultraverse::state::v2::proto::RowCluster &msg) {
        _clusterMap.clear();
        _aliases.clear();

        for (const auto &pair : msg.cluster_map()) {
            auto &rangeList = _clusterMap[pair.first];
            rangeList.reserve(static_cast<size_t>(pair.second.entries_size()));
            for (const auto &entry : pair.second.entries()) {
                auto rangePtr = std::make_shared<StateRange>();
                rangePtr->fromProtobuf(entry.range());
                std::vector<gid_t> gids;
                gids.reserve(static_cast<size_t>(entry.gids_size()));
                for (const auto gid : entry.gids()) {
                    gids.push_back(gid);
                }
                rangeList.emplace_back(std::move(rangePtr), std::move(gids));
            }
        }

        for (const auto &aliasMsg : msg.aliases()) {
            StateData key;
            key.fromProtobuf(aliasMsg.key());
            RowAlias alias;
            alias.fromProtobuf(aliasMsg.alias());
            _aliases[aliasMsg.column()].emplace(std::move(key), std::move(alias));
        }
    }
}
