//
// Created by cheesekun on 7/10/23.
//

#include <algorithm>
#include <cmath>
#include <execution>
#include <fstream>
#include <optional>
#include <unordered_set>

#include <fmt/format.h>


#include "utils/StringUtil.hpp"

#include "RowGraph.hpp"

#include "../cluster/StateRelationshipResolver.hpp"

namespace ultraverse::state::v2 {
    namespace {
        std::set<std::string> normalizeKeyColumns(const std::set<std::string> &keyColumns) {
            std::set<std::string> normalized;

            for (const auto &keyColumn : keyColumns) {
                normalized.insert(utility::toLower(keyColumn));
            }

            return normalized;
        }

        std::vector<std::vector<std::string>>
        normalizeKeyColumnGroups(const std::set<std::string> &keyColumns,
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

        std::unordered_map<std::string, std::vector<size_t>>
        buildCompositeGroupsByTable(const std::vector<std::vector<std::string>> &groups,
                                    const std::vector<bool> &groupIsComposite) {
            std::unordered_map<std::string, std::vector<size_t>> mapping;

            for (size_t index = 0; index < groups.size(); index++) {
                if (index >= groupIsComposite.size() || !groupIsComposite[index]) {
                    continue;
                }
                const auto &group = groups[index];
                if (group.empty()) {
                    continue;
                }

                const auto pair = utility::splitTableName(group.front());
                if (pair.first.empty()) {
                    continue;
                }

                mapping[pair.first].push_back(index);
            }

            return mapping;
        }

        std::unordered_map<std::string, std::vector<std::string>>
        buildKeyColumnsByTable(const std::set<std::string> &keyColumns) {
            std::unordered_map<std::string, std::vector<std::string>> mapping;

            for (const auto &column : keyColumns) {
                const auto pair = utility::splitTableName(column);
                if (pair.first.empty()) {
                    continue;
                }
                mapping[pair.first].push_back(column);
            }

            return mapping;
        }

    }

    RowGraph::RowGraph(const std::set<std::string> &keyColumns,
                       const RelationshipResolver &resolver,
                       const std::vector<std::vector<std::string>> &keyColumnGroups):
        _logger(createLogger("RowGraph")),
        _resolver(resolver),
        _keyColumns(normalizeKeyColumns(keyColumns)),
        _rangeComparisonMethod(RangeComparisonMethod::EQ_ONLY)
    {
        _keyColumnGroups = normalizeKeyColumnGroups(keyColumns, keyColumnGroups);
        _keyColumns.clear();
        for (const auto &group : _keyColumnGroups) {
            for (const auto &column : group) {
                _keyColumns.insert(column);
            }
        }

        _groupIsComposite = buildGroupCompositeFlags(_keyColumnGroups);
        _keyColumnsByTable = buildKeyColumnsByTable(_keyColumns);
        _compositeGroupsByTable = buildCompositeGroupsByTable(_keyColumnGroups, _groupIsComposite);
        _compositeWorkers.resize(_keyColumnGroups.size());

        for (size_t index = 0; index < _keyColumnGroups.size(); index++) {
            const auto &group = _keyColumnGroups[index];
            if (group.empty()) {
                continue;
            }

            const bool isCompositeGroup = index < _groupIsComposite.size() && _groupIsComposite[index];

            if (isCompositeGroup) {
                for (const auto &column : group) {
                    _groupIndexByColumn[column] = index;
                }

                _compositeColumns.insert(group.begin(), group.end());
                auto worker = std::make_unique<CompositeWorker>();
                worker->columns = group;
                worker->worker = std::thread(&RowGraph::compositeWorkerLoop, this, std::ref(*worker));
                _compositeWorkers[index] = std::move(worker);
                continue;
            }

            for (const auto &column : group) {
                if (_columnWorkers.find(column) != _columnWorkers.end()) {
                    continue;
                }
                auto worker = std::make_unique<ColumnWorker>();
                worker->column = column;
                worker->worker = std::thread(&RowGraph::columnWorkerLoop, this, std::ref(*worker));
                _columnWorkers.emplace(column, std::move(worker));
            }
        }

        _workerCount = static_cast<uint32_t>(_columnWorkers.size());
        for (const auto &workerPtr : _compositeWorkers) {
            if (workerPtr) {
                ++_workerCount;
            }
        }
    }
    
    RowGraph::~RowGraph() {
        {
            std::lock_guard<std::mutex> lock(_gcMutex);
            _gcPause.store(false, std::memory_order_release);
        }
        _gcCv.notify_all();

        for (auto &pair : _columnWorkers) {
            auto &worker = pair.second;
            {
                std::lock_guard<std::mutex> lock(worker->queueMutex);
                worker->running = false;
            }
            worker->queueCv.notify_all();
        }

        for (auto &workerPtr : _compositeWorkers) {
            if (!workerPtr) {
                continue;
            }
            auto &worker = *workerPtr;
            {
                std::lock_guard<std::mutex> lock(worker.queueMutex);
                worker.running = false;
            }
            worker.queueCv.notify_all();
        }

        for (auto &pair : _columnWorkers) {
            auto &worker = pair.second;
            if (worker->worker.joinable()) {
                worker->worker.join();
            }
        }

        for (auto &workerPtr : _compositeWorkers) {
            if (!workerPtr) {
                continue;
            }
            auto &worker = *workerPtr;
            if (worker.worker.joinable()) {
                worker.worker.join();
            }
        }
    }

    bool RowGraph::CompositeRange::isGlobalWildcard() const {
        if (ranges.empty()) {
            return false;
        }
        return std::all_of(ranges.begin(), ranges.end(), [](const StateRange &range) {
            return range.wildcard();
        });
    }

    std::size_t RowGraph::CompositeRangeHash::operator()(const CompositeRange &range) const {
        return range.hash;
    }

    bool RowGraph::CompositeRangeEq::operator()(const CompositeRange &lhs, const CompositeRange &rhs) const {
        if (lhs.ranges.size() != rhs.ranges.size()) {
            return false;
        }
        for (size_t index = 0; index < lhs.ranges.size(); index++) {
            if (!(lhs.ranges[index] == rhs.ranges[index])) {
                return false;
            }
        }
        return true;
    }
    
    RowGraphId RowGraph::addNode(std::shared_ptr<Transaction> transaction, bool hold) {
        auto node = std::make_shared<RowGraphNode>();
        std::atomic_store(&node->transaction, std::move(transaction));
        node->hold = hold;
        
        RowGraphId id = nullptr;
        {
            WriteLock _lock(_graphMutex);
            id = boost::add_vertex(node, _graph);
        }
        
        std::unordered_map<std::string, ColumnTask> tasksByColumn;
        tasksByColumn.reserve(_keyColumns.size());

        std::unordered_map<std::string, StateRange> compositeReadRanges;
        std::unordered_map<std::string, StateRange> compositeWriteRanges;

        std::set<std::string> tablesTouchedRead;
        std::set<std::string> tablesTouchedWrite;
        std::unordered_set<std::string> wildcardReadColumns;
        std::unordered_set<std::string> wildcardWriteColumns;
        bool globalReadWildcard = false;
        bool globalWriteWildcard = false;
        const auto transactionPtr = std::atomic_load(&node->transaction);
        if (!transactionPtr) {
            node->ready = true;
            return id;
        }

        auto mergeRange = [](StateRange &dst, const StateRange &src) {
            if (dst.wildcard()) {
                return;
            }
            if (src.wildcard()) {
                dst = src;
                return;
            }
            dst.OR_FAST(src);
        };

        auto addResolvedItem = [&](StateItem resolved, bool isWrite) {
            resolved.name = utility::toLower(resolved.name);
            const auto columnName = resolved.name;

            if (_compositeColumns.find(columnName) != _compositeColumns.end()) {
                auto &targetMap = isWrite ? compositeWriteRanges : compositeReadRanges;
                StateRange range = resolved.MakeRange2();
                auto it = targetMap.find(columnName);
                if (it == targetMap.end()) {
                    targetMap.emplace(columnName, std::move(range));
                } else {
                    mergeRange(it->second, range);
                }
                return;
            }

            auto &task = tasksByColumn[columnName];
            task.nodeId = id;
            if (isWrite) {
                task.writeItems.push_back(std::move(resolved));
            } else {
                task.readItems.push_back(std::move(resolved));
            }
        };

        auto markTableTouched = [&](const std::string &columnExpr, bool isWrite) {
            if (columnExpr.empty()) {
                if (isWrite) {
                    globalWriteWildcard = true;
                } else {
                    globalReadWildcard = true;
                }
                return;
            }

            const auto normalized = utility::toLower(columnExpr);
            const auto tablePair = utility::splitTableName(normalized);

            if (tablePair.first.empty()) {
                if (isWrite) {
                    globalWriteWildcard = true;
                } else {
                    globalReadWildcard = true;
                }
                return;
            }

            if (_keyColumnsByTable.find(tablePair.first) == _keyColumnsByTable.end()) {
                if (isWrite) {
                    globalWriteWildcard = true;
                } else {
                    globalReadWildcard = true;
                }
                return;
            }

            if (isWrite) {
                tablesTouchedWrite.insert(tablePair.first);
            } else {
                tablesTouchedRead.insert(tablePair.first);
            }
        };

        auto resolveKeyItem = [&](const StateItem &item) -> std::optional<StateItem> {
            if (item.name.empty()) {
                return std::nullopt;
            }

            auto resolvedRow = _resolver.resolveRowChain(item);
            if (resolvedRow != nullptr) {
                StateItem resolved = *resolvedRow;
                resolved.name = utility::toLower(resolved.name);

                if (_keyColumns.find(resolved.name) != _keyColumns.end()) {
                    return resolved;
                }

                const auto chained = utility::toLower(_resolver.resolveChain(resolved.name));
                if (!chained.empty() && _keyColumns.find(chained) != _keyColumns.end()) {
                    resolved.name = chained;
                    return resolved;
                }
            }

            const auto chained = utility::toLower(_resolver.resolveChain(item.name));
            if (!chained.empty() && _keyColumns.find(chained) != _keyColumns.end()) {
                StateItem resolved = item;
                resolved.name = chained;
                return resolved;
            }

            const auto itemName = utility::toLower(item.name);
            if (_keyColumns.find(itemName) != _keyColumns.end()) {
                StateItem resolved = item;
                resolved.name = itemName;
                return resolved;
            }

            return std::nullopt;
        };

        auto addWildcardForColumn = [&](const std::string &columnName, bool isWrite) {
            const auto normalized = utility::toLower(columnName);
            if (_compositeColumns.find(normalized) != _compositeColumns.end()) {
                return;
            }
            if (_keyColumns.find(normalized) == _keyColumns.end()) {
                return;
            }

            auto &targetSet = isWrite ? wildcardWriteColumns : wildcardReadColumns;
            if (!targetSet.insert(normalized).second) {
                return;
            }

            auto &task = tasksByColumn[normalized];
            task.nodeId = id;
            if (isWrite) {
                task.writeItems.push_back(StateItem::Wildcard(normalized));
            } else {
                task.readItems.push_back(StateItem::Wildcard(normalized));
            }
        };

        for (auto it = transactionPtr->readSet_begin(); it != transactionPtr->readSet_end(); ++it) {
            const auto &item = *it;
            auto resolved = resolveKeyItem(item);
            if (resolved.has_value()) {
                addResolvedItem(std::move(*resolved), false);
            } else {
                markTableTouched(item.name, false);
            }
        }

        for (auto it = transactionPtr->writeSet_begin(); it != transactionPtr->writeSet_end(); ++it) {
            const auto &item = *it;
            auto resolved = resolveKeyItem(item);
            if (resolved.has_value()) {
                addResolvedItem(std::move(*resolved), true);
            } else {
                markTableTouched(item.name, true);
            }
        }

        std::vector<bool> groupReadTouched(_keyColumnGroups.size(), globalReadWildcard);
        std::vector<bool> groupWriteTouched(_keyColumnGroups.size(), globalWriteWildcard);

        if (!globalReadWildcard) {
            for (const auto &tableName : tablesTouchedRead) {
                auto it = _compositeGroupsByTable.find(tableName);
                if (it == _compositeGroupsByTable.end()) {
                    continue;
                }
                for (auto index : it->second) {
                    groupReadTouched[index] = true;
                }
            }
        }

        if (!globalWriteWildcard) {
            for (const auto &tableName : tablesTouchedWrite) {
                auto it = _compositeGroupsByTable.find(tableName);
                if (it == _compositeGroupsByTable.end()) {
                    continue;
                }
                for (auto index : it->second) {
                    groupWriteTouched[index] = true;
                }
            }
        }

        auto hasSingleReadItems = [&tasksByColumn](const std::string &columnName) {
            auto it = tasksByColumn.find(columnName);
            return it != tasksByColumn.end() && !it->second.readItems.empty();
        };

        auto hasSingleWriteItems = [&tasksByColumn](const std::string &columnName) {
            auto it = tasksByColumn.find(columnName);
            return it != tasksByColumn.end() && !it->second.writeItems.empty();
        };

        auto addWildcardsForTable = [&](const std::string &tableName, bool isWrite) {
            auto it = _keyColumnsByTable.find(tableName);
            if (it == _keyColumnsByTable.end()) {
                return;
            }

            for (const auto &columnName : it->second) {
                if (isWrite) {
                    if (hasSingleWriteItems(columnName)) {
                        continue;
                    }
                } else {
                    if (hasSingleReadItems(columnName)) {
                        continue;
                    }
                }
                addWildcardForColumn(columnName, isWrite);
            }
        };

        if (globalReadWildcard) {
            for (const auto &columnName : _keyColumns) {
                if (hasSingleReadItems(columnName)) {
                    continue;
                }
                addWildcardForColumn(columnName, false);
            }
        } else {
            for (const auto &tableName : tablesTouchedRead) {
                addWildcardsForTable(tableName, false);
            }
        }

        if (globalWriteWildcard) {
            for (const auto &columnName : _keyColumns) {
                if (hasSingleWriteItems(columnName)) {
                    continue;
                }
                addWildcardForColumn(columnName, true);
            }
        } else {
            for (const auto &tableName : tablesTouchedWrite) {
                addWildcardsForTable(tableName, true);
            }
        }

        auto makeWildcardRange = [](const std::string &columnName) {
            return StateItem::Wildcard(columnName).MakeRange2();
        };

        auto computeCompositeHash = [](const std::vector<StateRange> &ranges) {
            std::size_t hash = 0;
            for (const auto &range : ranges) {
                hash ^= range.hash() + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            }
            return hash;
        };

        auto buildCompositeRange = [&](const std::vector<std::string> &columns,
                                       const std::unordered_map<std::string, StateRange> &rangeMap,
                                       bool groupTouched) -> std::optional<CompositeRange> {
            if (columns.empty()) {
                return std::nullopt;
            }

            bool hasAny = false;
            CompositeRange composite;
            composite.ranges.reserve(columns.size());

            for (const auto &columnName : columns) {
                auto it = rangeMap.find(columnName);
                if (it != rangeMap.end()) {
                    composite.ranges.push_back(it->second);
                    hasAny = true;
                } else if (hasAny || groupTouched) {
                    composite.ranges.push_back(makeWildcardRange(columnName));
                }
            }

            if (!hasAny && !groupTouched) {
                return std::nullopt;
            }

            if (composite.ranges.size() != columns.size()) {
                composite.ranges.clear();
                composite.ranges.reserve(columns.size());
                for (const auto &columnName : columns) {
                    composite.ranges.push_back(makeWildcardRange(columnName));
                }
            }

            composite.hash = computeCompositeHash(composite.ranges);
            return composite;
        };

        std::vector<std::pair<size_t, CompositeTask>> compositeTasks;
        compositeTasks.reserve(_keyColumnGroups.size());

        for (size_t index = 0; index < _keyColumnGroups.size(); index++) {
            const auto &group = _keyColumnGroups[index];
            if (group.empty()) {
                continue;
            }

            if (group.size() == 1) {
                const auto &columnName = group.front();
                if (groupReadTouched[index] && !hasSingleReadItems(columnName)) {
                    addWildcardForColumn(columnName, false);
                }
                if (groupWriteTouched[index] && !hasSingleWriteItems(columnName)) {
                    addWildcardForColumn(columnName, true);
                }
                continue;
            }

            CompositeTask task;
            task.nodeId = id;

            auto readRange = buildCompositeRange(group, compositeReadRanges, groupReadTouched[index]);
            if (readRange.has_value()) {
                task.readRanges.push_back(std::move(*readRange));
            }

            auto writeRange = buildCompositeRange(group, compositeWriteRanges, groupWriteTouched[index]);
            if (writeRange.has_value()) {
                task.writeRanges.push_back(std::move(*writeRange));
            }

            if (!task.readRanges.empty() || !task.writeRanges.empty()) {
                compositeTasks.emplace_back(index, std::move(task));
            }
        }

        const auto totalTasks = static_cast<uint32_t>(tasksByColumn.size() + compositeTasks.size());
        node->pendingColumns = totalTasks;
        if (totalTasks == 0) {
            node->ready = true;
            return id;
        }

        for (auto &pair : tasksByColumn) {
            enqueueTask(pair.first, std::move(pair.second));
        }

        for (auto &pair : compositeTasks) {
            enqueueCompositeTask(pair.first, std::move(pair.second));
        }

        return id;
    }
    
   
    /**
     * @copilot this function finds all entrypoints of the graph
     *  - an entrypoint is a node that has no incoming edges,
     *    or all incoming edges are marked as finalized (RowGraphNode::finalized == true)
     */
    std::unordered_set<RowGraphId> RowGraph::entrypoints() {
        ConcurrentReadLock _lock(_graphMutex);
        
        std::unordered_set<RowGraphId> result;
        
        auto it = boost::vertices(_graph).first;
        const auto itEnd = boost::vertices(_graph).second;
        
        
        while (it != itEnd) {
            auto id = *it;
            
            auto pair = boost::in_edges(id, _graph);
            auto it2 = pair.first;
            const auto it2End = pair.second;
            
            bool isEntrypoint = !_graph[id]->finalized && !_graph[id]->hold;
            
            while (isEntrypoint && it2 != it2End) {
                auto edge = *it2;
                auto source = boost::source(edge, _graph);
                
                auto &node = _graph[source];
                
                if (!node->finalized) {
                    isEntrypoint = false;
                    break;
                }
                
                ++it2;
            }
            
            if (isEntrypoint) {
                result.insert(id);
            }
            
            ++it;
        }
        
        return std::move(result);
    }
    
    bool RowGraph::isFinalized() {
        ConcurrentReadLock _lock(_graphMutex);
        auto pair = boost::vertices(_graph);
        
        return std::all_of(std::execution::unseq, pair.first, pair.second, [this](auto id) {
            return (bool) _graph[id]->finalized;
        });
    }
    
    RowGraphId RowGraph::entrypoint(int workerId) {
        ConcurrentReadLock _lock(_graphMutex);
        
        auto itBeg = boost::vertices(_graph).first;
        const auto itEnd = boost::vertices(_graph).second;
        
        auto it = std::find_if(std::execution::par, itBeg, itEnd, [this, workerId](auto id) {
            auto &node = _graph[id];
            int expected = -1;
            
            if (!node->ready || node->hold || node->finalized || node->processedBy != -1) {
                return false;
            }
            
            auto pair = boost::in_edges(id, _graph);
            auto it2Beg = pair.first;
            const auto it2End = pair.second;
            
            bool result = std::all_of(it2Beg, it2End, [this](const auto &edge) {
                auto source = boost::source(edge, _graph);
                auto &_node = _graph[source];
                
                return (bool) _node->finalized;
            });
            
            return result && node->processedBy.compare_exchange_strong(expected, workerId);
        });
        
        if (it != itEnd) {
            auto id = *it;
            return id;
        } else {
            return nullptr;
        }
    }
    
    std::shared_ptr<RowGraphNode> RowGraph::nodeFor(RowGraphId nodeId) {
        ConcurrentReadLock _lock(_graphMutex);
        return _graph[nodeId];
    }

    void RowGraph::addEdge(RowGraphId from, RowGraphId to) {
        if (from == nullptr || to == nullptr || from == to) {
            return;
        }
        WriteLock lock(_graphMutex);
        boost::add_edge(from, to, _graph);
    }

    void RowGraph::releaseNode(RowGraphId nodeId) {
        auto node = nodeFor(nodeId);
        if (node == nullptr) {
            return;
        }
        node->hold = false;
    }
    
    void RowGraph::pauseWorkers() {
        {
            std::lock_guard<std::mutex> lock(_gcMutex);
            _gcPause.store(true, std::memory_order_release);
        }
        notifyAllWorkers();
        _gcCv.notify_all();
        std::unique_lock<std::mutex> lock(_gcMutex);
        _gcCv.wait(lock, [this]() {
            return _activeTasks.load(std::memory_order_acquire) == 0 &&
                   _pausedWorkers.load(std::memory_order_acquire) >= _workerCount;
        });
    }

    void RowGraph::resumeWorkers() {
        {
            std::lock_guard<std::mutex> lock(_gcMutex);
            _gcPause.store(false, std::memory_order_release);
        }
        _gcCv.notify_all();
        notifyAllWorkers();
    }

    void RowGraph::notifyAllWorkers() {
        for (auto &pair : _columnWorkers) {
            pair.second->queueCv.notify_all();
        }
        for (auto &workerPtr : _compositeWorkers) {
            if (!workerPtr) {
                continue;
            }
            workerPtr->queueCv.notify_all();
        }
    }

    void RowGraph::gcInternal() {
        WriteLock _lock(_graphMutex);
        _logger->info("gc(): removing finalized / orphaned nodes");
        
        std::set<RowGraphId> toRemove;
        
        {
            boost::graph_traits<RowGraphInternal>::vertex_iterator vi, vi_end;
            boost::tie(vi, vi_end) = boost::vertices(_graph);
            
            std::for_each(vi, vi_end, [this, &toRemove](const auto &id) {
                auto node = _graph[id];

                const auto transactionPtr = std::atomic_load(&node->transaction);
                if (node->finalized && transactionPtr == nullptr) {
                    toRemove.emplace(id);
                }
            });
            
            for (auto id: toRemove) {
                // remove edges
                {
                    std::set<RowGraphId> edgeSources;
                    
                    auto pair = boost::in_edges(id, _graph);
                    auto it = pair.first;
                    const auto itEnd = pair.second;
                    
                    while (it != itEnd) {
                        auto edge = *it;
                        auto source = boost::source(edge, _graph);
                        
                        edgeSources.emplace(source);
                        
                        ++it;
                    }
                    
                    for (auto source: edgeSources) {
                        boost::remove_edge(source, id, _graph);
                    }
                }
                
                {
                    std::set<RowGraphId> edgeTargets;
                    
                    auto pair = boost::out_edges(id, _graph);
                    auto it = pair.first;
                    const auto itEnd = pair.second;
                    
                    while (it != itEnd) {
                        auto edge = *it;
                        auto target = boost::target(edge, _graph);
                        
                        edgeTargets.emplace(target);
                        
                        ++it;
                    }
                    
                    for (auto target: edgeTargets) {
                        boost::remove_edge(id, target, _graph);
                    }
                }
                
                boost::remove_vertex(id, _graph);
            }
            
            
            for (auto &pair: _columnWorkers) {
                auto &worker = pair.second;
                std::lock_guard<std::mutex> mapLock(worker->mapMutex);
                std::vector<StateRange> toRemoveRanges;
                
                for (auto &pair2: worker->nodeMap) {
                    auto &holder = pair2.second;
                    std::scoped_lock<std::mutex> holderLock(holder.mutex);
                    
                    if (toRemove.find(holder.read) != toRemove.end()) {
                        holder.read = nullptr;
                        holder.readGid = 0;
                    }
                    
                    if (toRemove.find(holder.write) != toRemove.end()) {
                        holder.write = nullptr;
                        holder.writeGid = 0;
                    }
                    
                    if (holder.read == nullptr && holder.write == nullptr) {
                        toRemoveRanges.emplace_back(pair2.first);
                    }
                }

                if (worker->hasWildcard) {
                    auto &holder = worker->wildcardHolder;
                    std::scoped_lock<std::mutex> holderLock(holder.mutex);

                    if (toRemove.find(holder.read) != toRemove.end()) {
                        holder.read = nullptr;
                        holder.readGid = 0;
                    }

                    if (toRemove.find(holder.write) != toRemove.end()) {
                        holder.write = nullptr;
                        holder.writeGid = 0;
                    }

                    if (holder.read == nullptr && holder.write == nullptr) {
                        worker->hasWildcard = false;
                    }
                }
                
                for (auto &range: toRemoveRanges) {
                    worker->nodeMap.erase(range);
                }
            }

            for (auto &workerPtr : _compositeWorkers) {
                if (!workerPtr) {
                    continue;
                }
                auto &worker = *workerPtr;
                std::lock_guard<std::mutex> mapLock(worker.mapMutex);
                std::vector<CompositeRange> toRemoveRanges;

                for (auto &pair2 : worker.nodeMap) {
                    auto &holder = pair2.second;
                    std::scoped_lock<std::mutex> holderLock(holder.mutex);

                    if (toRemove.find(holder.read) != toRemove.end()) {
                        holder.read = nullptr;
                        holder.readGid = 0;
                    }

                    if (toRemove.find(holder.write) != toRemove.end()) {
                        holder.write = nullptr;
                        holder.writeGid = 0;
                    }

                    if (holder.read == nullptr && holder.write == nullptr) {
                        toRemoveRanges.emplace_back(pair2.first);
                    }
                }

                if (worker.hasWildcard) {
                    auto &holder = worker.wildcardHolder;
                    std::scoped_lock<std::mutex> holderLock(holder.mutex);

                    if (toRemove.find(holder.read) != toRemove.end()) {
                        holder.read = nullptr;
                        holder.readGid = 0;
                    }

                    if (toRemove.find(holder.write) != toRemove.end()) {
                        holder.write = nullptr;
                        holder.writeGid = 0;
                    }

                    if (holder.read == nullptr && holder.write == nullptr) {
                        worker.hasWildcard = false;
                    }
                }

                for (auto &range : toRemoveRanges) {
                    worker.nodeMap.erase(range);
                }
            }
            
        }
        
        if (!toRemove.empty()) {
            _logger->info("gc(): {} nodes removed", toRemove.size());
        }
    }

    void RowGraph::gc() {
        bool expected = false;
        if (!_isGCRunning.compare_exchange_strong(expected, true)) {
            return;
        }
        pauseWorkers();
        gcInternal();
        resumeWorkers();
        _isGCRunning.store(false, std::memory_order_release);
    }
    
    void RowGraph::enqueueTask(const std::string &column, ColumnTask task) {
        auto it = _columnWorkers.find(column);
        if (it == _columnWorkers.end()) {
            markColumnTaskDone(task.nodeId);
            return;
        }
        
        auto &worker = it->second;
        {
            std::lock_guard<std::mutex> lock(worker->queueMutex);
            worker->queue.push_back(std::move(task));
        }
        worker->queueCv.notify_one();
    }

    void RowGraph::enqueueCompositeTask(size_t groupIndex, CompositeTask task) {
        if (groupIndex >= _compositeWorkers.size()) {
            markColumnTaskDone(task.nodeId);
            return;
        }
        auto &workerPtr = _compositeWorkers[groupIndex];
        if (!workerPtr) {
            markColumnTaskDone(task.nodeId);
            return;
        }
        auto &worker = *workerPtr;
        {
            std::lock_guard<std::mutex> lock(worker.queueMutex);
            worker.queue.push_back(std::move(task));
        }
        worker.queueCv.notify_one();
    }
    
    void RowGraph::columnWorkerLoop(ColumnWorker &worker) {
        bool paused = false;
        while (true) {
            ColumnTask task;
            {
                std::unique_lock<std::mutex> lock(worker.queueMutex);
                worker.queueCv.wait(lock, [this, &worker]() {
                    return !worker.queue.empty() || !worker.running ||
                           _gcPause.load(std::memory_order_acquire);
                });

                if (_gcPause.load(std::memory_order_acquire)) {
                    lock.unlock();
                    std::unique_lock<std::mutex> pauseLock(_gcMutex);
                    if (!paused) {
                        _pausedWorkers.fetch_add(1, std::memory_order_acq_rel);
                        paused = true;
                        _gcCv.notify_all();
                    }
                    _gcCv.wait(pauseLock, [this]() {
                        return !_gcPause.load(std::memory_order_acquire);
                    });
                    if (paused) {
                        _pausedWorkers.fetch_sub(1, std::memory_order_acq_rel);
                        paused = false;
                        _gcCv.notify_all();
                    }
                    continue;
                }

                if (!worker.running && worker.queue.empty()) {
                    return;
                }

                task = std::move(worker.queue.front());
                worker.queue.pop_front();
            }

            _activeTasks.fetch_add(1, std::memory_order_acq_rel);
            processColumnTask(worker, task);
            _activeTasks.fetch_sub(1, std::memory_order_acq_rel);
            _gcCv.notify_all();
            markColumnTaskDone(task.nodeId);
        }
    }

    void RowGraph::compositeWorkerLoop(CompositeWorker &worker) {
        bool paused = false;
        while (true) {
            CompositeTask task;
            {
                std::unique_lock<std::mutex> lock(worker.queueMutex);
                worker.queueCv.wait(lock, [this, &worker]() {
                    return !worker.queue.empty() || !worker.running ||
                           _gcPause.load(std::memory_order_acquire);
                });

                if (_gcPause.load(std::memory_order_acquire)) {
                    lock.unlock();
                    std::unique_lock<std::mutex> pauseLock(_gcMutex);
                    if (!paused) {
                        _pausedWorkers.fetch_add(1, std::memory_order_acq_rel);
                        paused = true;
                        _gcCv.notify_all();
                    }
                    _gcCv.wait(pauseLock, [this]() {
                        return !_gcPause.load(std::memory_order_acquire);
                    });
                    if (paused) {
                        _pausedWorkers.fetch_sub(1, std::memory_order_acq_rel);
                        paused = false;
                        _gcCv.notify_all();
                    }
                    continue;
                }

                if (!worker.running && worker.queue.empty()) {
                    return;
                }

                task = std::move(worker.queue.front());
                worker.queue.pop_front();
            }

            _activeTasks.fetch_add(1, std::memory_order_acq_rel);
            processCompositeTask(worker, task);
            _activeTasks.fetch_sub(1, std::memory_order_acq_rel);
            _gcCv.notify_all();
            markColumnTaskDone(task.nodeId);
        }
    }
    
    void RowGraph::processColumnTask(ColumnWorker &worker, ColumnTask &task) {
        auto node = nodeFor(task.nodeId);
        if (node == nullptr) {
            return;
        }
        const auto transactionPtr = std::atomic_load(&node->transaction);
        if (transactionPtr == nullptr) {
            return;
        }

        const auto gid = transactionPtr->gid();
        const auto comparisonMethod = rangeComparisonMethod();
        std::unordered_set<RowGraphId> edgeSources;
        edgeSources.reserve(task.readItems.size() + task.writeItems.size());
        
        auto addEdgeSource = [&](RowGraphId source, gid_t sourceGid) {
            if (source == nullptr || source == task.nodeId) {
                return;
            }
            if (sourceGid != 0 && sourceGid <= gid) {
                edgeSources.insert(source);
            }
        };

        auto addEdgesFromWildcardHolder = [&](bool isWrite) {
            std::unique_lock<std::mutex> mapLock(worker.mapMutex);
            if (!worker.hasWildcard) {
                return;
            }

            RWStateHolder &holder = worker.wildcardHolder;
            std::unique_lock<std::mutex> holderLock(holder.mutex);
            mapLock.unlock();

            if (isWrite) {
                addEdgeSource(holder.read, holder.readGid);
                addEdgeSource(holder.write, holder.writeGid);
            } else {
                addEdgeSource(holder.write, holder.writeGid);
            }
        };

        auto processWildcardItem = [&](bool isWrite) {
            std::unique_lock<std::mutex> mapLock(worker.mapMutex);

            for (auto &pair : worker.nodeMap) {
                auto &holder = pair.second;
                std::unique_lock<std::mutex> holderLock(holder.mutex);
                if (isWrite) {
                    addEdgeSource(holder.read, holder.readGid);
                    addEdgeSource(holder.write, holder.writeGid);
                } else {
                    addEdgeSource(holder.write, holder.writeGid);
                }
            }

            if (worker.hasWildcard) {
                auto &holder = worker.wildcardHolder;
                std::unique_lock<std::mutex> holderLock(holder.mutex);
                if (isWrite) {
                    addEdgeSource(holder.read, holder.readGid);
                    addEdgeSource(holder.write, holder.writeGid);
                } else {
                    addEdgeSource(holder.write, holder.writeGid);
                }
            }

            worker.hasWildcard = true;
            {
                auto &holder = worker.wildcardHolder;
                std::unique_lock<std::mutex> holderLock(holder.mutex);
                if (isWrite) {
                    holder.write = task.nodeId;
                    holder.writeGid = gid;
                } else {
                    holder.read = task.nodeId;
                    holder.readGid = gid;
                }
            }
        };
        
        auto withHolder = [&](const StateItem &item, auto &&fn) {
            const auto &range = item.MakeRange2();
            
            std::unique_lock<std::mutex> mapLock(worker.mapMutex);
            auto it = std::find_if(worker.nodeMap.begin(), worker.nodeMap.end(),
                                   [comparisonMethod, &range](const auto &pair) {
                                       if (comparisonMethod == RangeComparisonMethod::EQ_ONLY) {
                                           return pair.first == range;
                                       } else if (comparisonMethod == RangeComparisonMethod::INTERSECT) {
                                           return pair.first == range || StateRange::isIntersects(range, pair.first);
                                       }
                                       return false;
                                   });
            
            if (it == worker.nodeMap.end()) {
                it = worker.nodeMap.try_emplace(range).first;
            }
            
            RWStateHolder &holder = it->second;
            std::unique_lock<std::mutex> holderLock(holder.mutex);
            mapLock.unlock();
            fn(holder);
        };

        auto processItem = [&](const StateItem &item, bool isWrite) {
            const auto &range = item.MakeRange2();
            if (item.function_type == FUNCTION_WILDCARD || range.wildcard()) {
                processWildcardItem(isWrite);
                return;
            }

            addEdgesFromWildcardHolder(isWrite);

            withHolder(item, [&](RWStateHolder &holder) {
                if (isWrite) {
                    addEdgeSource(holder.read, holder.readGid);
                    addEdgeSource(holder.write, holder.writeGid);
                    holder.write = task.nodeId;
                    holder.writeGid = gid;
                } else {
                    addEdgeSource(holder.write, holder.writeGid);
                    holder.read = task.nodeId;
                    holder.readGid = gid;
                }
            });
        };

        for (const auto &item : task.readItems) {
            processItem(item, false);
        }

        for (const auto &item : task.writeItems) {
            processItem(item, true);
        }
        
        if (!edgeSources.empty()) {
            WriteLock lock(_graphMutex);
            for (auto source : edgeSources) {
                boost::add_edge(source, task.nodeId, _graph);
            }
        }
    }

    void RowGraph::processCompositeTask(CompositeWorker &worker, CompositeTask &task) {
        auto node = nodeFor(task.nodeId);
        if (node == nullptr) {
            return;
        }
        const auto transactionPtr = std::atomic_load(&node->transaction);
        if (transactionPtr == nullptr) {
            return;
        }

        const auto gid = transactionPtr->gid();
        const auto comparisonMethod = rangeComparisonMethod();
        std::unordered_set<RowGraphId> edgeSources;
        edgeSources.reserve(task.readRanges.size() + task.writeRanges.size());

        auto addEdgeSource = [&](RowGraphId source, gid_t sourceGid) {
            if (source == nullptr || source == task.nodeId) {
                return;
            }
            if (sourceGid != 0 && sourceGid <= gid) {
                edgeSources.insert(source);
            }
        };

        auto compositeIntersects = [](const CompositeRange &lhs, const CompositeRange &rhs) {
            if (lhs.ranges.size() != rhs.ranges.size()) {
                return false;
            }

            for (size_t index = 0; index < lhs.ranges.size(); index++) {
                const auto &left = lhs.ranges[index];
                const auto &right = rhs.ranges[index];
                if (left.wildcard() || right.wildcard()) {
                    continue;
                }
                if (!StateRange::isIntersects(left, right)) {
                    return false;
                }
            }
            return true;
        };

        auto addEdgesFromWildcardHolder = [&](bool isWrite) {
            std::unique_lock<std::mutex> mapLock(worker.mapMutex);
            if (!worker.hasWildcard) {
                return;
            }

            RWStateHolder &holder = worker.wildcardHolder;
            std::unique_lock<std::mutex> holderLock(holder.mutex);
            mapLock.unlock();

            if (isWrite) {
                addEdgeSource(holder.read, holder.readGid);
                addEdgeSource(holder.write, holder.writeGid);
            } else {
                addEdgeSource(holder.write, holder.writeGid);
            }
        };

        auto processGlobalWildcard = [&](bool isWrite) {
            std::unique_lock<std::mutex> mapLock(worker.mapMutex);

            for (auto &pair : worker.nodeMap) {
                auto &holder = pair.second;
                std::unique_lock<std::mutex> holderLock(holder.mutex);
                if (isWrite) {
                    addEdgeSource(holder.read, holder.readGid);
                    addEdgeSource(holder.write, holder.writeGid);
                } else {
                    addEdgeSource(holder.write, holder.writeGid);
                }
            }

            if (worker.hasWildcard) {
                auto &holder = worker.wildcardHolder;
                std::unique_lock<std::mutex> holderLock(holder.mutex);
                if (isWrite) {
                    addEdgeSource(holder.read, holder.readGid);
                    addEdgeSource(holder.write, holder.writeGid);
                } else {
                    addEdgeSource(holder.write, holder.writeGid);
                }
            }

            worker.hasWildcard = true;
            {
                auto &holder = worker.wildcardHolder;
                std::unique_lock<std::mutex> holderLock(holder.mutex);
                if (isWrite) {
                    holder.write = task.nodeId;
                    holder.writeGid = gid;
                } else {
                    holder.read = task.nodeId;
                    holder.readGid = gid;
                }
            }
        };

        auto withHolder = [&](const CompositeRange &range, auto &&fn) {
            std::unique_lock<std::mutex> mapLock(worker.mapMutex);
            auto it = std::find_if(worker.nodeMap.begin(), worker.nodeMap.end(),
                                   [comparisonMethod, &range, &compositeIntersects](const auto &pair) {
                                       if (comparisonMethod == RangeComparisonMethod::EQ_ONLY) {
                                           return pair.first.ranges == range.ranges;
                                       }
                                       if (comparisonMethod == RangeComparisonMethod::INTERSECT) {
                                           return pair.first.ranges == range.ranges || compositeIntersects(pair.first, range);
                                       }
                                       return false;
                                   });

            if (it == worker.nodeMap.end()) {
                it = worker.nodeMap.try_emplace(range).first;
            }

            RWStateHolder &holder = it->second;
            std::unique_lock<std::mutex> holderLock(holder.mutex);
            mapLock.unlock();
            fn(holder);
        };

        auto processRange = [&](const CompositeRange &range, bool isWrite) {
            if (range.isGlobalWildcard()) {
                processGlobalWildcard(isWrite);
                return;
            }

            addEdgesFromWildcardHolder(isWrite);

            withHolder(range, [&](RWStateHolder &holder) {
                if (isWrite) {
                    addEdgeSource(holder.read, holder.readGid);
                    addEdgeSource(holder.write, holder.writeGid);
                    holder.write = task.nodeId;
                    holder.writeGid = gid;
                } else {
                    addEdgeSource(holder.write, holder.writeGid);
                    holder.read = task.nodeId;
                    holder.readGid = gid;
                }
            });
        };

        for (const auto &range : task.readRanges) {
            processRange(range, false);
        }

        for (const auto &range : task.writeRanges) {
            processRange(range, true);
        }

        if (!edgeSources.empty()) {
            WriteLock lock(_graphMutex);
            for (auto source : edgeSources) {
                boost::add_edge(source, task.nodeId, _graph);
            }
        }
    }
    
    void RowGraph::markColumnTaskDone(RowGraphId nodeId) {
        auto node = nodeFor(nodeId);
        if (node == nullptr) {
            return;
        }
        
        auto remaining = node->pendingColumns.fetch_sub(1);
        if (remaining == 1) {
            node->ready = true;
        }
    }

    void RowGraph::dump() {
    }
    
    RangeComparisonMethod RowGraph::rangeComparisonMethod() const {
        return _rangeComparisonMethod;
    }
    
    void RowGraph::setRangeComparisonMethod(RangeComparisonMethod rangeComparisonMethod) {
        _rangeComparisonMethod = rangeComparisonMethod;
    }

// #ifdef ULTRAVERSE_TESTING
    size_t RowGraph::debugNodeMapSize(const std::string &column) {
        const auto normalized = utility::toLower(column);
        auto it = _columnWorkers.find(normalized);
        if (it == _columnWorkers.end()) {
            auto groupIt = _groupIndexByColumn.find(normalized);
            if (groupIt == _groupIndexByColumn.end()) {
                return 0;
            }
            const auto groupIndex = groupIt->second;
            if (groupIndex >= _compositeWorkers.size()) {
                return 0;
            }
            auto &workerPtr = _compositeWorkers[groupIndex];
            if (!workerPtr) {
                return 0;
            }
            auto &worker = *workerPtr;
            std::lock_guard<std::mutex> lock(worker.mapMutex);
            return worker.nodeMap.size();
        }
        auto &worker = it->second;
        std::lock_guard<std::mutex> lock(worker->mapMutex);
        return worker->nodeMap.size();
    }

    size_t RowGraph::debugTotalNodeMapSize() {
        size_t total = 0;
        for (auto &pair : _columnWorkers) {
            auto &worker = pair.second;
            std::lock_guard<std::mutex> lock(worker->mapMutex);
            total += worker->nodeMap.size();
        }
        for (auto &workerPtr : _compositeWorkers) {
            if (!workerPtr) {
                continue;
            }
            auto &worker = *workerPtr;
            std::lock_guard<std::mutex> lock(worker.mapMutex);
            total += worker.nodeMap.size();
        }
        return total;
    }
// #endif
}
