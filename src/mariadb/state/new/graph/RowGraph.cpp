//
// Created by cheesekun on 7/10/23.
//

#include <algorithm>
#include <cmath>
#include <execution>
#include <fstream>
#include <unordered_set>

#include <boost/graph/graphviz.hpp>

#include <fmt/format.h>


#include "utils/StringUtil.hpp"

#include "RowGraph.hpp"

#include "../cluster/StateRelationshipResolver.hpp"

namespace ultraverse::state::v2 {
    RowGraph::RowGraph(const std::set<std::string> &keyColumns, const RelationshipResolver &resolver):
        _logger(createLogger("RowGraph")),
        _keyColumns(keyColumns),
        _resolver(resolver),
        _rangeComparisonMethod(RangeComparisonMethod::EQ_ONLY)
    {
        for (const auto &column : _keyColumns) {
            auto worker = std::make_unique<ColumnWorker>();
            worker->column = column;
            worker->worker = std::thread(&RowGraph::columnWorkerLoop, this, std::ref(*worker));
            _columnWorkers.emplace(column, std::move(worker));
        }
    }
    
    RowGraph::~RowGraph() {
        for (auto &pair : _columnWorkers) {
            auto &worker = pair.second;
            {
                std::lock_guard<std::mutex> lock(worker->queueMutex);
                worker->running = false;
            }
            worker->queueCv.notify_all();
        }
        
        for (auto &pair : _columnWorkers) {
            auto &worker = pair.second;
            if (worker->worker.joinable()) {
                worker->worker.join();
            }
        }
    }
    
    RowGraphId RowGraph::addNode(std::shared_ptr<Transaction> transaction, bool hold) {
        auto node = std::make_shared<RowGraphNode>();
        node->transaction = std::move(transaction);
        node->hold = hold;
        
        RowGraphId id = nullptr;
        {
            WriteLock _lock(_graphMutex);
            id = boost::add_vertex(node, _graph);
        }
        
        std::unordered_map<std::string, ColumnTask> tasksByColumn;
        tasksByColumn.reserve(_keyColumns.size());
        
        for (auto it = node->transaction->readSet_begin(); it != node->transaction->readSet_end(); ++it) {
            const auto &item = *it;
            if (_keyColumns.find(item.name) == _keyColumns.end()) {
                continue;
            }
            auto &task = tasksByColumn[item.name];
            task.nodeId = id;
            task.readItems.push_back(item);
        }
        
        for (auto it = node->transaction->writeSet_begin(); it != node->transaction->writeSet_end(); ++it) {
            const auto &item = *it;
            if (_keyColumns.find(item.name) == _keyColumns.end()) {
                continue;
            }
            auto &task = tasksByColumn[item.name];
            task.nodeId = id;
            task.writeItems.push_back(item);
        }
        
        node->pendingColumns = static_cast<uint32_t>(tasksByColumn.size());
        if (tasksByColumn.empty()) {
            node->ready = true;
            return id;
        }
        
        for (auto &pair : tasksByColumn) {
            enqueueTask(pair.first, std::move(pair.second));
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
    
    void RowGraph::gc() {
        WriteLock _lock(_graphMutex);
        _logger->info("gc(): removing finalized / orphaned nodes");
        
        std::set<RowGraphId> toRemove;
        
        {
            boost::graph_traits<RowGraphInternal>::vertex_iterator vi, vi_end;
            boost::tie(vi, vi_end) = boost::vertices(_graph);
            
            std::for_each(vi, vi_end, [this, &toRemove](const auto &id) {
                auto node = _graph[id];
                
                if (node->finalized && node->transaction == nullptr) {
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
                
                for (auto &range: toRemoveRanges) {
                    worker->nodeMap.erase(range);
                }
            }
            
        }
        
        if (!toRemove.empty()) {
            _logger->info("gc(): {} nodes removed", toRemove.size());
        }
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
    
    void RowGraph::columnWorkerLoop(ColumnWorker &worker) {
        while (true) {
            ColumnTask task;
            {
                std::unique_lock<std::mutex> lock(worker.queueMutex);
                worker.queueCv.wait(lock, [&worker]() {
                    return !worker.queue.empty() || !worker.running;
                });
                
                if (!worker.running && worker.queue.empty()) {
                    return;
                }
                
                task = std::move(worker.queue.front());
                worker.queue.pop_front();
            }
            
            processColumnTask(worker, task);
            markColumnTaskDone(task.nodeId);
        }
    }
    
    void RowGraph::processColumnTask(ColumnWorker &worker, ColumnTask &task) {
        auto node = nodeFor(task.nodeId);
        if (node == nullptr || node->transaction == nullptr) {
            return;
        }
        
        const auto gid = node->transaction->gid();
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
        
        for (const auto &item : task.readItems) {
            withHolder(item, [&](RWStateHolder &holder) {
                addEdgeSource(holder.write, holder.writeGid);
                holder.read = task.nodeId;
                holder.readGid = gid;
            });
        }
        
        for (const auto &item : task.writeItems) {
            withHolder(item, [&](RWStateHolder &holder) {
                addEdgeSource(holder.read, holder.readGid);
                addEdgeSource(holder.write, holder.writeGid);
                holder.write = task.nodeId;
                holder.writeGid = gid;
            });
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
        auto it = _columnWorkers.find(column);
        if (it == _columnWorkers.end()) {
            return 0;
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
        return total;
    }
// #endif
}
