//
// Created by cheesekun on 7/10/23.
//

#include <cmath>
#include <fstream>
#include <future>
#include <execution>

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
        _taskExecutor(std::thread::hardware_concurrency() * 2),
        _rangeComparisonMethod(RangeComparisonMethod::EQ_ONLY)
    {
    
    }
    
    RowGraph::~RowGraph() {
        _taskExecutor.shutdown();
    }
    
    RowGraphId RowGraph::addNode(std::shared_ptr<Transaction> transaction) {
        auto node = std::make_shared<RowGraphNode>();
        node->transaction = std::move(transaction);
        
        
        RowGraphId id = nullptr;
        {
            WriteLock _lock(_graphMutex);
            id = boost::add_vertex(node, _graph);
        }
        
        
        _taskExecutor.post<int>([this, id] {
            this->buildEdge(id);
            return 0;
        });

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
            
            bool isEntrypoint = !_graph[id]->finalized;
            
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
            
            if (!node->ready || node->finalized || node->processedBy != -1) {
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
            
            
            for (auto &pair: _nodeMap) {
                std::vector<StateRange> toRemoveRanges;
                
                for (auto &pair2: pair.second) {
                    auto &holder = pair2.second;
                    
                    if (toRemove.find(holder.read) != toRemove.end()) {
                        holder.read = nullptr;
                    }
                    
                    if (toRemove.find(holder.write) != toRemove.end()) {
                        holder.write = nullptr;
                    }
                    
                    if (holder.read == nullptr && holder.write == nullptr) {
                        toRemoveRanges.emplace_back(pair2.first);
                    }
                }
                
                for (auto &range: toRemoveRanges) {
                    pair.second.erase(range);
                }
            }
            
        }
        
        if (!toRemove.empty()) {
            _logger->info("gc(): {} nodes removed", toRemove.size());
        }
    }
    
    void RowGraph::buildEdge(RowGraphId nodeId) {
        auto node = nodeFor(nodeId);
        const auto transaction = node->transaction;
        const auto gid = transaction->gid();
        
        const auto getRWStateHolder = [this](const StateItem &item) {
            std::optional<std::reference_wrapper<RWStateHolder>> holderRef = std::nullopt;
            std::string name = item.name;
            
            if (name.empty()) {
                name = item.name;
            }
            
            if (_keyColumns.find(name) == _keyColumns.end()) {
                return holderRef;
            }
            
            const auto &range = item.MakeRange2();
            const auto comparisonMethod = rangeComparisonMethod();
            
            {
                std::shared_lock<std::shared_mutex> _lock(_nodeMapMutex);
                auto &map = _nodeMap[name];
                
                auto it = std::find_if(map.begin(), map.end(), [comparisonMethod, &range](const auto &pair) {
                    if (comparisonMethod == RangeComparisonMethod::EQ_ONLY) {
                        return pair.first == range;
                    } else if (comparisonMethod == RangeComparisonMethod::INTERSECT) {
                        return pair.first == range || StateRange::isIntersects(range, pair.first);
                    } else {
                        return false;
                    }
                });
                
                if (it != map.end()) {
                    holderRef = std::make_optional(std::ref(it->second));
                }
            }
            
            if (holderRef == std::nullopt) {
                std::unique_lock<std::shared_mutex> _lock(_nodeMapMutex);
                auto &map = _nodeMap[name];
                
                holderRef = std::make_optional(std::ref(map[range]));
            }
            
            return holderRef;
        };
        
        std::unordered_set<RowGraphId> relations;
        std::shared_mutex relationsMutex;
        
        auto isInRelationship = [this, gid, &relations, &relationsMutex] (RowGraphId id) {
            const auto &targetNode = _graph[id];
            
            if (targetNode->transaction == nullptr) {
                return true;
            }
            
            if (gid < targetNode->transaction->gid()) {
                return true;
            }
            
            std::shared_lock<std::shared_mutex> _lock(relationsMutex);
            return relations.find(id) != relations.end();
        };
        
        auto addRelationship = [&relations, &relationsMutex] (RowGraphId id) {
            std::unique_lock<std::shared_mutex> _lock(relationsMutex);
            relations.insert(id);
        };
        
        
        // prevent reference to self
        relations.insert(nodeId);
        
        {
            // WRITE - READ / READ - READ
            auto it = transaction->readSet_begin();
            const auto itEnd = transaction->readSet_end();
            
            std::for_each(it, itEnd, [this, &relations, &getRWStateHolder, &isInRelationship, &addRelationship, nodeId] (const auto &item) {
                auto holderRef = getRWStateHolder(item);
                if (holderRef == std::nullopt) {
                    return;
                }
                
                auto &holder = holderRef->get();
                
                {
                    std::scoped_lock<std::mutex> _lock(holder.mutex);
                    
                    
                    if (holder.write != nullptr && !isInRelationship(holder.write)) {
                        // WRITE - READ
                        std::unique_lock<std::shared_mutex> _lock2(_nodeMapMutex);
                        boost::add_edge(holder.write, nodeId, _graph);
                        addRelationship(holder.write);
                    }
                
                    holder.read = nodeId;
                }
            });
        }
        
        {
            // WRITE - WRITE, READ - WRITE
            auto it = transaction->writeSet_begin();
            const auto itEnd = transaction->writeSet_end();
            
            std::for_each(it, itEnd, [this, &relations, &getRWStateHolder, &isInRelationship, &addRelationship, nodeId] (const auto &item) {
                auto holderRef = getRWStateHolder(item);
                if (holderRef == std::nullopt) {
                    return;
                }
                
                auto &holder = holderRef->get();
                
                {
                    std::scoped_lock<std::mutex> _lock(holder.mutex);
                    
                    if (holder.read != nullptr && !isInRelationship(holder.read)) {
                        // READ - WRITE
                        std::unique_lock<std::shared_mutex> _lock2(_nodeMapMutex);
                        boost::add_edge(holder.read, nodeId, _graph);
                        addRelationship(holder.read);
                    }
                    
                    if (holder.write != nullptr && !isInRelationship(holder.write)) {
                        // WRITE - WRITE
                        std::unique_lock<std::shared_mutex> _lock2(_nodeMapMutex);
                        boost::add_edge(holder.write, nodeId, _graph);
                        relations.insert(holder.write);
                        addRelationship(holder.write);
                    }
                    
                    holder.write = nodeId;
                }
            });
        }
 
        node->ready = true;
    }

    void RowGraph::dump() {
    }
    
    RangeComparisonMethod RowGraph::rangeComparisonMethod() const {
        return _rangeComparisonMethod;
    }
    
    void RowGraph::setRangeComparisonMethod(RangeComparisonMethod rangeComparisonMethod) {
        _rangeComparisonMethod = rangeComparisonMethod;
    }
}