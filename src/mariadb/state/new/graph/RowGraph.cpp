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
        _resolver(resolver)
    {
    
    }
    
    RowGraphId RowGraph::addNode(std::shared_ptr<Transaction> transaction) {
        auto node = std::make_shared<RowGraphNode>();
        node->transaction = std::move(transaction);
        
        
        _mutex.lock();
        auto id = boost::add_vertex(node, _graph);
        
        buildEdge(id);
        _mutex.unlock();
        
        return id;
    }
    
    std::unordered_set<RowGraphId> RowGraph::dependenciesOf(RowGraphId nodeId) {
        std::unordered_set<RowGraphId> result;
        
        {
            std::scoped_lock<std::mutex> _lock(_mutex);
            // get all edges points to nodeId (u -> v; v is nodeId)
            
            auto pair = boost::in_edges(nodeId, _graph);
            auto it = pair.first;
            const auto itEnd = pair.second;
            
            while (it != itEnd) {
                auto id = boost::source(*it, _graph);
                result.insert(id);
                
                ++it;
            }
        }
        
        return std::move(result);
    }
    
    std::unordered_set<RowGraphId> RowGraph::dependentsOf(RowGraphId nodeId) {
        std::unordered_set<RowGraphId> result;
        
        {
            std::scoped_lock<std::mutex> _lock(_mutex);
            // get all edges points from nodeId (u -> v; u is nodeId)
            
            auto pair = boost::out_edges(nodeId, _graph);
            auto it = pair.first;
            const auto itEnd = pair.second;
            
            while (it != itEnd) {
                auto id = boost::target(*it, _graph);
                result.insert(id);
                
                ++it;
            }
        }
        
        return std::move(result);
    }
    
    /**
     * @copilot this function finds all entrypoints of the graph
     *  - an entrypoint is a node that has no incoming edges,
     *    or all incoming edges are marked as finalized (RowGraphNode::finalized == true)
     */
    std::unordered_set<RowGraphId> RowGraph::entrypoints() {
        std::scoped_lock<std::mutex> _lock(_mutex);
        
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
    
    bool RowGraph::isFinalized() const {
        auto pair = boost::vertices(_graph);
        
        return std::all_of(pair.first, pair.second, [this](auto id) {
            return (bool) _graph[id]->finalized;
        });
    }
    
    RowGraphId RowGraph::entrypoint(int workerId) {
        std::scoped_lock<std::mutex> _lock(_mutex);
        
        auto itBeg = boost::vertices(_graph).first;
        const auto itEnd = boost::vertices(_graph).second;
        
        auto it = std::find_if(std::execution::par_unseq, itBeg, itEnd, [this](auto id) {
            auto &node = _graph[id];
            if (!node->finalized && node->processedBy == -1) {
                auto pair = boost::in_edges(id, _graph);
                auto it2Beg = pair.first;
                const auto it2End = pair.second;
                
                return std::all_of(it2Beg, it2End, [this](const auto &edge) {
                    auto source = boost::source(edge, _graph);
                    auto &_node = _graph[source];
                    
                    return (bool) _node->finalized;
                });
            }

            return false;
        });
        
        if (it != itEnd) {
            auto id = *it;
            _graph[id]->processedBy = workerId;
            return id;
        } else {
            return nullptr;
        }
    }
    
    std::shared_ptr<RowGraphNode> RowGraph::nodeFor(RowGraphId nodeId) {
        // std::scoped_lock<std::mutex> _lock(_mutex);
        
        return _graph[nodeId];
    }
    
    void RowGraph::gc() {
        // _logger->info("performing gc..");
        _logger->info("gc(): removing finalized / orphaned nodes");
        
        std::mutex _removeRowMutex;
        std::set<RowGraphId> toRemove;
        
        {
            std::scoped_lock<std::mutex> _lock(_mutex);
            
            boost::graph_traits<RowGraphInternal>::vertex_iterator vi, vi_end;
            boost::tie(vi, vi_end) = boost::vertices(_graph);
            
            std::for_each(std::execution::par_unseq, vi, vi_end, [this, &_removeRowMutex, &toRemove](const auto &id) {
                auto node = _graph[id];
                
                if (node->finalized && node->transaction == nullptr) {
                    std::scoped_lock<std::mutex> _lock(_removeRowMutex);
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
                for (auto &pair2: pair.second) {
                    auto &holder = pair2.second;
                    
                    if (toRemove.find(holder.read) != toRemove.end()) {
                        holder.read = nullptr;
                    }
                    
                    if (toRemove.find(holder.write) != toRemove.end()) {
                        holder.write = nullptr;
                    }
                }
            }
        }
        
        if (!toRemove.empty()) {
            _logger->info("gc(): {} nodes removed", toRemove.size());
        }
    }
    
    void RowGraph::buildEdge(RowGraphId nodeId) {
        auto &node = _graph[nodeId];
        const auto &transaction = node->transaction;
        
        std::unordered_set<RowGraphId> relations;
        // prevent reference to self
        relations.insert(nodeId);
        
        std::mutex _mapMutex;
        
        std::vector<std::pair<std::reference_wrapper<RWStateHolder>, int>> defers;
        
        {
            // WRITE - READ / READ - READ
            auto it = transaction->whereSet_begin();
            const auto itEnd = transaction->whereSet_end();
            
            std::for_each(std::execution::par, it, itEnd, [this, &relations, &defers, &_mapMutex, nodeId] (const auto &item) {
                std::string name = std::move(_resolver.resolveChain(item.name));
                
                if (name.empty()) {
                    name = item.name;
                }
                
                if (_keyColumns.find(name) == _keyColumns.end()) {
                    return;
                }
                
                const auto &range = item.MakeRange2();
                auto &map = _nodeMap[item.name];
                
                auto it2 = std::find_if(map.begin(), map.end(), [&range](const auto &pair) {
                    return StateRange::isIntersects(range, pair.first);
                });
                
                if (it2 != map.end()) {
                    // WRITE - READ / READ - READ
                    auto &holder = it2->second;
                    
                    /*
                    if (holder.read != UINT64_MAX && relations.find(holder.read) == relations.end()) {
                        // READ - READ
                        if (nodeId - holder.read <= 10) {
                            boost::add_edge(holder.read, nodeId, _graph);
                            relations.insert(holder.read);
                        }
                    }
                     */
                    
                    if (holder.write != nullptr && relations.find(holder.write) == relations.end()) {
                        // WRITE - READ
                        std::scoped_lock<std::mutex> _mapLock(_mapMutex);
                        boost::add_edge(holder.write, nodeId, _graph);
                        relations.insert(holder.write);
                    }
                    
                    {
                        std::scoped_lock<std::mutex> _mapLock(_mapMutex);
                        defers.emplace_back(std::ref(holder), 0);
                    }
                } else {
                    std::scoped_lock<std::mutex> _mapLock(_mapMutex);
                    map.emplace(range, RWStateHolder { nodeId, nullptr });
                }
            });
        }
        
        {
            // WRITE - WRITE, READ - WRITE
            auto it = transaction->itemSet_begin();
            const auto itEnd = transaction->itemSet_end();
            
            std::for_each(std::execution::par, it, itEnd, [this, &relations, &defers, &_mapMutex, &transaction, nodeId] (const auto &item) {
                std::string name = std::move(_resolver.resolveChain(item.name));
                
                if (name.empty()) {
                    name = item.name;
                }
                
                /*
                auto tableName = utility::splitTableName(name).first;
                bool isInWriteSet = (
                    (transaction->writeSet().find(name) != transaction->writeSet().end()) ||
                    (transaction->writeSet().find(tableName + ".*") == transaction->writeSet().end())
                );
                
                if (!isInWriteSet) {
                    return;
                }
                
                if (_keyColumns.find(name) == _keyColumns.end()) {
                    return;
                }
                 */
                
                const auto &range = item.MakeRange2();
                auto &map = _nodeMap[item.name];
                
                auto it2 = std::find_if(map.begin(), map.end(), [&range](const auto &pair) {
                    return StateRange::isIntersects(range, pair.first);
                });
                
                if (it2 != map.end()) {
                    auto &holder = it2->second;
                    
                    if (holder.read != nullptr && relations.find(holder.read) == relations.end()) {
                        // READ - WRITE
                        std::scoped_lock<std::mutex> _mapLock(_mapMutex);
                        boost::add_edge(holder.read, nodeId, _graph);
                        relations.insert(holder.read);
                    }
                    
                    if (holder.write != nullptr && relations.find(holder.write) == relations.end()) {
                        // WRITE - WRITE
                        std::scoped_lock<std::mutex> _mapLock(_mapMutex);
                        boost::add_edge(holder.write, nodeId, _graph);
                        relations.insert(holder.write);
                    }
                    
                    {
                        std::scoped_lock<std::mutex> _mapLock(_mapMutex);
                        defers.emplace_back(std::ref(holder), 1);
                    }
                } else {
                    std::scoped_lock<std::mutex> _mapLock(_mapMutex);
                    map.emplace(range, RWStateHolder { nullptr, nodeId });
                }
            });
            
            for (auto &pair : defers) {
                auto &holder = pair.first.get();
                auto mode = pair.second;
                
                if (mode == 0) {
                    holder.read = nodeId;
                } else if (mode == 1) {
                    holder.write = nodeId;
                }
            }
        }
    }
    
    /**
     * @copilot: this method dumps the graph to graphviz format
     */
    void RowGraph::dump() {
        std::scoped_lock<std::mutex> _lock(_mutex);
        
        /*
        std::ofstream ofs("graph.dot");
        boost::write_graphviz(ofs, _graph, [this](std::ostream &os, const auto &node) {
            std::shared_ptr<Transaction> txn = _graph[node]->transaction;
            
            if (_graph[node]->finalized && txn == nullptr) {
                os << fmt::format("[label=\"{}\\n(worker #{})\"]", node, _graph[node]->processedBy);
                return;
            }
            
            std::stringstream sstream;
            
            {
                auto it = txn->itemSet_begin();
                const auto itEnd = txn->itemSet_end();
                
                while (it != itEnd) {
                    const auto &item = *it;
                    std::string name = _resolver.resolveChain(item.name);
                    
                    // _logger->debug("name: {} -> {}", item.name, name);
                    
                    if (name.empty()) {
                        name = item.name;
                    }
                    
                    if (_keyColumns.find(name) == _keyColumns.end()) {
                        ++it;
                        continue;
                    }
                    
                    sstream << (*it).MakeRange2().MakeWhereQuery(name) << ",";
                    ++it;
                }
            }
            
            std::stringstream sstream2;
            
            {
                auto it = txn->whereSet_begin();
                const auto itEnd = txn->whereSet_end();
                
                while (it != itEnd) {
                    const auto &item = *it;
                    std::string name = std::move(_resolver.resolveChain(item.name));
                    
                    if (name.empty()) {
                        name = item.name;
                    }
                    
                    if (_keyColumns.find(name) == _keyColumns.end()) {
                        ++it;
                        continue;
                    }
                    
                    sstream2 << (*it).MakeRange2().MakeWhereQuery(name) << ",";
                    ++it;
                }
            }
            
            std::stringstream sstream4;
            
            {
                for (const auto &column: txn->writeSet()) {
                    sstream4 << column << ",";
                }
            }
            
            
            std::stringstream sstream3;
            
            {
                for (auto &query: txn->queries()) {
                    sstream3 << query->statement() << "\\n";
                }
            }
            
            / *
            os << fmt::format(
                "[label=\"{}\\nREAD [{}]\\nWRITE [{}]\\nWRITESET [{}]\\n{}\"]",
                txn->gid(),
                sstream2.str(),
                sstream.str(),
                sstream4.str(),
                sstream3.str()
            );
             * /
            
            os << fmt::format("[label=\"{}\"]", node);
            
            / *
            os << fmt::format(
                "[label=\"{}\\nREAD [{}]\\nWRITE [{}]\"]",
                txn->gid(),
                sstream2.str(),
                sstream.str()
            );
             * /
        });
         */
    }
}