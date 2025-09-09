//
// Created by cheesekun on 10/27/22.
//

#include <cereal/types/memory.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/map.hpp>

#include "ColumnDependencyGraph.hpp"

namespace ultraverse::state::v2 {
    template <typename Archive>
    void ColumnDependencyNode::serialize(Archive &archive) {
        archive(
            accessType,
            columnSet,
            hash
        );
    }
    
    template <typename Archive>
    void ColumnDependencyGraph::save(Archive &archive) const {
        std::map<
            int64_t,
            std::pair<std::shared_ptr<ColumnDependencyNode>, std::shared_ptr<std::vector<int64_t>>>
        > serializedGraph;
        
        for (const auto &pair: _nodeMap) {
            auto nodeIdx = pair.second;
            auto node = _graph[nodeIdx];
            
            std::pair<std::shared_ptr<ColumnDependencyNode>, std::shared_ptr<std::vector<int64_t>>> serializedNode {
                _graph[nodeIdx],
                std::make_shared<std::vector<int64_t>>()
            };
            
            boost::graph_traits<Graph>::adjacency_iterator ai, aiEnd, next;
            boost::tie(ai, aiEnd) = boost::adjacent_vertices(nodeIdx, _graph);
            
            for (next = ai; ai != aiEnd; ai = next) {
                next++;
                
                serializedNode.second->push_back((int64_t) *ai);
            }
            
            serializedGraph[nodeIdx] = serializedNode;
        }
        
        archive(serializedGraph);
    }
    
    template <typename Archive>
    void ColumnDependencyGraph::load(Archive &archive) {
        std::map<
            int64_t,
            std::pair<std::shared_ptr<ColumnDependencyNode>, std::shared_ptr<std::vector<int64_t>>>
        > serializedGraph;
        
        archive(serializedGraph);
        
        // step 1: add vertices
        for (auto &pair: serializedGraph) {
            auto nodeIdx = pair.first;
            auto node = pair.second.first;
            
            auto newIdx = add_vertex(node, _graph);
            
            assert(nodeIdx == newIdx);
            _nodeMap.insert({ node->hash, nodeIdx });
        }
        
        // step 2: reconstruct edges
        for (auto &pair: serializedGraph) {
            auto nodeIdx = pair.first;
            auto vertices = pair.second.second;
            
            for (auto &vertex: *vertices) {
                // Graph의 OutEdgeList를 boost::setS로 설정하였으므로 A -> B와 B -> A를 실행하여도 duplicate되진 않을것임
                add_edge(nodeIdx, vertex, _graph);
            }
        }
    }
}
