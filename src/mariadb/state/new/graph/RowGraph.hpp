//
// Created by cheesekun on 7/10/23.
//

#ifndef ULTRAVERSE_ROWGRAPH_HPP
#define ULTRAVERSE_ROWGRAPH_HPP

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <mutex>

#include <boost/graph/adjacency_list.hpp>

#include "../../StateItem.h"
#include "../Transaction.hpp"

#include "utils/log.hpp"

namespace ultraverse::state::v2 {
    
    class RelationshipResolver;
    
    struct RowGraphNode {
        uint64_t id;
        std::shared_ptr<Transaction> transaction;
        
        std::atomic_bool ready = false;
        std::atomic_int processedBy = -1;
        std::atomic_bool finalized = false;
    };
    
    using RowGraphInternal =
        boost::adjacency_list<boost::listS, boost::listS, boost::bidirectionalS, std::shared_ptr<RowGraphNode>>;
    
    using RowGraphId =
        boost::graph_traits<RowGraphInternal>::vertex_descriptor;
    
    class RowGraph {
    public:
        struct RWStateHolder {
            RowGraphId read  = RowGraphId(-1);
            RowGraphId write = RowGraphId(-1);
        };
        explicit RowGraph(const std::set<std::string> &keyColumns, const RelationshipResolver &resolver);
        
        RowGraphId addNode(std::shared_ptr<Transaction> transaction);
        
        std::unordered_set<RowGraphId> dependenciesOf(RowGraphId nodeId);
        std::unordered_set<RowGraphId> dependentsOf(RowGraphId nodeId);
        
        std::unordered_set<RowGraphId> entrypoints();
        
        bool isFinalized() const;
       
        /**
         * finds for 'entrypoint node', and marks it as processed by workerId
         * @returns node id for the 'entrypoint', or UINT64_MAX if not found
         */
        RowGraphId entrypoint(int workerId);
        
        std::shared_ptr<RowGraphNode> nodeFor(RowGraphId nodeId);
        
        /**
         * call me on every 1000 transactions
         */
        void gc();
        
        void dump();
        
    private:
        void buildEdge(RowGraphId nodeId);
        
        LoggerPtr _logger;
        const RelationshipResolver &_resolver;
        
        std::set<std::string> _keyColumns;
        
        RowGraphInternal _graph;
        std::map<
            std::string,
            std::unordered_map<StateRange, RWStateHolder>
        > _nodeMap;
        
        
        std::mutex _mutex;
    };
}

#endif //ULTRAVERSE_ROWGRAPH_HPP
