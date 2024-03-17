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
#include <shared_mutex>

#include <boost/graph/adjacency_list.hpp>

#include "../../StateItem.h"
#include "../Transaction.hpp"

#include "../RangeComparisonMethod.hpp"

#include "utils/log.hpp"
#include "base/TaskExecutor.hpp"


namespace ultraverse::state::v2 {
    
    class RelationshipResolver;
    
    struct RowGraphNode {
        uint64_t id;
        std::shared_ptr<Transaction> transaction;
        
        std::mutex mutex;
        
        std::atomic_bool ready = false;
        std::atomic_int processedBy = -1;
        std::atomic_bool finalized = false;
        std::atomic_bool willBeRemoved = false;
    };
    
    using RowGraphInternal =
        boost::adjacency_list<boost::listS, boost::listS, boost::bidirectionalS, std::shared_ptr<RowGraphNode>>;
    
    using RowGraphId =
        boost::graph_traits<RowGraphInternal>::vertex_descriptor;
    
    using RWMutex =
        std::shared_mutex;
    
    using ConcurrentReadLock =
        std::shared_lock<RWMutex>;
    
    using WriteLock =
        std::unique_lock<RWMutex>;
    
    /**
     * @brief 트랜잭션 동시 실행 가능 여부 판정을 위한 Rowid-level 그래프
     */
    class RowGraph {
    public:
        struct RWStateHolder {
            RowGraphId read  = nullptr;
            RowGraphId write = nullptr;
            
            std::mutex mutex;
        };
        explicit RowGraph(const std::set<std::string> &keyColumns, const RelationshipResolver &resolver);
        
        ~RowGraph();
        
        /**
         * @brief 트랜잭션을 그래프에 추가하고, 의존성을 해결한다.
         * @return 그래프의 노드 ID를 반환한다.
         * @note 그래프의 노드 ID는 트랜잭션 GID와 다르다!
         */
        RowGraphId addNode(std::shared_ptr<Transaction> transaction);
        
        /**
         * @brief 사용되지 않음
         */
        std::unordered_set<RowGraphId> dependenciesOf(RowGraphId nodeId);
        /**
         * @brief 사용되지 않음
         */
        std::unordered_set<RowGraphId> dependentsOf(RowGraphId nodeId);
        
        /**
         * @brief 사용되지 않음
         */
        std::unordered_set<RowGraphId> entrypoints();
        
        /**
         * @brief 모든 그래프 노드가 처리되었는지 여부를 반환한다.
         */
        bool isFinalized();
       
        /**
         * @brief workerId가 처리할 수 있는 노드를 찾는다.
         * @return 노드 ID를 반환한다. 단, 당장 처리할 수 있는 노드가 없으면 nullptr를 반환한다.
         */
        RowGraphId entrypoint(int workerId);
        
        /**
         * @brief 노드 ID로 노드에 액세스한다.
         */
        std::shared_ptr<RowGraphNode> nodeFor(RowGraphId nodeId);
        
        /**
         * @brief 가비지 콜렉팅을 실시한다. (처리된 노드들을 제거한다.)
         */
        void gc();
        
        /**
         * @brief 현재 그래프를 graphviz dot 형식으로 출력한다.
         */
        void dump();
        
        RangeComparisonMethod rangeComparisonMethod() const;
        void setRangeComparisonMethod(RangeComparisonMethod rangeComparisonMethod);
        
    private:
        /**
         * @brief 의존성을 해결하여 노드와 노드간 간선 (edge)를 추가한다.
         */
        void buildEdge(RowGraphId nodeId);
        
        LoggerPtr _logger;
        const RelationshipResolver &_resolver;
        
        std::set<std::string> _keyColumns;
        
        RowGraphInternal _graph;
        
        /**
         * @brief (컬럼, Range)를 가장 마지막으로 읽고 쓴 노드 ID를 저장하는 맵
         * @details 노드간 간선을 빠르게 추가하기 위해 사용한다.
         */
        std::map<
            std::string,
            std::unordered_map<StateRange, RWStateHolder>
        > _nodeMap;
        std::shared_mutex _nodeMapMutex;
        
        
        RWMutex _graphMutex;
        /** @deprecated `_graphMutex`를 대신 사용하십시오. */
        std::mutex _mutex;
        std::atomic_bool _isGCRunning = false;
        std::atomic_uint64_t _workerCount = 0;
        
        TaskExecutor _taskExecutor;
        
        RangeComparisonMethod _rangeComparisonMethod;
    };
}

#endif //ULTRAVERSE_ROWGRAPH_HPP
