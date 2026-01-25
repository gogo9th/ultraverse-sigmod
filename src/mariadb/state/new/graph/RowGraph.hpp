//
// Created by cheesekun on 7/10/23.
//

#ifndef ULTRAVERSE_ROWGRAPH_HPP
#define ULTRAVERSE_ROWGRAPH_HPP

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/graph/adjacency_list.hpp>

#include "../../StateItem.h"
#include "../Transaction.hpp"

#include "../RangeComparisonMethod.hpp"

#include "utils/log.hpp"


namespace ultraverse::state::v2 {
    
    class RelationshipResolver;
    
    struct RowGraphNode {
        uint64_t id;
        std::shared_ptr<Transaction> transaction;
        
        std::mutex mutex;
        
        std::atomic_bool ready = false;
        std::atomic_bool hold = false;
        std::atomic_int processedBy = -1;
        std::atomic_bool finalized = false;
        std::atomic_bool willBeRemoved = false;
        std::atomic_uint32_t pendingColumns = 0;
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
            gid_t readGid = 0;
            gid_t writeGid = 0;
            
            std::mutex mutex;
        };
        struct ColumnTask {
            RowGraphId nodeId;
            std::vector<StateItem> readItems;
            std::vector<StateItem> writeItems;
        };
        struct ColumnWorker {
            std::string column;
            std::unordered_map<StateRange, RWStateHolder> nodeMap;
            std::mutex mapMutex;
            RWStateHolder wildcardHolder;
            bool hasWildcard = false;

            std::mutex queueMutex;
            std::condition_variable queueCv;
            std::deque<ColumnTask> queue;
            std::atomic_bool running = true;
            std::thread worker;
        };
        struct CompositeRange {
            std::vector<StateRange> ranges;
            std::size_t hash = 0;

            bool isGlobalWildcard() const;
        };
        struct CompositeRangeHash {
            std::size_t operator()(const CompositeRange &range) const;
        };
        struct CompositeRangeEq {
            bool operator()(const CompositeRange &lhs, const CompositeRange &rhs) const;
        };
        struct CompositeTask {
            RowGraphId nodeId;
            std::vector<CompositeRange> readRanges;
            std::vector<CompositeRange> writeRanges;
        };
        struct CompositeWorker {
            std::vector<std::string> columns;
            std::unordered_map<CompositeRange, RWStateHolder, CompositeRangeHash, CompositeRangeEq> nodeMap;
            std::mutex mapMutex;
            RWStateHolder wildcardHolder;
            bool hasWildcard = false;

            std::mutex queueMutex;
            std::condition_variable queueCv;
            std::deque<CompositeTask> queue;
            std::atomic_bool running = true;
            std::thread worker;
        };
        explicit RowGraph(const std::set<std::string> &keyColumns,
                          const RelationshipResolver &resolver,
                          const std::vector<std::vector<std::string>> &keyColumnGroups = {});
        
        ~RowGraph();
        
        /**
         * @brief 트랜잭션을 그래프에 추가하고, 의존성을 해결한다.
         * @return 그래프의 노드 ID를 반환한다.
         * @note 그래프의 노드 ID는 트랜잭션 GID와 다르다!
         */
        RowGraphId addNode(std::shared_ptr<Transaction> transaction, bool hold = false);
        
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
         * @brief 수동으로 간선을 추가한다.
         */
        void addEdge(RowGraphId from, RowGraphId to);

        /**
         * @brief 수동 hold를 해제한다.
         */
        void releaseNode(RowGraphId nodeId);
        
        /**
         * @brief 가비지 콜렉팅을 실시한다. (처리된 노드들을 제거한다.)
         */
        void gc();
        
        /**
         * @brief 현재 그래프를 디버그용으로 출력한다.
         */
        void dump();
        
        RangeComparisonMethod rangeComparisonMethod() const;
        void setRangeComparisonMethod(RangeComparisonMethod rangeComparisonMethod);

// #ifdef ULTRAVERSE_TESTING
        size_t debugNodeMapSize(const std::string &column);
        size_t debugTotalNodeMapSize();
// #endif
        
    private:
        /**
         * @brief 의존성을 해결하여 노드와 노드간 간선 (edge)를 추가한다.
         */
        void enqueueTask(const std::string &column, ColumnTask task);
        void enqueueCompositeTask(size_t groupIndex, CompositeTask task);
        void columnWorkerLoop(ColumnWorker &worker);
        void processColumnTask(ColumnWorker &worker, ColumnTask &task);
        void compositeWorkerLoop(CompositeWorker &worker);
        void processCompositeTask(CompositeWorker &worker, CompositeTask &task);
        void markColumnTaskDone(RowGraphId nodeId);
        void pauseWorkers();
        void resumeWorkers();
        void notifyAllWorkers();
        void gcInternal();
        
        LoggerPtr _logger;
        const RelationshipResolver &_resolver;
        
        std::set<std::string> _keyColumns;
        std::vector<std::vector<std::string>> _keyColumnGroups;
        std::unordered_map<std::string, std::vector<size_t>> _compositeGroupsByTable;
        std::unordered_map<std::string, std::vector<std::string>> _keyColumnsByTable;
        std::vector<bool> _groupIsComposite;
        std::unordered_map<std::string, size_t> _groupIndexByColumn;
        std::unordered_set<std::string> _compositeColumns;
        
        RowGraphInternal _graph;
        
        /**
         * @brief (컬럼, Range)를 가장 마지막으로 읽고 쓴 노드 ID를 저장하는 맵
         * @details 노드간 간선을 빠르게 추가하기 위해 사용한다.
         */
        std::unordered_map<std::string, std::unique_ptr<ColumnWorker>> _columnWorkers;
        std::vector<std::unique_ptr<CompositeWorker>> _compositeWorkers;
        
        
        RWMutex _graphMutex;
        /** @deprecated `_graphMutex`를 대신 사용하십시오. */
        std::mutex _mutex;
        std::atomic_bool _isGCRunning = false;
        std::mutex _gcMutex;
        std::condition_variable _gcCv;
        std::atomic_bool _gcPause = false;
        std::atomic_uint32_t _activeTasks = 0;
        std::atomic_uint32_t _pausedWorkers = 0;
        uint32_t _workerCount = 0;
        
        RangeComparisonMethod _rangeComparisonMethod;
    };
}

#endif //ULTRAVERSE_ROWGRAPH_HPP
