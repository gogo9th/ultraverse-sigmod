//
// Created by cheesekun on 8/29/22.
//

#ifndef ULTRAVERSE_STATECHANGER_HPP
#define ULTRAVERSE_STATECHANGER_HPP

#include <functional>
#include <optional>
#include <string>

#include "Transaction.hpp"
#include "StateIO.hpp"
#include "StateChangeContext.hpp"
#include "StateChangePlan.hpp"
#include "ColumnDependencyGraph.hpp"
#include "HashWatcher.hpp"
#include "ProcLogReader.hpp"
#include "ProcMatcher.hpp"
#include "TableDependencyGraph.hpp"

#include "cluster/CandidateColumn.hpp"
#include "cluster/RowCluster.hpp"

#include "mariadb/DBHandle.hpp"
#include "mariadb/DBHandlePoolAdapter.hpp"
#include "utils/log.hpp"
#include "mariadb/state/new/graph/RowGraph.hpp"


namespace ultraverse::state::v2 {
    class StateCluster;
    class CachedRelationshipResolver;
    class StateRelationshipResolver;
    struct StateChangeReplayPlan;

    namespace OperationMode {
        enum Value {
            NORMAL,
            PREPARE,
            FULL_REPLAY
        };
    }
    
    class StateChanger {
    public:
        static const std::string QUERY_TAG_STATECHANGE;
        
        StateChanger(mariadb::DBHandlePoolBase &dbHandlePool, const StateChangePlan &plan);
        StateChanger(mariadb::DBHandlePoolBase &dbHandlePool, const StateChangePlan &plan, StateChangerIO io);
        
        void makeCluster();
        
        void prepare();
        
        /**
         * @brief 벤치마크용: auto-rollback 액션.
         */
        void bench_prepareRollback();
        
        void replay();
        
        void fullReplay();
        
    private:
        constexpr static int CLUSTER_EXPAND_FLAG_NO_FLAGS    = 0;
        constexpr static int CLUSTER_EXPAND_FLAG_STRICT      = 0b01;
        constexpr static int CLUSTER_EXPAND_FLAG_INCLUDE_FK  = 0b10;
        constexpr static int CLUSTER_EXPAND_FLAG_WILDCARD    = 0b100;
        constexpr static int CLUSTER_EXPAND_FLAG_DONT_EXPAND = 0b1000;

        struct ReplayAnalysisResult {
            std::vector<gid_t> replayGids;
            size_t totalCount = 0;
            size_t totalQueryCount = 0;
            size_t replayQueryCount = 0;
        };

        ReplayAnalysisResult analyzeReplayPlan(
            StateCluster &rowCluster,
            StateRelationshipResolver &relationshipResolver,
            CachedRelationshipResolver &cachedResolver,
            StateChangeReplayPlan *replayPlan,
            const std::function<bool(gid_t, size_t)> &isRollbackTarget,
            const std::function<std::optional<std::string>(gid_t)> &userQueryPath,
            const std::function<bool(gid_t)> &shouldRevalidateTarget);
        
        void replayThreadMain(int workerId,
                              RowGraph &rowGraph,
                              std::atomic_bool &running,
                              std::atomic_uint64_t &replayedTxns);
        
        std::shared_ptr<Transaction> loadUserQuery(const std::string &path);
        std::shared_ptr<Transaction> parseUserQuery(const std::string &sql);
        
        void loadBackup(const std::string &dbName, const std::string &fileName);
        
        /**
         * creates intermediate database.
         */
        void createIntermediateDB();
        
        /**
         * drops intermediate database.
         * called when task has failed?
         */
        void dropIntermediateDB();
        
        /**
         * updates primary keys
         */
        void updatePrimaryKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp, std::string schemaName = "");
        
        /**
         * updates foreign keys
         */
        void updateForeignKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp, std::string schemaName = "");
        
        int64_t getAutoIncrement(mariadb::DBHandle &dbHandle, std::string table);
        void setAutoIncrement(mariadb::DBHandle &dbHandle, std::string table, int64_t value);

        void applyStatementContext(mariadb::DBHandle &dbHandle, const Query &query);
        
        LoggerPtr _logger;
        
        mariadb::DBHandlePoolBase &_dbHandlePool;
        
        StateChangePlan _plan;
        OperationMode::Value _mode;
        
        std::string _intermediateDBName;
        
        std::unique_ptr<IStateLogReader> _reader;
        std::unique_ptr<IStateClusterStore> _clusterStore;
        std::unique_ptr<IBackupLoader> _backupLoader;
        bool _closeStandardFds;
        
        std::shared_ptr<StateChangeContext> _context;
        
        std::atomic_bool _isRunning;
        std::vector<std::thread> _executorThreads;
        
        
        /** REMOVE ME */
        std::unordered_map<std::string, state::StateHash> _stateHashMap;
        
        std::unique_ptr<ColumnDependencyGraph> _columnGraph;
        std::unique_ptr<TableDependencyGraph> _tableGraph;
        
        std::unique_ptr<HashWatcher> _hashWatcher;
        std::unique_ptr<ProcLogReader> _procLogReader;
        
        std::mutex _changedTablesMutex;
        std::unordered_set<std::string> _changedTables;
        
        /** REMOVE ME */
        std::atomic_uint64_t _replayedQueries;
        std::atomic_uint64_t _replayedTxns;
        
        double _phase1Time;
        double _phase2Time;
    };
}


#endif //ULTRAVERSE_STATECHANGER_HPP
