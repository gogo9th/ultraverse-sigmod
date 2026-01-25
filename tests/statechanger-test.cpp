#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "state_test_helpers.hpp"

#include "mariadb/DBHandle.hpp"
#include "mariadb/state/new/StateChanger.hpp"
#include "mariadb/state/new/StateChangePlan.hpp"
#include "mariadb/state/new/StateChangeContext.hpp"
#include "mariadb/state/new/StateChangeReplayPlan.hpp"
#include "mariadb/state/new/StateIO.hpp"
#include "mariadb/state/new/cluster/StateCluster.hpp"
#include "mariadb/state/new/cluster/StateRelationshipResolver.hpp"
#include "utils/StringUtil.hpp"

#include <nlohmann/json.hpp>

namespace {
    using ultraverse::mariadb::MockedDBHandle;
    using ultraverse::state::v2::StateChanger;
    using ultraverse::state::v2::StateChangerIO;
    using ultraverse::state::v2::StateChangePlan;
    using ultraverse::state::v2::StateChangeReplayPlan;
    using ultraverse::state::v2::StateCluster;
    using ultraverse::state::v2::StateRelationshipResolver;
    using ultraverse::state::v2::CachedRelationshipResolver;
    using ultraverse::state::v2::MockedStateLogReader;
    using ultraverse::state::v2::MockedStateClusterStore;
    using ultraverse::state::v2::IBackupLoader;
    using ultraverse::state::v2::test_helpers::MockedDBHandlePool;
    using ultraverse::state::v2::Transaction;
    using ultraverse::state::v2::Query;
    using ultraverse::state::v2::gid_t;

    constexpr int kReplayTotalTransactions = 5000;
    constexpr int kReplayChains = 5;

    class NoopBackupLoader final: public IBackupLoader {
    public:
        void loadBackup(const std::string &dbName, const std::string &fileName) override {
            (void) dbName;
            (void) fileName;
        }
    };

    std::string makeTempDir(const std::string &prefix);

    StateChangePlan makePlan(int threadNum) {
        StateChangePlan plan;
        plan.setDBName("testdb");
        plan.setThreadNum(threadNum);
        plan.setDropIntermediateDB(false);
        plan.setFullReplay(false);
        plan.setDryRun(false);
        plan.setStateLogPath(makeTempDir("statechanger_plan"));
        plan.setStateLogName("plan");
        plan.keyColumns().insert("items.id");
        return plan;
    }

    std::shared_ptr<Query> makeQuery(const std::string &dbName,
                                     const std::string &statement,
                                     const std::vector<StateItem> &reads,
                                     const std::vector<StateItem> &writes) {
        auto query = std::make_shared<Query>();
        query->setDatabase(dbName);
        query->setStatement(statement);

        auto &readSet = query->readSet();
        readSet = reads;

        auto &writeSet = query->writeSet();
        writeSet = writes;

        for (const auto &item : readSet) {
            if (!item.name.empty()) {
                query->readColumns().insert(ultraverse::utility::toLower(item.name));
            }
        }

        for (const auto &item : writeSet) {
            if (!item.name.empty()) {
                query->writeColumns().insert(ultraverse::utility::toLower(item.name));
            }
        }

        return query;
    }

    std::shared_ptr<Transaction> makeTransaction(gid_t gid,
                                                  const std::string &dbName,
                                                  const std::string &statement,
                                                  const std::vector<StateItem> &reads,
                                                  const std::vector<StateItem> &writes) {
        auto txn = std::make_shared<Transaction>();
        txn->setGid(gid);
        txn->setTimestamp(0);
        txn->setXid(0);
        txn->setFlags(0);

        auto query = makeQuery(dbName, statement, reads, writes);
        *txn << query;

        return txn;
    }

    std::optional<gid_t> parseGidToken(const std::string &query) {
        auto pos = query.find("/*TXN:");
        if (pos == std::string::npos) {
            return std::nullopt;
        }
        pos += 6;
        auto end = query.find("*/", pos);
        if (end == std::string::npos) {
            return std::nullopt;
        }
        auto token = query.substr(pos, end - pos);
        return static_cast<gid_t>(std::stoull(token));
    }

    std::vector<gid_t> extractExecutedGids(const std::vector<std::string> &queries) {
        std::vector<gid_t> gids;
        gids.reserve(queries.size());

        for (const auto &query : queries) {
            auto token = parseGidToken(query);
            if (!token.has_value()) {
                continue;
            }
            gids.push_back(*token);
        }

        return gids;
    }

    std::unordered_map<gid_t, size_t> buildPositionIndex(const std::vector<gid_t> &executionOrder) {
        std::unordered_map<gid_t, size_t> position;
        position.reserve(executionOrder.size());

        for (size_t i = 0; i < executionOrder.size(); i++) {
            position[executionOrder[i]] = i;
        }

        return position;
    }

    std::string makeTempDir(const std::string &prefix) {
        static std::atomic<uint64_t> counter{0};
        auto suffix = std::to_string(counter.fetch_add(1));
        auto dir = std::filesystem::temp_directory_path() / (prefix + "_" + suffix);
        std::filesystem::create_directories(dir);
        return dir.string();
    }

    void seedEmptyInfoSchemaResults(const std::shared_ptr<MockedDBHandle::SharedState> &sharedState,
                                    size_t count = 2) {
        std::scoped_lock lock(sharedState->mutex);
        for (size_t i = 0; i < count; i++) {
            sharedState->results.push({});
        }
    }

    nlohmann::json readJsonReport(const std::string &path) {
        std::ifstream stream(path);
        REQUIRE(stream.is_open());
        return nlohmann::json::parse(stream);
    }

    std::vector<gid_t> loadReplayPlanGids(const StateChangePlan &plan) {
        const auto path = plan.stateLogPath() + "/" + plan.stateLogName() + ".ultreplayplan";
        return StateChangeReplayPlan::load(path).gids;
    }

    void writeReplayPlan(const std::string &dir,
                         const std::string &name,
                         const std::vector<gid_t> &gids,
                         const std::map<gid_t, Transaction> &userQueries = {},
                         const std::vector<gid_t> &rollbackGids = {}) {
        StateChangeReplayPlan plan;
        plan.gids = gids;
        plan.userQueries = userQueries;
        plan.rollbackGids = rollbackGids;
        plan.save(dir + "/" + name + ".ultreplayplan");
    }

    class DelayedDBHandle final : public ultraverse::mariadb::DBHandle {
    public:
        DelayedDBHandle(std::shared_ptr<MockedDBHandle::SharedState> state,
                        std::unordered_map<gid_t, std::chrono::milliseconds> delays)
            : _inner(std::make_shared<MockedDBHandle>(std::move(state))),
              _delays(std::move(delays)) {
        }

        void connect(const std::string &host, int port, const std::string &user, const std::string &password) override {
            _inner->connect(host, port, user, password);
        }

        void disconnect() override {
            _inner->disconnect();
        }

        int executeQuery(const std::string &query) override {
            auto gid = parseGidToken(query);
            if (gid.has_value()) {
                auto it = _delays.find(*gid);
                if (it != _delays.end()) {
                    std::this_thread::sleep_for(it->second);
                }
            }
            return _inner->executeQuery(query);
        }

        const char *lastError() const override {
            return _inner->lastError();
        }

        int lastErrno() const override {
            return _inner->lastErrno();
        }

        std::unique_ptr<ultraverse::mariadb::DBResult> storeResult() override {
            return _inner->storeResult();
        }

        int nextResult() override {
            return _inner->nextResult();
        }

        void setAutocommit(bool enabled) override {
            _inner->setAutocommit(enabled);
        }

        std::shared_ptr<MYSQL> handle() override {
            return _inner->handle();
        }

    private:
        std::shared_ptr<MockedDBHandle> _inner;
        std::unordered_map<gid_t, std::chrono::milliseconds> _delays;
    };

    class DelayedDBHandleLease final : public ultraverse::mariadb::DBHandleLeaseBase {
    public:
        DelayedDBHandleLease(
            std::shared_ptr<DelayedDBHandle> handle,
            std::function<void()> releaser
        ):
            _handle(std::move(handle)),
            _releaser(std::move(releaser))
        {
        }

        ~DelayedDBHandleLease() override {
            if (_releaser) {
                _releaser();
            }
        }

        ultraverse::mariadb::DBHandle &get() override {
            return *_handle;
        }

    private:
        std::shared_ptr<DelayedDBHandle> _handle;
        std::function<void()> _releaser;
    };

    class DelayedDBHandlePool final : public ultraverse::mariadb::DBHandlePoolBase {
    public:
        DelayedDBHandlePool(
            int poolSize,
            std::shared_ptr<MockedDBHandle::SharedState> sharedState,
            std::unordered_map<gid_t, std::chrono::milliseconds> delays
        ):
            _poolSize(poolSize),
            _sharedState(std::move(sharedState)),
            _delays(std::move(delays))
        {
            for (int i = 0; i < poolSize; i++) {
                _handles.push(std::make_shared<DelayedDBHandle>(_sharedState, _delays));
            }
        }

        std::unique_ptr<ultraverse::mariadb::DBHandleLeaseBase> take() override {
            std::unique_lock lock(_mutex);
            _condvar.wait(lock, [this]() { return !_handles.empty(); });

            auto handle = _handles.front();
            _handles.pop();
            lock.unlock();

            return std::make_unique<DelayedDBHandleLease>(handle, [this, handle]() {
                std::scoped_lock lock(_mutex);
                _handles.push(handle);
                _condvar.notify_one();
            });
        }

        int poolSize() const override {
            return _poolSize;
        }

    private:
        int _poolSize;
        std::shared_ptr<MockedDBHandle::SharedState> _sharedState;
        std::unordered_map<gid_t, std::chrono::milliseconds> _delays;
        std::mutex _mutex;
        std::condition_variable _condvar;
        std::queue<std::shared_ptr<DelayedDBHandle>> _handles;
    };
}

TEST_CASE("StateChanger prepare outputs dependent GIDs only", "[statechanger][prepare]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    auto plan = makePlan(1);
    plan.rollbackGids().push_back(1);

    StateItem key1 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(1)));
    StateItem key2 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(2)));

    auto txn1 = makeTransaction(1, plan.dbName(), "/*TXN:1*/", {}, {key1});
    auto txn2 = makeTransaction(2, plan.dbName(), "/*TXN:2*/", {key1}, {});
    auto txn3 = makeTransaction(3, plan.dbName(), "/*TXN:3*/", {key2}, {});

    StateCluster cluster(plan.keyColumns());
    ultraverse::state::v2::StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);
    CachedRelationshipResolver cachedResolver(resolver, 1000);

    cluster.insert(txn1, cachedResolver);
    cluster.insert(txn2, cachedResolver);
    cluster.insert(txn3, cachedResolver);
    cluster.merge();

    auto clusterStore = std::make_unique<MockedStateClusterStore>();
    clusterStore->save(cluster);

    auto logReader = std::make_unique<MockedStateLogReader>();
    logReader->addTransaction(txn1, 1);
    logReader->addTransaction(txn2, 2);
    logReader->addTransaction(txn3, 3);

    MockedDBHandlePool pool(1, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::move(clusterStore);
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));

    changer.prepare();

    auto gids = loadReplayPlanGids(plan);
    REQUIRE(gids.size() == 1);
    REQUIRE(gids[0] == 2);
}

TEST_CASE("StateChanger prepare handles multiple rollback targets and partial-key filtering", "[statechanger][prepare]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    auto plan = makePlan(1);
    plan.keyColumns().insert("items.type");
    plan.keyColumns().insert("orders.id");
    plan.setKeyColumnGroups({{"items.id", "items.type"}, {"orders.id"}});

    StateCluster cluster(plan.keyColumns());
    ultraverse::state::v2::StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);
    CachedRelationshipResolver cachedResolver(resolver, 1000);

    auto logReader = std::make_unique<MockedStateLogReader>();

    std::vector<ultraverse::state::v2::gid_t> expected;
    ultraverse::state::v2::gid_t gid = 1;

    auto addTxn = [&](const std::vector<StateItem> &reads, const std::vector<StateItem> &writes) {
        std::string statement = "/*TXN:" + std::to_string(gid) + "*/";
        auto txn = makeTransaction(gid, plan.dbName(), statement, reads, writes);
        cluster.insert(txn, cachedResolver);
        logReader->addTransaction(txn, gid);
        return gid++;
    };

    StateItem itemId1 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(1)));
    StateItem itemId2 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(2)));
    StateItem itemTypeA = StateItem::EQ("items.type", StateData(std::string("A")));
    StateItem itemTypeB = StateItem::EQ("items.type", StateData(std::string("B")));
    StateItem orderId100 = StateItem::EQ("orders.id", StateData(static_cast<int64_t>(100)));
    StateItem orderId101 = StateItem::EQ("orders.id", StateData(static_cast<int64_t>(101)));

    addTxn({StateItem::EQ("items.id", StateData(static_cast<int64_t>(9))),
            StateItem::EQ("items.type", StateData(std::string("Z")))}, {});

    ultraverse::state::v2::gid_t rollbackItems = addTxn({}, {itemId1, itemTypeA});
    plan.rollbackGids().push_back(rollbackItems);

    for (int i = 0; i < 20; i++) {
        ultraverse::state::v2::gid_t replayGid = addTxn({itemId1, itemTypeA}, {});
        expected.push_back(replayGid);
    }

    for (int i = 0; i < 10; i++) {
        addTxn({itemId1}, {});
    }

    for (int i = 0; i < 10; i++) {
        addTxn({itemTypeA}, {});
    }

    for (int i = 0; i < 10; i++) {
        addTxn({itemId2, itemTypeA}, {});
    }

    for (int i = 0; i < 10; i++) {
        addTxn({itemId1, itemTypeB}, {});
    }

    ultraverse::state::v2::gid_t rollbackOrders = addTxn({}, {orderId100});
    plan.rollbackGids().push_back(rollbackOrders);

    for (int i = 0; i < 20; i++) {
        ultraverse::state::v2::gid_t replayGid = addTxn({orderId100}, {});
        expected.push_back(replayGid);
    }

    for (int i = 0; i < 10; i++) {
        addTxn({orderId101}, {});
    }

    for (int i = 0; i < 10; i++) {
        addTxn({orderId100, itemId1}, {});
    }

    cluster.merge();

    auto clusterStore = std::make_unique<MockedStateClusterStore>();
    clusterStore->save(cluster);

    MockedDBHandlePool pool(1, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::move(clusterStore);
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));

    changer.prepare();

    auto gids = loadReplayPlanGids(plan);
    std::sort(gids.begin(), gids.end());
    std::sort(expected.begin(), expected.end());

    REQUIRE(!expected.empty());
    REQUIRE(!gids.empty());

    REQUIRE(gids == expected);
}

TEST_CASE("StateChanger prepare includes column-wise dependent queries without key columns", "[statechanger][prepare][columnwise]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    auto plan = makePlan(1);
    plan.rollbackGids().push_back(1);

    StateCluster cluster(plan.keyColumns());
    ultraverse::state::v2::StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);
    CachedRelationshipResolver cachedResolver(resolver, 1000);

    auto txn1 = makeTransaction(1, plan.dbName(), "/*TXN:1*/",
                                {},
                                {StateItem::EQ("items.name", StateData(std::string("A")))});
    auto txn2 = makeTransaction(2, plan.dbName(), "/*TXN:2*/",
                                {StateItem::EQ("items.name", StateData(std::string("A")))},
                                {});
    auto txn3 = makeTransaction(3, plan.dbName(), "/*TXN:3*/",
                                {StateItem::EQ("items.id", StateData(static_cast<int64_t>(99)))},
                                {});

    cluster.insert(txn1, cachedResolver);
    cluster.insert(txn2, cachedResolver);
    cluster.insert(txn3, cachedResolver);
    cluster.merge();

    auto clusterStore = std::make_unique<MockedStateClusterStore>();
    clusterStore->save(cluster);

    auto logReader = std::make_unique<MockedStateLogReader>();
    logReader->addTransaction(txn1, 1);
    logReader->addTransaction(txn2, 2);
    logReader->addTransaction(txn3, 3);

    MockedDBHandlePool pool(1, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::move(clusterStore);
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));

    changer.prepare();

    auto gids = loadReplayPlanGids(plan);
    REQUIRE(gids.size() == 1);
    REQUIRE(gids[0] == 2);
}

/**
 * Ensures column-wise dependency propagation is transitive: if a dependent
 * transaction writes a new column, later reads of that column are replayed.
 */
TEST_CASE("StateChanger prepare propagates column taint transitively", "[statechanger][prepare][columnwise]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    auto plan = makePlan(1);
    plan.rollbackGids().push_back(1);

    StateItem writeA = StateItem::EQ("items.color", StateData(std::string("red")));
    StateItem readA = StateItem::EQ("items.color", StateData(std::string("red")));
    StateItem writeB = StateItem::EQ("items.size", StateData(std::string("L")));
    StateItem readB = StateItem::EQ("items.size", StateData(std::string("L")));

    auto txn1 = makeTransaction(1, plan.dbName(), "/*TXN:1*/", {}, {writeA});
    auto txn2 = makeTransaction(2, plan.dbName(), "/*TXN:2*/", {readA}, {writeB});
    auto txn3 = makeTransaction(3, plan.dbName(), "/*TXN:3*/", {readB}, {});

    StateCluster cluster(plan.keyColumns());
    ultraverse::state::v2::StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);
    CachedRelationshipResolver cachedResolver(resolver, 1000);

    cluster.insert(txn1, cachedResolver);
    cluster.insert(txn2, cachedResolver);
    cluster.insert(txn3, cachedResolver);
    cluster.merge();

    auto clusterStore = std::make_unique<MockedStateClusterStore>();
    clusterStore->save(cluster);

    auto logReader = std::make_unique<MockedStateLogReader>();
    logReader->addTransaction(txn1, 1);
    logReader->addTransaction(txn2, 2);
    logReader->addTransaction(txn3, 3);

    MockedDBHandlePool pool(1, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::move(clusterStore);
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));

    changer.prepare();

    auto gids = loadReplayPlanGids(plan);
    std::sort(gids.begin(), gids.end());

    REQUIRE(gids.size() == 2);
    REQUIRE(gids[0] == 2);
    REQUIRE(gids[1] == 3);
}

TEST_CASE("StateChanger auto-rollback selects rollback targets by ratio", "[statechanger][auto-rollback]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    auto plan = makePlan(1);
    plan.setAutoRollbackRatio(0.5);
    plan.setReportPath(plan.stateLogPath() + "/auto_report.json");

    StateCluster cluster(plan.keyColumns());
    ultraverse::state::v2::StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);
    CachedRelationshipResolver cachedResolver(resolver, 1000);

    auto logReader = std::make_unique<MockedStateLogReader>();

    StateItem key = StateItem::EQ("items.id", StateData(static_cast<int64_t>(1)));
    for (ultraverse::state::v2::gid_t gid = 1; gid <= 5; gid++) {
        std::string statement = "/*TXN:" + std::to_string(gid) + "*/";
        auto txn = makeTransaction(gid, plan.dbName(), statement, {}, {key});
        cluster.insert(txn, cachedResolver);
        logReader->addTransaction(txn, gid);
    }

    cluster.merge();

    auto clusterStore = std::make_unique<MockedStateClusterStore>();
    clusterStore->save(cluster);

    MockedDBHandlePool pool(1, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::move(clusterStore);
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));

    changer.bench_prepareRollback();

    auto report = readJsonReport(plan.reportPath());
    auto rollbackGids = report.at("rollbackGids").get<std::vector<ultraverse::state::v2::gid_t>>();
    std::sort(rollbackGids.begin(), rollbackGids.end());

    std::vector<ultraverse::state::v2::gid_t> expectedRollback{1, 3, 5};
    REQUIRE(rollbackGids == expectedRollback);

    REQUIRE(report.at("totalCount").get<size_t>() == 5);
    REQUIRE(report.at("replayGidCount").get<size_t>() == 2);
    REQUIRE(report.at("totalQueryCount").get<size_t>() == 5);
    REQUIRE(report.at("replayQueryCount").get<size_t>() == 2);
}

TEST_CASE("StateChanger auto-rollback respects gid-range and skip-gids", "[statechanger][auto-rollback]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    auto plan = makePlan(1);
    plan.setAutoRollbackRatio(0.5);
    plan.setStartGid(2);
    plan.setEndGid(6);
    plan.skipGids().push_back(4);
    plan.setReportPath(plan.stateLogPath() + "/auto_report_range.json");

    StateCluster cluster(plan.keyColumns());
    ultraverse::state::v2::StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);
    CachedRelationshipResolver cachedResolver(resolver, 1000);

    auto logReader = std::make_unique<MockedStateLogReader>();

    StateItem key = StateItem::EQ("items.id", StateData(static_cast<int64_t>(1)));
    for (ultraverse::state::v2::gid_t gid = 1; gid <= 6; gid++) {
        std::string statement = "/*TXN:" + std::to_string(gid) + "*/";
        auto txn = makeTransaction(gid, plan.dbName(), statement, {}, {key});
        cluster.insert(txn, cachedResolver);
        logReader->addTransaction(txn, gid);
    }

    cluster.merge();

    auto clusterStore = std::make_unique<MockedStateClusterStore>();
    clusterStore->save(cluster);

    MockedDBHandlePool pool(1, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::move(clusterStore);
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));

    changer.bench_prepareRollback();

    auto report = readJsonReport(plan.reportPath());
    auto rollbackGids = report.at("rollbackGids").get<std::vector<ultraverse::state::v2::gid_t>>();
    std::sort(rollbackGids.begin(), rollbackGids.end());

    std::vector<ultraverse::state::v2::gid_t> expectedRollback{3, 6};
    REQUIRE(rollbackGids == expectedRollback);

    REQUIRE(report.at("totalCount").get<size_t>() == 4);
}

TEST_CASE("StateChanger prepare refreshes target cache after skipped rollback revalidation", "[statechanger][prepare]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    auto plan = makePlan(1);
    plan.rollbackGids().push_back(1);
    plan.rollbackGids().push_back(2);
    plan.skipGids().push_back(2);

    StateItem key = StateItem::EQ("items.id", StateData(static_cast<int64_t>(1)));
    auto txn1 = makeTransaction(1, plan.dbName(), "/*TXN:1*/", {}, {key});
    auto txn2 = makeTransaction(2, plan.dbName(), "/*TXN:2*/", {}, {key});
    auto txn3 = makeTransaction(3, plan.dbName(), "/*TXN:3*/", {key}, {});

    StateCluster cluster(plan.keyColumns());
    ultraverse::state::v2::StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);
    CachedRelationshipResolver cachedResolver(resolver, 1000);

    cluster.insert(txn1, cachedResolver);
    cluster.insert(txn2, cachedResolver);
    cluster.insert(txn3, cachedResolver);
    cluster.merge();

    auto clusterStore = std::make_unique<MockedStateClusterStore>();
    clusterStore->save(cluster);

    auto logReader = std::make_unique<MockedStateLogReader>();
    logReader->addTransaction(txn1, 1);
    logReader->addTransaction(txn2, 2);
    logReader->addTransaction(txn3, 3);

    MockedDBHandlePool pool(1, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::move(clusterStore);
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));

    changer.prepare();

    auto gids = loadReplayPlanGids(plan);
    REQUIRE(gids.size() == 1);
    REQUIRE(gids[0] == 3);
}

TEST_CASE("StateChanger replay respects dependency order within chains", "[statechanger][replay]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    constexpr int kThreadNum = 4;
    static_assert(kReplayTotalTransactions % kReplayChains == 0, "chain count must divide total transactions");

    auto plan = makePlan(kThreadNum);

    auto logReader = std::make_unique<MockedStateLogReader>();
    logReader->open();

    std::vector<std::vector<ultraverse::state::v2::gid_t>> chains(kReplayChains);
    std::vector<ultraverse::state::v2::gid_t> gidsToReplay;
    gidsToReplay.reserve(kReplayTotalTransactions);

    for (ultraverse::state::v2::gid_t gid = 1; gid <= static_cast<ultraverse::state::v2::gid_t>(kReplayTotalTransactions); gid++) {
        int chainIndex = static_cast<int>((gid - 1) % kReplayChains);
        int64_t keyValue = chainIndex + 1;
        StateItem keyItem = StateItem::EQ("items.id", StateData(keyValue));

        std::string statement = "/*TXN:" + std::to_string(gid) + "*/";
        auto txn = makeTransaction(gid, plan.dbName(), statement, {}, {keyItem});

        logReader->addTransaction(txn, gid);

        if (gid % 17 != 0) {
            gidsToReplay.push_back(gid);
            chains[chainIndex].push_back(gid);
        }
    }
    auto planDir = makeTempDir("replay_chain");
    const std::string planName = "plan";
    plan.setStateLogPath(planDir);
    plan.setStateLogName(planName);
    writeReplayPlan(planDir, planName, gidsToReplay);

    MockedDBHandlePool pool(kThreadNum, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::make_unique<MockedStateClusterStore>();
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));
    changer.replay();

    std::vector<std::string> executedQueries;
    {
        std::scoped_lock lock(sharedState->mutex);
        executedQueries = sharedState->queries;
    }

    auto executionOrder = extractExecutedGids(executedQueries);
    REQUIRE(executionOrder.size() == gidsToReplay.size());

    auto positionIndex = buildPositionIndex(executionOrder);
    for (const auto &chain : chains) {
        if (chain.empty()) {
            continue;
        }

        for (size_t i = 1; i < chain.size(); i++) {
            auto prevGid = chain[i - 1];
            auto nextGid = chain[i];
            REQUIRE(positionIndex.count(prevGid) == 1);
            REQUIRE(positionIndex.count(nextGid) == 1);
            REQUIRE(positionIndex[prevGid] < positionIndex[nextGid]);
        }
    }
}

/**
 * Ensures non-conflicting chains can execute in parallel by allowing
 * interleaving between chains while preserving per-chain order.
 */
TEST_CASE("StateChanger replay interleaves independent chains", "[statechanger][replay][parallel]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    constexpr int kThreadNum = 2;
    auto plan = makePlan(kThreadNum);

    auto logReader = std::make_unique<MockedStateLogReader>();
    logReader->open();

    std::vector<ultraverse::state::v2::gid_t> gidsToReplay;
    std::vector<ultraverse::state::v2::gid_t> chainA;
    std::vector<ultraverse::state::v2::gid_t> chainB;

    for (ultraverse::state::v2::gid_t gid = 1; gid <= 6; gid++) {
        int64_t keyValue = (gid % 2 == 1) ? 1 : 2;
        StateItem keyItem = StateItem::EQ("items.id", StateData(keyValue));

        std::string statement = "/*TXN:" + std::to_string(gid) + "*/";
        auto txn = makeTransaction(gid, plan.dbName(), statement, {}, {keyItem});
        logReader->addTransaction(txn, gid);
        gidsToReplay.push_back(gid);

        if (keyValue == 1) {
            chainA.push_back(gid);
        } else {
            chainB.push_back(gid);
        }
    }
    auto planDir = makeTempDir("replay_parallel");
    const std::string planName = "plan";
    plan.setStateLogPath(planDir);
    plan.setStateLogName(planName);
    writeReplayPlan(planDir, planName, gidsToReplay);

    std::unordered_map<ultraverse::state::v2::gid_t, std::chrono::milliseconds> delays{
        {1, std::chrono::milliseconds(80)},
        {3, std::chrono::milliseconds(80)},
        {5, std::chrono::milliseconds(80)}
    };

    DelayedDBHandlePool pool(kThreadNum, sharedState, delays);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::make_unique<MockedStateClusterStore>();
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));
    changer.replay();

    std::vector<std::string> executedQueries;
    {
        std::scoped_lock lock(sharedState->mutex);
        executedQueries = sharedState->queries;
    }

    auto executionOrder = extractExecutedGids(executedQueries);
    REQUIRE(executionOrder.size() == gidsToReplay.size());

    auto positionIndex = buildPositionIndex(executionOrder);
    for (size_t i = 1; i < chainA.size(); i++) {
        REQUIRE(positionIndex[chainA[i - 1]] < positionIndex[chainA[i]]);
    }
    for (size_t i = 1; i < chainB.size(); i++) {
        REQUIRE(positionIndex[chainB[i - 1]] < positionIndex[chainB[i]]);
    }

    const auto firstChainB = std::min({positionIndex[2], positionIndex[4], positionIndex[6]});
    const auto lastChainA = std::max({positionIndex[1], positionIndex[3], positionIndex[5]});
    REQUIRE(firstChainB < lastChainA);
}

TEST_CASE("StateChanger replay-from runs pre-replay range before replay plan", "[statechanger][replay][replay-from]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    constexpr int kThreadNum = 2;
    auto plan = makePlan(kThreadNum);
    plan.setReplayFromGid(2);

    auto planDir = makeTempDir("replay_from");
    const std::string planName = "plan";
    plan.setStateLogPath(planDir);
    plan.setStateLogName(planName);

    StateItem key1 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(1)));
    StateItem key2 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(2)));
    StateItem key3 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(3)));

    auto txn1 = makeTransaction(1, plan.dbName(), "/*TXN:1*/", {}, {key1});
    auto txn2 = makeTransaction(2, plan.dbName(), "/*TXN:2*/", {}, {key1});
    auto txn3 = makeTransaction(3, plan.dbName(), "/*TXN:3*/", {}, {key2});
    auto txn4 = makeTransaction(4, plan.dbName(), "/*TXN:4*/", {}, {key3});
    auto txn5 = makeTransaction(5, plan.dbName(), "/*TXN:5*/", {}, {key1});
    auto txn6 = makeTransaction(6, plan.dbName(), "/*TXN:6*/", {}, {key2});

    auto logReader = std::make_unique<MockedStateLogReader>();
    logReader->addTransaction(txn1, 1);
    logReader->addTransaction(txn2, 2);
    logReader->addTransaction(txn3, 3);
    logReader->addTransaction(txn4, 4);
    logReader->addTransaction(txn5, 5);
    logReader->addTransaction(txn6, 6);

    std::vector<ultraverse::state::v2::gid_t> replayGids{5, 6};
    std::vector<ultraverse::state::v2::gid_t> rollbackGids{4};
    writeReplayPlan(planDir, planName, replayGids, {}, rollbackGids);

    MockedDBHandlePool pool(kThreadNum, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::make_unique<MockedStateClusterStore>();
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));
    changer.replay();

    std::vector<std::string> executedQueries;
    {
        std::scoped_lock lock(sharedState->mutex);
        executedQueries = sharedState->queries;
    }

    auto executionOrder = extractExecutedGids(executedQueries);

    auto positionIndex = buildPositionIndex(executionOrder);
    REQUIRE(positionIndex.count(2) == 1);
    REQUIRE(positionIndex.count(3) == 1);
    REQUIRE(positionIndex.count(5) == 1);
    REQUIRE(positionIndex.count(6) == 1);
    REQUIRE(positionIndex.count(1) == 0);
    REQUIRE(positionIndex.count(4) == 0);

    const auto lastPreReplay = std::max(positionIndex[2], positionIndex[3]);
    const auto firstReplay = std::min(positionIndex[5], positionIndex[6]);
    REQUIRE(lastPreReplay < firstReplay);
}

TEST_CASE("StateChanger replay-from accepts gid 0", "[statechanger][replay][replay-from]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();
    seedEmptyInfoSchemaResults(sharedState);

    constexpr int kThreadNum = 2;
    auto plan = makePlan(kThreadNum);
    plan.setReplayFromGid(0);

    auto planDir = makeTempDir("replay_from_zero");
    const std::string planName = "plan";
    plan.setStateLogPath(planDir);
    plan.setStateLogName(planName);

    StateItem key0 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(0)));
    StateItem key1 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(1)));
    StateItem key2 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(2)));
    StateItem key3 = StateItem::EQ("items.id", StateData(static_cast<int64_t>(3)));

    auto txn0 = makeTransaction(0, plan.dbName(), "/*TXN:0*/", {}, {key0});
    auto txn1 = makeTransaction(1, plan.dbName(), "/*TXN:1*/", {}, {key1});
    auto txn2 = makeTransaction(2, plan.dbName(), "/*TXN:2*/", {}, {key2});
    auto txn3 = makeTransaction(3, plan.dbName(), "/*TXN:3*/", {}, {key3});

    auto logReader = std::make_unique<MockedStateLogReader>();
    logReader->addTransaction(txn0, 0);
    logReader->addTransaction(txn1, 1);
    logReader->addTransaction(txn2, 2);
    logReader->addTransaction(txn3, 3);

    std::vector<ultraverse::state::v2::gid_t> replayGids{3};
    std::vector<ultraverse::state::v2::gid_t> rollbackGids{2};
    writeReplayPlan(planDir, planName, replayGids, {}, rollbackGids);

    MockedDBHandlePool pool(kThreadNum, sharedState);

    StateChangerIO io;
    io.stateLogReader = std::move(logReader);
    io.clusterStore = std::make_unique<MockedStateClusterStore>();
    io.backupLoader = std::make_unique<NoopBackupLoader>();
    io.closeStandardFds = false;

    StateChanger changer(pool, plan, std::move(io));
    changer.replay();

    std::vector<std::string> executedQueries;
    {
        std::scoped_lock lock(sharedState->mutex);
        executedQueries = sharedState->queries;
    }

    auto executionOrder = extractExecutedGids(executedQueries);
    auto positionIndex = buildPositionIndex(executionOrder);

    REQUIRE(positionIndex.count(0) == 1);
    REQUIRE(positionIndex.count(1) == 1);
    REQUIRE(positionIndex.count(2) == 0);
    REQUIRE(positionIndex.count(3) == 1);

    const auto lastPreReplay = std::max(positionIndex[0], positionIndex[1]);
    REQUIRE(lastPreReplay < positionIndex[3]);
}
