#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "state_test_helpers.hpp"

#include "mariadb/DBHandle.hpp"
#include "mariadb/state/new/StateChanger.hpp"
#include "mariadb/state/new/StateChangePlan.hpp"
#include "mariadb/state/new/StateChangeContext.hpp"
#include "mariadb/state/new/StateIO.hpp"
#include "mariadb/state/new/cluster/StateCluster.hpp"
#include "mariadb/state/new/cluster/StateRelationshipResolver.hpp"
#include "utils/StringUtil.hpp"

namespace {
    using ultraverse::mariadb::MockedDBHandle;
    using ultraverse::state::v2::StateChanger;
    using ultraverse::state::v2::StateChangerIO;
    using ultraverse::state::v2::StateChangePlan;
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

    class ScopedIStreamRedirect {
    public:
        ScopedIStreamRedirect(std::istream &stream, std::streambuf *newBuf):
            _stream(stream),
            _oldBuf(stream.rdbuf(newBuf)) {
        }

        ~ScopedIStreamRedirect() {
            _stream.rdbuf(_oldBuf);
        }

    private:
        std::istream &_stream;
        std::streambuf *_oldBuf;
    };

    class ScopedOStreamRedirect {
    public:
        ScopedOStreamRedirect(std::ostream &stream, std::streambuf *newBuf):
            _stream(stream),
            _oldBuf(stream.rdbuf(newBuf)) {
        }

        ~ScopedOStreamRedirect() {
            _stream.rdbuf(_oldBuf);
        }

    private:
        std::ostream &_stream;
        std::streambuf *_oldBuf;
    };

    StateChangePlan makePlan(int threadNum) {
        StateChangePlan plan;
        plan.setDBName("testdb");
        plan.setThreadNum(threadNum);
        plan.setDropIntermediateDB(false);
        plan.setFullReplay(false);
        plan.setDryRun(false);
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

    std::vector<gid_t> parseGidLines(const std::string &output) {
        std::vector<gid_t> gids;
        std::istringstream stream(output);
        std::string line;

        while (std::getline(stream, line)) {
            if (line.empty()) {
                continue;
            }
            gids.push_back(static_cast<gid_t>(std::stoull(line)));
        }

        return gids;
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

    std::ostringstream out;
    ScopedOStreamRedirect coutRedirect(std::cout, out.rdbuf());

    changer.prepare();

    auto gids = parseGidLines(out.str());
    REQUIRE(gids.size() == 1);
    REQUIRE(gids[0] == 2);
}

TEST_CASE("StateChanger prepare handles multiple rollback targets and partial-key filtering", "[statechanger][prepare]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();

    auto plan = makePlan(1);
    plan.keyColumns().insert("items.type");
    plan.keyColumns().insert("orders.id");

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

    std::ostringstream out;
    ScopedOStreamRedirect coutRedirect(std::cout, out.rdbuf());

    changer.prepare();

    auto gids = parseGidLines(out.str());
    std::sort(gids.begin(), gids.end());
    std::sort(expected.begin(), expected.end());

    REQUIRE(!expected.empty());
    REQUIRE(!gids.empty());

    REQUIRE(gids == expected);
}

TEST_CASE("StateChanger prepare includes column-wise dependent queries without key columns", "[statechanger][prepare][columnwise]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();

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

    std::ostringstream out;
    ScopedOStreamRedirect coutRedirect(std::cout, out.rdbuf());

    changer.prepare();

    auto gids = parseGidLines(out.str());
    REQUIRE(gids.size() == 1);
    REQUIRE(gids[0] == 2);
}

/**
 * Ensures column-wise dependency propagation is transitive: if a dependent
 * transaction writes a new column, later reads of that column are replayed.
 */
TEST_CASE("StateChanger prepare propagates column taint transitively", "[statechanger][prepare][columnwise]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();

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

    std::ostringstream out;
    ScopedOStreamRedirect coutRedirect(std::cout, out.rdbuf());

    changer.prepare();

    auto gids = parseGidLines(out.str());
    std::sort(gids.begin(), gids.end());

    REQUIRE(gids.size() == 2);
    REQUIRE(gids[0] == 2);
    REQUIRE(gids[1] == 3);
}

TEST_CASE("StateChanger replay respects dependency order within chains", "[statechanger][replay]") {
    auto sharedState = std::make_shared<MockedDBHandle::SharedState>();

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

    std::ostringstream stdinBuilder;
    for (auto gid : gidsToReplay) {
        stdinBuilder << gid << "\n";
    }
    std::istringstream stdinSource(stdinBuilder.str());
    ScopedIStreamRedirect cinRedirect(std::cin, stdinSource.rdbuf());

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

    std::ostringstream stdinBuilder;
    for (auto gid : gidsToReplay) {
        stdinBuilder << gid << "\n";
    }
    std::istringstream stdinSource(stdinBuilder.str());
    ScopedIStreamRedirect cinRedirect(std::cin, stdinSource.rdbuf());

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
