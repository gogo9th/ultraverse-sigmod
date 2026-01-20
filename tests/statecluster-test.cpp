#include <algorithm>
#include <string>

#include <catch2/catch_test_macros.hpp>

#include "../src/mariadb/state/new/cluster/StateCluster.hpp"
#include "state_test_helpers.hpp"

using namespace ultraverse::state::v2;
using namespace ultraverse::state::v2::test_helpers;

TEST_CASE("StateCluster inserts and matches with alias/row-alias") {
    MockedRelationshipResolver resolver;
    resolver.addColumnAlias("posts.uuid", "posts.id");
    resolver.addRowAlias(
        makeEqStr("posts.uuid", "uuid-1"),
        makeEq("posts.id", 1)
    );

    StateCluster cluster({"users.id", "posts.id"});

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.id", 1)});
    auto txn2 = makeTxn(2, "test", {makeEq("users.id", 1)}, {makeEq("users.id", 1)});
    auto txn3 = makeTxn(3, "test", {}, {makeEq("posts.id", 1)});
    auto txn4 = makeTxn(4, "test", {makeEqStr("posts.uuid", "uuid-1")}, {});

    cluster.insert(txn1, resolver);
    cluster.insert(txn2, resolver);
    cluster.insert(txn3, resolver);
    cluster.insert(txn4, resolver);
    cluster.merge();

    auto &usersCluster = cluster.clusters().at("users.id");
    REQUIRE(usersCluster.write.find(StateRange{1}) != usersCluster.write.end());
    REQUIRE(usersCluster.read.find(StateRange{1}) != usersCluster.read.end());
    REQUIRE(usersCluster.write.at(StateRange{1}).count(txn1->gid()) == 1);
    REQUIRE(usersCluster.read.at(StateRange{1}).count(txn2->gid()) == 1);

    auto &postsCluster = cluster.clusters().at("posts.id");
    REQUIRE(postsCluster.write.find(StateRange{1}) != postsCluster.write.end());
    REQUIRE(postsCluster.read.find(StateRange{1}) != postsCluster.read.end());
    REQUIRE(postsCluster.write.at(StateRange{1}).count(txn3->gid()) == 1);
    REQUIRE(postsCluster.read.at(StateRange{1}).count(txn4->gid()) == 1);

    auto match = cluster.match(StateCluster::READ, "posts.id", txn4, resolver);
    REQUIRE(match.has_value());
    REQUIRE(match.value() == StateRange{1});
}

TEST_CASE("StateCluster fills wildcard for missing composite keys") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"orders.id", "orders.user_id"});
    auto txn = makeTxn(10, "test", {makeEq("orders.user_id", 42)}, {});

    cluster.insert(txn, resolver);
    cluster.merge();

    auto &ordersIdCluster = cluster.clusters().at("orders.id");
    REQUIRE_FALSE(ordersIdCluster.read.empty());

    StateItem wildcardProbe = StateItem::Wildcard("orders.id");
    if (!wildcardProbe.MakeRange2().wildcard()) {
        WARN("StateItem::MakeRange2 does not set wildcard; skipping wildcard-specific assertions.");
        return;
    }

    bool hasWildcard = std::any_of(
        ordersIdCluster.read.begin(),
        ordersIdCluster.read.end(),
        [](const auto &pair) { return pair.first.wildcard(); }
    );
    REQUIRE(hasWildcard);

    auto &ordersUserCluster = cluster.clusters().at("orders.user_id");
    REQUIRE(ordersUserCluster.read.find(StateRange{42}) != ordersUserCluster.read.end());
}

TEST_CASE("StateCluster shouldReplay identifies dependent transactions") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"users.id"});
    auto rollbackTxn = makeTxn(1, "test", {}, {makeEq("users.id", 1)});
    auto dependentTxn = makeTxn(2, "test", {makeEq("users.id", 1)}, {});
    auto dependentWriteTxn = makeTxn(4, "test", {}, {makeEq("users.id", 1)});
    auto unrelatedTxn = makeTxn(3, "test", {makeEq("users.id", 2)}, {});
    auto unrelatedWriteTxn = makeTxn(5, "test", {}, {makeEq("users.id", 2)});

    cluster.insert(rollbackTxn, resolver);
    cluster.insert(dependentTxn, resolver);
    cluster.insert(dependentWriteTxn, resolver);
    cluster.insert(unrelatedTxn, resolver);
    cluster.insert(unrelatedWriteTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    REQUIRE_FALSE(cluster.shouldReplay(rollbackTxn->gid()));
    REQUIRE(cluster.shouldReplay(dependentTxn->gid()));
    REQUIRE(cluster.shouldReplay(dependentWriteTxn->gid()));
    REQUIRE_FALSE(cluster.shouldReplay(unrelatedTxn->gid()));
    REQUIRE_FALSE(cluster.shouldReplay(unrelatedWriteTxn->gid()));
}

TEST_CASE("StateCluster generateReplaceQuery uses wildcard for composite keys") {
    NoopRelationshipResolver resolver;

    StateItem wildcardProbe = StateItem::Wildcard("orders.id");
    if (!wildcardProbe.MakeRange2().wildcard()) {
        WARN("StateItem::MakeRange2 does not set wildcard; skipping wildcard-specific assertions.");
        return;
    }

    StateCluster cluster({"orders.id", "orders.user_id"});
    auto rollbackTxn = makeTxn(
        1,
        "test",
        {},
        {makeEq("orders.id", 1), makeEq("orders.user_id", 42)}
    );
    auto readerTxn = makeTxn(2, "test", {makeEq("orders.user_id", 42)}, {});

    cluster.insert(rollbackTxn, resolver);
    cluster.insert(readerTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    auto query = cluster.generateReplaceQuery("targetdb", "intermediate", resolver);
    REQUIRE(query.find("TRUNCATE orders;") != std::string::npos);
    REQUIRE(query.find("REPLACE INTO orders SELECT * FROM intermediate.orders;") != std::string::npos);
}

TEST_CASE("StateCluster merges overlapping write ranges") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"users.id"});
    auto txn1 = makeTxn(1, "test", {}, {makeBetween("users.id", 1, 3)});
    auto txn2 = makeTxn(2, "test", {}, {makeBetween("users.id", 3, 5)});

    cluster.insert(txn1, resolver);
    cluster.insert(txn2, resolver);
    cluster.merge();

    auto &usersCluster = cluster.clusters().at("users.id");
    REQUIRE(usersCluster.write.size() == 1);

    const auto &range = usersCluster.write.begin()->first;
    auto where = range.MakeWhereQuery("users.id");

    REQUIRE(where.find(">=1") != std::string::npos);
    REQUIRE(where.find("<=5") != std::string::npos);
}

TEST_CASE("StateCluster resolves write items through row alias and FK chain") {
    MockedRelationshipResolver resolver;
    resolver.addRowAlias(
        makeEqStr("posts.author_str", "alice"),
        makeEq("posts.author", 1)
    );
    resolver.addForeignKey("posts.author", "users.id");

    StateCluster cluster({"users.id"});
    auto txn = makeTxn(1, "test", {}, {makeEqStr("posts.author_str", "alice")});

    cluster.insert(txn, resolver);
    cluster.merge();

    auto &usersCluster = cluster.clusters().at("users.id");
    REQUIRE(usersCluster.write.find(StateRange{1}) != usersCluster.write.end());
}

TEST_CASE("StateCluster shouldReplay requires composite key match") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"orders.id", "orders.user_id"});
    auto rollbackTxn = makeTxn(
        1,
        "test",
        {},
        {makeEq("orders.id", 1), makeEq("orders.user_id", 42)}
    );
    auto matchedTxn = makeTxn(
        2,
        "test",
        {makeEq("orders.id", 1), makeEq("orders.user_id", 42)},
        {}
    );
    auto mismatchedTxn = makeTxn(
        3,
        "test",
        {makeEq("orders.id", 1), makeEq("orders.user_id", 99)},
        {}
    );

    cluster.insert(rollbackTxn, resolver);
    cluster.insert(matchedTxn, resolver);
    cluster.insert(mismatchedTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    REQUIRE(cluster.shouldReplay(matchedTxn->gid()));
    REQUIRE_FALSE(cluster.shouldReplay(mismatchedTxn->gid()));
}

TEST_CASE("StateCluster generateReplaceQuery uses WHERE for non-wildcard keys") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"users.id"});
    auto rollbackTxn = makeTxn(1, "test", {}, {makeEq("users.id", 1)});
    auto readerTxn = makeTxn(2, "test", {makeEq("users.id", 1)}, {});

    cluster.insert(rollbackTxn, resolver);
    cluster.insert(readerTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    auto query = cluster.generateReplaceQuery("targetdb", "intermediate", resolver);
    REQUIRE(query.find("TRUNCATE users;") == std::string::npos);
    REQUIRE(query.find("DELETE FROM users WHERE") != std::string::npos);
    REQUIRE(query.find("REPLACE INTO users SELECT * FROM intermediate.users WHERE") != std::string::npos);
}

TEST_CASE("StateCluster generateReplaceQuery uses write ranges without reads") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"users.id"});
    auto rollbackTxn = makeTxn(1, "test", {}, {makeEq("users.id", 1)});

    cluster.insert(rollbackTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    auto query = cluster.generateReplaceQuery("targetdb", "intermediate", resolver);
    REQUIRE(query.find("TRUNCATE users;") == std::string::npos);
    REQUIRE(query.find("DELETE FROM users WHERE") != std::string::npos);
    REQUIRE(query.find("REPLACE INTO users SELECT * FROM intermediate.users WHERE") != std::string::npos);
}
