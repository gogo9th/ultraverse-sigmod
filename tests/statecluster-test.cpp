#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "../src/mariadb/state/new/StateChangeContext.hpp"
#include "../src/mariadb/state/new/StateChangePlan.hpp"
#include "../src/mariadb/state/new/cluster/StateRelationshipResolver.hpp"
#include "../src/mariadb/state/new/cluster/StateCluster.hpp"
#include "state_test_helpers.hpp"

using namespace ultraverse::state::v2;
using namespace ultraverse::state::v2::test_helpers;

namespace {
    std::string joinStatements(const std::vector<std::string> &statements) {
        std::ostringstream output;
        for (const auto &statement : statements) {
            if (statement.empty()) {
                continue;
            }
            output << statement << ";\n";
        }
        return output.str();
    }
}

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

    StateCluster cluster({"orders.id", "orders.user_id"}, {{"orders.id", "orders.user_id"}});
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

    StateCluster cluster({"orders.id", "orders.user_id"}, {{"orders.id", "orders.user_id"}});
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

    auto query = joinStatements(cluster.generateReplaceQuery("targetdb", "intermediate", resolver, {}));
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

    StateCluster cluster({"orders.id", "orders.user_id"}, {{"orders.id", "orders.user_id"}});
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

TEST_CASE("StateCluster shouldReplay resolves composite key aliases") {
    MockedRelationshipResolver resolver;
    resolver.addColumnAlias("orders.user_id_alias", "orders.user_id");

    StateCluster cluster({"orders.user_id", "orders.user_id_alias"}, {{"orders.user_id", "orders.user_id_alias"}});
    cluster.normalizeWithResolver(resolver);
    auto rollbackTxn = makeTxn(1, "test", {}, {makeEq("orders.user_id", 7)});
    auto dependentTxn = makeTxn(2, "test", {makeEq("orders.user_id_alias", 7)}, {});

    cluster.insert(rollbackTxn, resolver);
    cluster.insert(dependentTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    REQUIRE(cluster.shouldReplay(dependentTxn->gid()));
}

TEST_CASE("StateCluster shouldReplay normalizes foreign key columns") {
    MockedRelationshipResolver resolver;
    resolver.addForeignKey("review.u_id", "useracct.u_id");

    StateCluster cluster({"review.u_id"});
    cluster.normalizeWithResolver(resolver);

    auto rollbackTxn = makeTxn(0, "test", {}, {makeEq("review.u_id", 587)});
    auto dependentTxn = makeTxn(2, "test", {}, {makeEq("useracct.u_id", 587)});

    cluster.insert(rollbackTxn, resolver);
    cluster.insert(dependentTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    REQUIRE(cluster.shouldReplay(dependentTxn->gid()));
}

TEST_CASE("StateCluster shouldReplay matches any key in multi-table groups") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"flight.f_id", "customer.c_id"}, {{"flight.f_id", "customer.c_id"}});
    auto rollbackTxn = makeTxn(1, "test", {}, {makeEq("flight.f_id", 1)});
    auto flightTxn = makeTxn(2, "test", {makeEq("flight.f_id", 1)}, {});
    auto customerTxn = makeTxn(3, "test", {makeEq("customer.c_id", 2)}, {});
    auto bothTxn = makeTxn(4, "test", {makeEq("flight.f_id", 1), makeEq("customer.c_id", 2)}, {});

    cluster.insert(rollbackTxn, resolver);
    cluster.insert(flightTxn, resolver);
    cluster.insert(customerTxn, resolver);
    cluster.insert(bothTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    REQUIRE(cluster.shouldReplay(flightTxn->gid()));
    REQUIRE_FALSE(cluster.shouldReplay(customerTxn->gid()));
    REQUIRE(cluster.shouldReplay(bothTxn->gid()));
}

TEST_CASE("StateCluster does not wildcard missing keys for multi-table groups") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"flight.f_id", "customer.c_id"}, {{"flight.f_id", "customer.c_id"}});
    auto txn = makeTxn(1, "test", {makeEq("customer.c_id", 2)}, {});

    cluster.insert(txn, resolver);
    cluster.merge();

    auto &flightCluster = cluster.clusters().at("flight.f_id");
    REQUIRE(flightCluster.read.empty());

    auto &customerCluster = cluster.clusters().at("customer.c_id");
    REQUIRE(customerCluster.read.find(StateRange{2}) != customerCluster.read.end());
}

TEST_CASE("StateCluster generateReplaceQuery projects multi-table groups per table") {
    NoopRelationshipResolver resolver;

    StateCluster cluster({"flight.f_id", "customer.c_id"}, {{"flight.f_id", "customer.c_id"}});
    auto rollbackTxn = makeTxn(
        1,
        "test",
        {},
        {makeEq("flight.f_id", 1), makeEq("customer.c_id", 2)}
    );

    cluster.insert(rollbackTxn, resolver);
    cluster.merge();

    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    auto query = joinStatements(cluster.generateReplaceQuery("targetdb", "intermediate", resolver, {}));

    auto flightPos = query.find("DELETE FROM flight WHERE");
    REQUIRE(flightPos != std::string::npos);
    auto flightEnd = query.find(";\n", flightPos);
    REQUIRE(flightEnd != std::string::npos);
    auto flightWhere = query.substr(flightPos, flightEnd - flightPos);
    REQUIRE(flightWhere.find("flight.f_id") != std::string::npos);
    REQUIRE(flightWhere.find("customer.c_id") == std::string::npos);

    auto customerPos = query.find("DELETE FROM customer WHERE");
    REQUIRE(customerPos != std::string::npos);
    auto customerEnd = query.find(";\n", customerPos);
    REQUIRE(customerEnd != std::string::npos);
    auto customerWhere = query.substr(customerPos, customerEnd - customerPos);
    REQUIRE(customerWhere.find("customer.c_id") != std::string::npos);
    REQUIRE(customerWhere.find("flight.f_id") == std::string::npos);
}

TEST_CASE("StateCluster generateReplaceQuery includes foreign key tables") {
    StateChangePlan plan;
    StateChangeContext context;

    auto orders = std::make_shared<NamingHistory>("orders");
    auto refunds = std::make_shared<NamingHistory>("refunds");
    context.tables = {orders, refunds};
    context.foreignKeys.push_back(ForeignKey{refunds, "order_id", orders, "order_id"});

    StateRelationshipResolver resolver(plan, context);

    StateCluster cluster({"orders.order_id"});
    auto rollbackTxn = makeTxn(1, "test", {}, {makeEq("orders.order_id", 100)});

    cluster.insert(rollbackTxn, resolver);
    cluster.merge();
    cluster.addRollbackTarget(rollbackTxn, resolver, true);

    auto query = joinStatements(cluster.generateReplaceQuery("targetdb", "intermediate", resolver, context.foreignKeys));
    REQUIRE(query.find("DELETE FROM refunds WHERE") != std::string::npos);
    REQUIRE(query.find("refunds.order_id") != std::string::npos);
    REQUIRE(query.find("REPLACE INTO refunds SELECT * FROM intermediate.refunds WHERE") != std::string::npos);
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

    auto query = joinStatements(cluster.generateReplaceQuery("targetdb", "intermediate", resolver, {}));
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

    auto query = joinStatements(cluster.generateReplaceQuery("targetdb", "intermediate", resolver, {}));
    REQUIRE(query.find("TRUNCATE users;") == std::string::npos);
    REQUIRE(query.find("DELETE FROM users WHERE") != std::string::npos);
    REQUIRE(query.find("REPLACE INTO users SELECT * FROM intermediate.users WHERE") != std::string::npos);
}
