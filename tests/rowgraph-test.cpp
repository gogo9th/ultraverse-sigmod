#include <chrono>
#include <set>
#include <string>
#include <unordered_set>

#include <catch2/catch_test_macros.hpp>

#include "../src/mariadb/state/new/RangeComparisonMethod.hpp"
#include "../src/mariadb/state/new/graph/RowGraph.hpp"
#include "state_test_helpers.hpp"

using namespace ultraverse::state::v2;
using namespace ultraverse::state::v2::test_helpers;

TEST_CASE("RowGraph builds dependencies and entrypoints") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"users.id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.id", 1)});
    auto txn2 = makeTxn(2, "test", {makeEq("users.id", 1)}, {});
    auto txn3 = makeTxn(3, "test", {}, {makeEq("users.id", 1)});
    auto txn4 = makeTxn(4, "test", {makeEq("users.id", 2)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);
    auto n3 = graph.addNode(txn3);
    auto n4 = graph.addNode(txn4);

    REQUIRE(waitUntilAllReady(graph, {n1, n2, n3, n4}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(4) != entryGids.end());
    REQUIRE(entryGids.find(2) == entryGids.end());
    REQUIRE(entryGids.find(3) == entryGids.end());

    graph.nodeFor(n1)->finalized = true;
    graph.nodeFor(n4)->finalized = true;

    entryGids = entrypointGids(graph);
    REQUIRE(entryGids.size() == 1);
    REQUIRE(entryGids.find(2) != entryGids.end());

    graph.nodeFor(n2)->finalized = true;

    entryGids = entrypointGids(graph);
    REQUIRE(entryGids.size() == 1);
    REQUIRE(entryGids.find(3) != entryGids.end());
}

TEST_CASE("RowGraph range comparison method affects dependencies") {
    NoopRelationshipResolver resolver;

    {
        RowGraph graph({"users.id"}, resolver);
        graph.setRangeComparisonMethod(RangeComparisonMethod::EQ_ONLY);

        auto txn1 = makeTxn(1, "test", {}, {makeBetween("users.id", 1, 10)});
        auto txn2 = makeTxn(2, "test", {makeEq("users.id", 5)}, {});

        auto n1 = graph.addNode(txn1);
        auto n2 = graph.addNode(txn2);

        REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

        auto entryGids = entrypointGids(graph);
        REQUIRE(entryGids.find(1) != entryGids.end());
        REQUIRE(entryGids.find(2) != entryGids.end());
    }

    {
        RowGraph graph({"users.id"}, resolver);
        graph.setRangeComparisonMethod(RangeComparisonMethod::INTERSECT);

        auto txn1 = makeTxn(1, "test", {}, {makeBetween("users.id", 1, 10)});
        auto txn2 = makeTxn(2, "test", {makeEq("users.id", 5)}, {});

        auto n1 = graph.addNode(txn1);
        auto n2 = graph.addNode(txn2);

        REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

        auto entryGids = entrypointGids(graph);
        REQUIRE(entryGids.find(1) != entryGids.end());
        REQUIRE(entryGids.find(2) == entryGids.end());
    }
}

TEST_CASE("RowGraph entrypoints are deterministic under parallel build") {
    NoopRelationshipResolver resolver;

    for (int i = 0; i < 100; i++) {
        RowGraph graph({"users.id"}, resolver);
        graph.setRangeComparisonMethod(RangeComparisonMethod::INTERSECT);

        auto txn1 = makeTxn(1, "test", {}, {makeBetween("users.id", 1, 10)});
        auto txn2 = makeTxn(2, "test", {makeEq("users.id", 5)}, {});

        auto n1 = graph.addNode(txn1);
        auto n2 = graph.addNode(txn2);

        REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

        auto entryGids = entrypointGids(graph);
        REQUIRE(entryGids.find(1) != entryGids.end());
        REQUIRE(entryGids.find(2) == entryGids.end());
    }
}

TEST_CASE("RowGraph serializes when key columns are missing") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"users.id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("orders.id", 1)});
    auto txn2 = makeTxn(2, "test", {}, {makeEq("users.id", 1)});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) == entryGids.end());
}

TEST_CASE("RowGraph resolves foreign key dependencies") {
    MockedRelationshipResolver resolver;
    resolver.addForeignKey("posts.author_id", "users.id");
    RowGraph graph({"users.id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.id", 1)});
    auto txn2 = makeTxn(2, "test", {makeEq("posts.author_id", 1)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) == entryGids.end());
}

TEST_CASE("RowGraph resolves row aliases to key columns") {
    MockedRelationshipResolver resolver;
    resolver.addRowAlias(makeEqStr("users.handle", "alice"), makeEq("users.id", 1));
    RowGraph graph({"users.id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.id", 1)});
    auto txn2 = makeTxn(2, "test", {makeEqStr("users.handle", "alice")}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) == entryGids.end());
}

TEST_CASE("RowGraph uses key-set intersection for multi-table groups") {
    NoopRelationshipResolver resolver;
    std::set<std::string> keyColumns{"flight.f_id", "customer.c_id"};
    std::vector<std::vector<std::string>> keyGroups{{"flight.f_id", "customer.c_id"}};
    RowGraph graph(keyColumns, resolver, keyGroups);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("flight.f_id", 1)});
    auto txn2 = makeTxn(2, "test", {}, {makeEq("customer.c_id", 2)});
    auto txn3 = makeTxn(3, "test", {}, {makeEq("flight.f_id", 1), makeEq("customer.c_id", 2)});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);
    auto n3 = graph.addNode(txn3);

    REQUIRE(waitUntilAllReady(graph, {n1, n2, n3}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) != entryGids.end());
    REQUIRE(entryGids.find(3) == entryGids.end());
}

TEST_CASE("RowGraph does not wildcard missing columns in multi-table groups") {
    MockedRelationshipResolver resolver;
    resolver.addForeignKey("reservation.f_id", "flight.f_id");
    resolver.addForeignKey("reservation.c_id", "customer.c_id");

    std::set<std::string> keyColumns{"flight.f_id", "customer.c_id"};
    std::vector<std::vector<std::string>> keyGroups{{"flight.f_id", "customer.c_id"}};
    RowGraph graph(keyColumns, resolver, keyGroups);

    auto txnFlight = makeTxn(1, "test", {}, {makeEq("flight.f_id", 1)});
    auto txnCustomer = makeTxn(2, "test", {}, {makeEq("customer.c_id", 2)});
    auto txnReservation = makeTxn(3, "test", {makeEq("reservation.c_id", 2)}, {});

    auto n1 = graph.addNode(txnFlight);
    auto n2 = graph.addNode(txnCustomer);
    auto n3 = graph.addNode(txnReservation);

    REQUIRE(waitUntilAllReady(graph, {n1, n2, n3}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) != entryGids.end());
    REQUIRE(entryGids.find(3) == entryGids.end());

    graph.nodeFor(n2)->finalized = true;
    entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(3) != entryGids.end());
}

TEST_CASE("RowGraph limits table wildcard to touched table in multi-table groups") {
    NoopRelationshipResolver resolver;
    std::set<std::string> keyColumns{"flight.f_id", "customer.c_id"};
    std::vector<std::vector<std::string>> keyGroups{{"flight.f_id", "customer.c_id"}};
    RowGraph graph(keyColumns, resolver, keyGroups);

    auto txnFlight = makeTxn(1, "test", {}, {makeEq("flight.f_id", 1)});
    auto txnCustomer = makeTxn(2, "test", {}, {makeEq("customer.c_id", 2)});
    auto txnFlightOnly = makeTxn(3, "test", {makeEq("flight.name", 1)}, {});

    auto n1 = graph.addNode(txnFlight);
    auto n2 = graph.addNode(txnCustomer);
    auto n3 = graph.addNode(txnFlightOnly);

    REQUIRE(waitUntilAllReady(graph, {n1, n2, n3}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) != entryGids.end());
    REQUIRE(entryGids.find(3) == entryGids.end());

    graph.nodeFor(n1)->finalized = true;
    entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(3) != entryGids.end());
}

TEST_CASE("RowGraph resolves column alias through foreign key chain") {
    MockedRelationshipResolver resolver;
    resolver.addColumnAlias("orders.user_id_str", "orders.user_id");
    resolver.addForeignKey("orders.user_id", "users.id");
    RowGraph graph({"users.id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.id", 42)});
    auto txn2 = makeTxn(2, "test", {makeEq("orders.user_id_str", 42)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) == entryGids.end());
}

TEST_CASE("RowGraph resolves row alias through foreign key chain") {
    MockedRelationshipResolver resolver;
    resolver.addRowAlias(makeEqStr("orders.user_id_str", "000042"), makeEq("orders.user_id", 42));
    resolver.addForeignKey("orders.user_id", "users.id");
    RowGraph graph({"users.id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.id", 42)});
    auto txn2 = makeTxn(2, "test", {makeEqStr("orders.user_id_str", "000042")}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) == entryGids.end());
}

TEST_CASE("RowGraph applies wildcard for keyless table access") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"users.id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.name", 1)});
    auto txn2 = makeTxn(2, "test", {makeEq("users.id", 1)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) == entryGids.end());
}

TEST_CASE("RowGraph treats multi-dimensional keys as conjunction (paper spec)") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"orders.user_id", "orders.item_id"}, resolver, {{"orders.user_id", "orders.item_id"}});

    auto txn1 = makeTxn(1, "test", {}, {makeEq("orders.user_id", 1), makeEq("orders.item_id", 10)});
    auto txn2 = makeTxn(2, "test", {makeEq("orders.user_id", 1), makeEq("orders.item_id", 11)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) != entryGids.end());
}

TEST_CASE("RowGraph separates dependencies by key column") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"users.id", "users.group_id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.id", 1)});
    auto txn2 = makeTxn(2, "test", {}, {makeEq("users.group_id", 7)});
    auto txn3 = makeTxn(3, "test", {makeEq("users.id", 1)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);
    auto n3 = graph.addNode(txn3);

    REQUIRE(waitUntilAllReady(graph, {n1, n2, n3}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) != entryGids.end());
    REQUIRE(entryGids.find(3) == entryGids.end());

    graph.nodeFor(n1)->finalized = true;

    entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) == entryGids.end());
    REQUIRE(entryGids.find(2) != entryGids.end());
    REQUIRE(entryGids.find(3) != entryGids.end());
}

TEST_CASE("RowGraph intersects ranges for dependencies") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"users.id"}, resolver);
    graph.setRangeComparisonMethod(RangeComparisonMethod::INTERSECT);

    auto txn1 = makeTxn(1, "test", {}, {makeBetween("users.id", 1, 10)});
    auto txn2 = makeTxn(2, "test", {makeBetween("users.id", 5, 15)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) == entryGids.end());
}

TEST_CASE("RowGraph treats disjoint ranges as independent") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"users.id"}, resolver);
    graph.setRangeComparisonMethod(RangeComparisonMethod::INTERSECT);

    auto txn1 = makeTxn(1, "test", {}, {makeBetween("users.id", 1, 10)});
    auto txn2 = makeTxn(2, "test", {makeBetween("users.id", 20, 30)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));

    auto entryGids = entrypointGids(graph);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(entryGids.find(2) != entryGids.end());
}

TEST_CASE("RowGraph GC clears nodeMap entries for finalized nodes") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"users.id"}, resolver);

    auto txn1 = makeTxn(1, "test", {}, {makeEq("users.id", 1)});
    auto txn2 = makeTxn(2, "test", {makeEq("users.id", 1)}, {});

    auto n1 = graph.addNode(txn1);
    auto n2 = graph.addNode(txn2);

    REQUIRE(waitUntilAllReady(graph, {n1, n2}, std::chrono::milliseconds(5000)));
    REQUIRE(graph.debugTotalNodeMapSize() > 0);

    graph.nodeFor(n1)->finalized = true;
    graph.nodeFor(n1)->transaction.reset();
    graph.nodeFor(n2)->finalized = true;
    graph.nodeFor(n2)->transaction.reset();

    graph.gc();
    REQUIRE(graph.debugTotalNodeMapSize() == 0);
}

TEST_CASE("RowGraph entrypoints scale with thousands of nodes") {
    NoopRelationshipResolver resolver;
    RowGraph graph({"users.id"}, resolver);

    const int kNodes = 4000;
    std::vector<RowGraphId> ids;
    ids.reserve(kNodes);

    for (int i = 0; i < kNodes; i++) {
        const ultraverse::state::v2::gid_t gid = static_cast<ultraverse::state::v2::gid_t>(i + 1);
        if (i % 2 == 0) {
            auto txn = makeTxn(gid, "test", {}, {makeEq("users.id", 1)});
            ids.push_back(graph.addNode(txn));
        } else {
            auto txn = makeTxn(gid, "test", {makeEq("users.id", 1)}, {});
            ids.push_back(graph.addNode(txn));
        }
    }

    REQUIRE(waitUntilAllReady(graph, ids, std::chrono::milliseconds(10000)));

    auto start = std::chrono::steady_clock::now();
    auto entryGids = entrypointGids(graph);
    auto elapsed = std::chrono::steady_clock::now() - start;

    REQUIRE(entryGids.size() == 1);
    REQUIRE(entryGids.find(1) != entryGids.end());
    REQUIRE(elapsed < std::chrono::milliseconds(2000));
}
