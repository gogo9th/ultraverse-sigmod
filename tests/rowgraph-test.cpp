#include <chrono>
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
