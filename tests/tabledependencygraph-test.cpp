#include <catch2/catch_test_macros.hpp>

#include <set>
#include <string>
#include <vector>

#include "mariadb/state/new/TableDependencyGraph.hpp"
#include "mariadb/state/new/StateChangeContext.hpp"
#include "mariadb/state/new/cluster/NamingHistory.hpp"

using ultraverse::state::v2::TableDependencyGraph;
using ultraverse::state::v2::ColumnSet;
using ultraverse::state::v2::ForeignKey;
using ultraverse::state::v2::NamingHistory;

namespace {
std::set<std::string> asSet(const std::vector<std::string> &values) {
    return std::set<std::string>(values.begin(), values.end());
}

ForeignKey makeFK(const std::string &fromTable, const std::string &toTable) {
    ForeignKey fk;
    fk.fromTable = std::make_shared<NamingHistory>(fromTable);
    fk.toTable = std::make_shared<NamingHistory>(toTable);
    fk.fromColumn = "id";
    fk.toColumn = "id";
    return fk;
}
} // namespace

TEST_CASE("TableDependencyGraph addTable prevents duplicates", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    REQUIRE(graph.addTable("users"));
    REQUIRE_FALSE(graph.addTable("users"));
    REQUIRE(graph.getDependencies("users").empty());
}

TEST_CASE("TableDependencyGraph addRelationship auto-adds tables and de-duplicates", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    REQUIRE(graph.addRelationship("users", "orders"));
    REQUIRE_FALSE(graph.addRelationship("users", "orders"));

    REQUIRE(graph.isRelated("users", "orders"));
    REQUIRE_FALSE(graph.isRelated("orders", "users"));

    auto deps = graph.getDependencies("users");
    REQUIRE(asSet(deps) == std::set<std::string>{"orders"});
}

TEST_CASE("TableDependencyGraph addRelationship from ColumnSet builds cartesian edges", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet{"users.id", "payments.id"};
    ColumnSet writeSet{"orders.total", "payments.amount"};

    REQUIRE(graph.addRelationship(readSet, writeSet));

    REQUIRE(asSet(graph.getDependencies("users")) ==
            std::set<std::string>{"orders", "payments"});
    REQUIRE(asSet(graph.getDependencies("payments")) ==
            std::set<std::string>{"orders", "payments"});

    REQUIRE_FALSE(graph.addRelationship(readSet, writeSet));
}

TEST_CASE("TableDependencyGraph addRelationship handles empty sets", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet{"users.id"};
    ColumnSet emptySet;

    REQUIRE_FALSE(graph.addRelationship(emptySet, readSet));
    REQUIRE_FALSE(graph.addRelationship(readSet, emptySet));
}

TEST_CASE("TableDependencyGraph addRelationship handles table-only columns", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet{"users"};
    ColumnSet writeSet{"orders.id"};

    REQUIRE(graph.addRelationship(readSet, writeSet));
    REQUIRE(asSet(graph.getDependencies("users")) == std::set<std::string>{"orders"});
}

TEST_CASE("TableDependencyGraph addRelationship from foreign keys", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    std::vector<ForeignKey> foreignKeys;
    foreignKeys.emplace_back(makeFK("orders", "users"));

    REQUIRE(graph.addRelationship(foreignKeys));
    REQUIRE(asSet(graph.getDependencies("orders")) == std::set<std::string>{"users"});
}

TEST_CASE("TableDependencyGraph getDependencies for missing table is empty", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    REQUIRE(graph.getDependencies("missing").empty());
}

TEST_CASE("TableDependencyGraph hasPeerDependencies behavior", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    REQUIRE_FALSE(graph.hasPeerDependencies("missing"));

    graph.addRelationship("users", "orders");
    REQUIRE_FALSE(graph.hasPeerDependencies("users"));
    REQUIRE(graph.hasPeerDependencies("orders"));
}

TEST_CASE("TableDependencyGraph isRelated returns false for missing tables", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    REQUIRE_FALSE(graph.isRelated("unknown", "users"));
    REQUIRE_FALSE(graph.isRelated("users", "unknown"));

    graph.addRelationship("users", "orders");
    REQUIRE(graph.isRelated("users", "orders"));
}
