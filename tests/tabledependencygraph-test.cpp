#include <catch2/catch_test_macros.hpp>

#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "ultraverse_state.pb.h"

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

bool hasPath(TableDependencyGraph &graph, const std::string &from, const std::string &to) {
    if (from == to) {
        return true;
    }

    std::unordered_set<std::string> visited;
    std::vector<std::string> stack;
    stack.push_back(from);

    while (!stack.empty()) {
        auto current = stack.back();
        stack.pop_back();
        if (!visited.insert(current).second) {
            continue;
        }
        for (const auto &next : graph.getDependencies(current)) {
            if (next == to) {
                return true;
            }
            if (visited.find(next) == visited.end()) {
                stack.push_back(next);
            }
        }
    }

    return false;
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

TEST_CASE("TableDependencyGraph addRelationship handles write-only sets", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet{"users.id"};
    ColumnSet emptySet;

    REQUIRE(graph.addRelationship(emptySet, readSet));
    REQUIRE(asSet(graph.getDependencies("users")) == std::set<std::string>{"users"});
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

TEST_CASE("TableDependencyGraph addRelationship ignores read-only query (SELECT)", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet{"users.id", "accounts.balance"};
    ColumnSet writeSet;

    REQUIRE_FALSE(graph.addRelationship(readSet, writeSet));
    REQUIRE(graph.getDependencies("users").empty());
    REQUIRE(graph.getDependencies("accounts").empty());
}

/**
 * Ensures transitive dependency reachability can be derived from the graph
 * (Rule 2), even when only direct edges are stored.
 */
TEST_CASE("TableDependencyGraph transitive reachability via traversal", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    graph.addRelationship("users", "orders");
    graph.addRelationship("orders", "payments");

    REQUIRE(hasPath(graph, "users", "orders"));
    REQUIRE(hasPath(graph, "orders", "payments"));
    REQUIRE(hasPath(graph, "users", "payments"));
}

TEST_CASE("TableDependencyGraph Table A INSERT policy maps reads to target writes", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet{"users.id", "accounts.id"};
    ColumnSet writeSet{"transactions.id", "transactions.amount"};

    REQUIRE(graph.addRelationship(readSet, writeSet));

    REQUIRE(asSet(graph.getDependencies("users")) == std::set<std::string>{"transactions"});
    REQUIRE(asSet(graph.getDependencies("accounts")) == std::set<std::string>{"transactions"});
}

TEST_CASE("TableDependencyGraph Table A UPDATE/DELETE policy includes self and referencing tables", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet{"users.id", "users.email"};
    ColumnSet writeSet{"users.email", "orders.user_id"};

    REQUIRE(graph.addRelationship(readSet, writeSet));

    REQUIRE(asSet(graph.getDependencies("users")) == std::set<std::string>{"orders", "users"});
    REQUIRE(graph.isRelated("users", "users"));
    REQUIRE(graph.isRelated("users", "orders"));
    REQUIRE_FALSE(graph.isRelated("orders", "users"));
}

TEST_CASE("TableDependencyGraph Table A CREATE/ALTER policy uses FK reads", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet{"accounts.id"};
    ColumnSet writeSet{"transfers.id", "transfers.amount"};

    REQUIRE(graph.addRelationship(readSet, writeSet));
    REQUIRE(asSet(graph.getDependencies("accounts")) == std::set<std::string>{"transfers"});
}

TEST_CASE("TableDependencyGraph Table A DROP/TRUNCATE write-only set creates dependencies", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    ColumnSet readSet;
    ColumnSet writeSet{"accounts.id", "transactions.account_id"};

    REQUIRE(graph.addRelationship(readSet, writeSet));
    REQUIRE(asSet(graph.getDependencies("accounts")) ==
            std::set<std::string>{"accounts", "transactions"});
    REQUIRE(asSet(graph.getDependencies("transactions")) ==
            std::set<std::string>{"accounts", "transactions"});
    REQUIRE(graph.isRelated("accounts", "accounts"));
    REQUIRE(graph.isRelated("transactions", "transactions"));
}

TEST_CASE("TableDependencyGraph addRelationship from foreign keys uses current names", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    std::vector<ForeignKey> foreignKeys;

    ForeignKey fkOrders = makeFK("orders", "users");
    foreignKeys.emplace_back(fkOrders);

    ForeignKey fkPayments = makeFK("payments", "users");
    fkPayments.fromTable->addRenameHistory("invoices", 10);
    fkPayments.toTable->addRenameHistory("members", 10);
    foreignKeys.emplace_back(fkPayments);

    REQUIRE(graph.addRelationship(foreignKeys));
    REQUIRE(asSet(graph.getDependencies("orders")) == std::set<std::string>{"users"});
    REQUIRE(asSet(graph.getDependencies("invoices")) == std::set<std::string>{"members"});
}

TEST_CASE("TableDependencyGraph protobuf round-trip preserves dependencies", "[table-dependency-graph]") {
    TableDependencyGraph graph;
    graph.addRelationship("users", "orders");
    graph.addRelationship("orders", "payments");

    ultraverse::state::v2::proto::TableDependencyGraph protoGraph;
    graph.toProtobuf(&protoGraph);
    std::string payload;
    REQUIRE(protoGraph.SerializeToString(&payload));

    ultraverse::state::v2::proto::TableDependencyGraph restoredProto;
    REQUIRE(restoredProto.ParseFromString(payload));

    TableDependencyGraph restored;
    restored.fromProtobuf(restoredProto);

    REQUIRE(restored.isRelated("users", "orders"));
    REQUIRE(restored.isRelated("orders", "payments"));
    REQUIRE_FALSE(restored.isRelated("users", "payments"));
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
