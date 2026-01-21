#include <memory>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "../src/mariadb/state/new/analysis/TaintAnalyzer.hpp"
#include "../src/mariadb/state/new/cluster/StateCluster.hpp"
#include "state_test_helpers.hpp"

using namespace ultraverse::state::v2;
using namespace ultraverse::state::v2::analysis;
using namespace ultraverse::state::v2::test_helpers;

namespace {
    ForeignKey makeForeignKey(const std::string &fromTable,
                              const std::string &fromColumn,
                              const std::string &toTable,
                              const std::string &toColumn) {
        ForeignKey fk;
        fk.fromTable = std::make_shared<NamingHistory>(fromTable);
        fk.fromColumn = fromColumn;
        fk.toTable = std::make_shared<NamingHistory>(toTable);
        fk.toColumn = toColumn;
        return fk;
    }
}

TEST_CASE("TaintAnalyzer isColumnRelated resolves direct, FK, and wildcard") {
    std::vector<ForeignKey> foreignKeys{
        makeForeignKey("posts", "author_id", "users", "id"),
        makeForeignKey("comments", "author_id", "posts", "author_id")
    };

    SECTION("direct match") {
        REQUIRE(TaintAnalyzer::isColumnRelated("users.id", "users.id", foreignKeys));
    }

    SECTION("foreign key chain") {
        REQUIRE(TaintAnalyzer::isColumnRelated("comments.author_id", "users.id", foreignKeys));
    }

    SECTION("wildcard handling") {
        REQUIRE(TaintAnalyzer::isColumnRelated("users.*", "posts.id", foreignKeys));
    }
}

TEST_CASE("TaintAnalyzer columnSetsRelated detects related columns") {
    ColumnSet taintedWrites{"users.id"};
    ColumnSet candidateColumns{"posts.author_id"};
    std::vector<ForeignKey> foreignKeys{
        makeForeignKey("posts", "author_id", "users", "id")
    };

    REQUIRE(TaintAnalyzer::columnSetsRelated(taintedWrites, candidateColumns, foreignKeys));
}

TEST_CASE("TaintAnalyzer columnSetsRelated returns false for disjoint sets") {
    ColumnSet taintedWrites{"users.id"};
    ColumnSet candidateColumns{"orders.id"};
    std::vector<ForeignKey> foreignKeys{
        makeForeignKey("posts", "author_id", "users", "id")
    };

    REQUIRE_FALSE(TaintAnalyzer::columnSetsRelated(taintedWrites, candidateColumns, foreignKeys));
}

TEST_CASE("TaintAnalyzer collectColumnRW skips DDL and aggregates columns") {
    auto txn = std::make_shared<Transaction>();
    txn->setGid(1);

    auto q1 = makeQuery("test", {makeEq("users.id", 1)}, {makeEq("users.name", 1)});
    auto ddl = makeQuery("test", {makeEq("ddl.table", 1)}, {makeEq("ddl.column", 1)});
    ddl->setFlags(Query::FLAG_IS_DDL);
    auto q2 = makeQuery("test", {makeEq("posts.author_id", 1)}, {});

    *txn << q1;
    *txn << ddl;
    *txn << q2;

    auto rw = TaintAnalyzer::collectColumnRW(*txn);

    REQUIRE(rw.read.size() == 2);
    REQUIRE(rw.read.count("users.id") == 1);
    REQUIRE(rw.read.count("posts.author_id") == 1);
    REQUIRE(rw.read.count("ddl.table") == 0);

    REQUIRE(rw.write.size() == 1);
    REQUIRE(rw.write.count("users.name") == 1);
    REQUIRE(rw.write.count("ddl.column") == 0);
}

TEST_CASE("TaintAnalyzer hasKeyColumnItems detects key column items") {
    {
        NoopRelationshipResolver resolver;
        StateCluster cluster({"users.id"});

        auto readTxn = makeTxn(1, "test", {makeEq("users.id", 1)}, {});
        REQUIRE(TaintAnalyzer::hasKeyColumnItems(*readTxn, cluster, resolver));

        auto writeTxn = makeTxn(2, "test", {}, {makeEq("users.id", 2)});
        REQUIRE(TaintAnalyzer::hasKeyColumnItems(*writeTxn, cluster, resolver));
    }

    {
        MockedRelationshipResolver resolver;
        resolver.addForeignKey("posts.author_id", "users.id");

        StateCluster cluster({"users.id"});
        auto fkTxn = makeTxn(3, "test", {makeEq("posts.author_id", 1)}, {});
        REQUIRE(TaintAnalyzer::hasKeyColumnItems(*fkTxn, cluster, resolver));
    }
}

TEST_CASE("TaintAnalyzer hasKeyColumnItems ignores DDL queries") {
    NoopRelationshipResolver resolver;
    StateCluster cluster({"users.id"});

    auto txn = std::make_shared<Transaction>();
    txn->setGid(10);

    auto ddl = makeQuery("test", {makeEq("users.id", 1)}, {});
    ddl->setFlags(Query::FLAG_IS_DDL);
    *txn << ddl;

    REQUIRE_FALSE(TaintAnalyzer::hasKeyColumnItems(*txn, cluster, resolver));
}
