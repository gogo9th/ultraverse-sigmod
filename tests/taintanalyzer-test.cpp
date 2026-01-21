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

TEST_CASE("TaintAnalyzer isColumnRelated handles non-related and wildcard edge cases") {
    std::vector<ForeignKey> foreignKeys{
        makeForeignKey("posts", "author_id", "users", "id"),
        makeForeignKey("comments", "post_id", "posts", "id")
    };

    SECTION("same table different columns without wildcard are not related") {
        REQUIRE_FALSE(TaintAnalyzer::isColumnRelated("users.id", "users.name", foreignKeys));
    }

    SECTION("wildcard matches any column within the same table") {
        REQUIRE(TaintAnalyzer::isColumnRelated("users.*", "users.name", foreignKeys));
        REQUIRE(TaintAnalyzer::isColumnRelated("users.id", "users.*", foreignKeys));
    }

    SECTION("wildcard across tables without FK is not related") {
        REQUIRE_FALSE(TaintAnalyzer::isColumnRelated("users.*", "orders.id", foreignKeys));
        REQUIRE_FALSE(TaintAnalyzer::isColumnRelated("orders.*", "users.id", foreignKeys));
    }

    SECTION("wildcard across tables with FK matches the FK column") {
        REQUIRE(TaintAnalyzer::isColumnRelated("users.*", "posts.author_id", foreignKeys));
    }

    SECTION("wildcard across tables with FK does not match non-FK columns") {
        REQUIRE_FALSE(TaintAnalyzer::isColumnRelated("users.*", "posts.slug", foreignKeys));
    }

    SECTION("wildcard on both tables with FK is related") {
        REQUIRE(TaintAnalyzer::isColumnRelated("users.*", "posts.*", foreignKeys));
    }

    SECTION("case-insensitive input is normalized through FK resolution") {
        REQUIRE(TaintAnalyzer::isColumnRelated("USERS.ID", "posts.author_id", foreignKeys));
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

TEST_CASE("TaintAnalyzer columnSetsRelated handles FK chains and empty sets") {
    std::vector<ForeignKey> foreignKeys{
        makeForeignKey("posts", "author_id", "users", "id"),
        makeForeignKey("comments", "author_id", "posts", "author_id")
    };

    SECTION("foreign key chain matches through intermediate table") {
        ColumnSet taintedWrites{"users.id"};
        ColumnSet candidateColumns{"comments.author_id"};
        REQUIRE(TaintAnalyzer::columnSetsRelated(taintedWrites, candidateColumns, foreignKeys));
    }

    SECTION("wildcard in tainted writes still honors FK columns") {
        ColumnSet taintedWrites{"users.*"};
        ColumnSet candidateColumns{"posts.author_id"};
        REQUIRE(TaintAnalyzer::columnSetsRelated(taintedWrites, candidateColumns, foreignKeys));
    }

    SECTION("empty taint or candidate set returns false") {
        ColumnSet emptyWrites{};
        ColumnSet candidateColumns{"users.id"};
        REQUIRE_FALSE(TaintAnalyzer::columnSetsRelated(emptyWrites, candidateColumns, foreignKeys));

        ColumnSet taintedWrites{"users.id"};
        ColumnSet emptyCandidates{};
        REQUIRE_FALSE(TaintAnalyzer::columnSetsRelated(taintedWrites, emptyCandidates, foreignKeys));
    }
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

TEST_CASE("TaintAnalyzer collectColumnRW aggregates across queries and deduplicates") {
    auto txn = std::make_shared<Transaction>();
    txn->setGid(2);

    auto q1 = makeQuery("test", {makeEq("users.id", 1)}, {makeEq("posts.id", 1)});
    auto q2 = makeQuery("test", {makeEq("users.id", 2)}, {makeEq("posts.id", 2)});
    auto q3 = makeQuery("test", {makeEq("users.name", 3)}, {});

    *txn << q1;
    *txn << q2;
    *txn << q3;

    auto rw = TaintAnalyzer::collectColumnRW(*txn);

    REQUIRE(rw.read.size() == 2);
    REQUIRE(rw.read.count("users.id") == 1);
    REQUIRE(rw.read.count("users.name") == 1);

    REQUIRE(rw.write.size() == 1);
    REQUIRE(rw.write.count("posts.id") == 1);
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

TEST_CASE("TaintAnalyzer hasKeyColumnItems resolves alias and FK chains") {
    MockedRelationshipResolver resolver;
    resolver.addColumnAlias("posts.author", "users.handle");
    resolver.addForeignKey("users.handle", "users.id");

    StateCluster cluster({"users.id"});
    auto aliasFkTxn = makeTxn(4, "test", {makeEqStr("posts.author", "@alice")}, {});
    REQUIRE(TaintAnalyzer::hasKeyColumnItems(*aliasFkTxn, cluster, resolver));
}

TEST_CASE("TaintAnalyzer hasKeyColumnItems supports multiple key columns") {
    NoopRelationshipResolver resolver;
    StateCluster cluster({"users.id", "orders.id"});

    auto ordersTxn = makeTxn(5, "test", {makeEq("orders.id", 42)}, {});
    REQUIRE(TaintAnalyzer::hasKeyColumnItems(*ordersTxn, cluster, resolver));

    auto unrelatedTxn = makeTxn(6, "test", {makeEq("payments.id", 99)}, {});
    REQUIRE_FALSE(TaintAnalyzer::hasKeyColumnItems(*unrelatedTxn, cluster, resolver));
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

TEST_CASE("TaintAnalyzer hasKeyColumnItems ignores DDL but considers other queries") {
    NoopRelationshipResolver resolver;
    StateCluster cluster({"users.id"});

    auto txn = std::make_shared<Transaction>();
    txn->setGid(11);

    auto ddl = makeQuery("test", {makeEq("users.id", 1)}, {});
    ddl->setFlags(Query::FLAG_IS_DDL);
    auto readQuery = makeQuery("test", {makeEq("users.id", 2)}, {});

    *txn << ddl;
    *txn << readQuery;

    REQUIRE(TaintAnalyzer::hasKeyColumnItems(*txn, cluster, resolver));
}
