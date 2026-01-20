#include <catch2/catch_test_macros.hpp>

#include <unordered_set>
#include <string>

#include "mariadb/state/new/StateChangeContext.hpp"
#include "mariadb/state/new/cluster/RowCluster.hpp"

#include "state_test_helpers.hpp"

using ultraverse::state::v2::ForeignKey;
using ultraverse::state::v2::NamingHistory;
using ultraverse::state::v2::RowCluster;
using ultraverse::state::v2::Transaction;
using ultraverse::state::v2::Query;
using ultraverse::state::v2::test_helpers::makeEq;

namespace {
ForeignKey makeFK(const std::string &fromTable, const std::string &fromColumn,
                  const std::string &toTable, const std::string &toColumn) {
    ForeignKey fk;
    fk.fromTable = std::make_shared<NamingHistory>(fromTable);
    fk.fromColumn = fromColumn;
    fk.toTable = std::make_shared<NamingHistory>(toTable);
    fk.toColumn = toColumn;
    return fk;
}
} // namespace

TEST_CASE("RowCluster merges intersecting ranges") {
    RowCluster cluster;
    cluster.addKey("users.id");

    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 1);
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 2);

    cluster.mergeCluster("users.id");

    auto &ranges = cluster.keyMap().at("users.id");
    REQUIRE(ranges.size() == 1);

    std::unordered_set<gid_t> gids(ranges.front().second.begin(), ranges.front().second.end());
    REQUIRE(gids.count(1) == 1);
    REQUIRE(gids.count(2) == 1);
}

TEST_CASE("RowCluster wildcard merges all ranges") {
    RowCluster cluster;
    cluster.addKey("users.id");

    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 1);
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(10), 2);
    cluster.setWildcard("users.id", true);

    cluster.mergeCluster("users.id");

    auto &ranges = cluster.keyMap().at("users.id");
    REQUIRE(ranges.size() == 1);

    auto where = ranges.front().first->MakeWhereQuery("users.id");
    REQUIRE(where.find("users.id=1") != std::string::npos);
    REQUIRE(where.find("users.id=10") != std::string::npos);
}

TEST_CASE("RowCluster resolves aliases and foreign keys") {
    RowCluster cluster;

    auto alias = makeEq("accounts.aid", 10);
    auto real = makeEq("users.id", 1);
    cluster.addAlias("accounts.aid", alias, real);

    const auto &resolved = RowCluster::resolveAlias(alias, cluster.aliasMap());
    REQUIRE(resolved.name == "users.id");
    REQUIRE(resolved.MakeRange2() == StateRange{1});
    REQUIRE(RowCluster::resolveAliasName(cluster.aliasMap(), "accounts.aid") == "users.id");

    std::vector<ForeignKey> foreignKeys;
    foreignKeys.emplace_back(makeFK("posts", "author", "users", "id"));
    foreignKeys.emplace_back(makeFK("users", "id", "accounts", "uid"));

    REQUIRE(RowCluster::resolveForeignKey("posts.author", foreignKeys) == "accounts.uid");
}

TEST_CASE("RowCluster detects related query via alias map") {
    RowCluster cluster;
    auto alias = makeEq("accounts.aid", 10);
    auto real = makeEq("users.id", 1);
    cluster.addAlias("accounts.aid", alias, real);

    Query query;
    query.readSet().push_back(alias);

    auto range = std::make_shared<StateRange>(1);
    REQUIRE(RowCluster::isQueryRelated("users.id", range, query, {}, cluster.aliasMap()));
}

TEST_CASE("RowCluster getKeyRangeOf2 matches gid list") {
    RowCluster cluster;
    cluster.addKey("users.id");
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 42);

    Transaction transaction;
    transaction.setGid(42);

    auto ranges = cluster.getKeyRangeOf2(transaction, "users.id", {});
    REQUIRE(ranges.size() == 1);
    REQUIRE(ranges.front().second.front() == 42);
}
