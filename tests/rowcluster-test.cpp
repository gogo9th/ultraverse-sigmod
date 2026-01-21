#include <catch2/catch_test_macros.hpp>

#include <initializer_list>
#include <unordered_set>
#include <string>
#include <vector>

#include "mariadb/state/new/StateChangeContext.hpp"
#include "mariadb/state/new/cluster/RowCluster.hpp"

#include "state_test_helpers.hpp"

using ultraverse::state::v2::ForeignKey;
using ultraverse::state::v2::NamingHistory;
using ultraverse::state::v2::RowCluster;
using ultraverse::state::v2::Transaction;
using ultraverse::state::v2::Query;
using ultraverse::state::v2::test_helpers::makeEq;
using ultraverse::state::v2::test_helpers::makeBetween;
using ultraverse::state::v2::test_helpers::makeEqStr;
using ultraverse::state::v2::test_helpers::makeQuery;

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

std::shared_ptr<StateRange> makeRangeBetween(int64_t begin, int64_t end) {
    auto range = std::make_shared<StateRange>();
    range->SetBetween(StateData{begin}, StateData{end});
    return range;
}

StateItem makeCondition(EN_CONDITION_TYPE type, std::vector<StateItem> items) {
    StateItem item;
    item.condition_type = type;
    item.arg_list = std::move(items);
    return item;
}

StateItem makeIn(const std::string &name, std::initializer_list<int64_t> values) {
    StateItem item;
    item.name = name;
    item.function_type = FUNCTION_IN_INTERNAL;
    for (auto value : values) {
        item.data_list.emplace_back(StateData{value});
    }
    return item;
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
    REQUIRE(RowCluster::isQueryRelated("users.id", *range, query, {}, cluster.aliasMap()));
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

TEST_CASE("RowCluster merges transitive intersecting ranges") {
    RowCluster cluster;
    cluster.addKey("users.id");

    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 1);
    cluster.addKeyRange("users.id", makeRangeBetween(1, 2), 2);
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(2), 3);

    cluster.mergeCluster("users.id");

    auto &ranges = cluster.keyMap().at("users.id");
    REQUIRE(ranges.size() == 1);

    std::unordered_set<gid_t> gids(ranges.front().second.begin(), ranges.front().second.end());
    REQUIRE(gids.count(1) == 1);
    REQUIRE(gids.count(2) == 1);
    REQUIRE(gids.count(3) == 1);
}

TEST_CASE("RowCluster keeps disjoint ranges separate") {
    RowCluster cluster;
    cluster.addKey("users.id");

    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 1);
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(10), 2);
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(20), 3);

    cluster.mergeCluster("users.id");

    auto &ranges = cluster.keyMap().at("users.id");
    REQUIRE(ranges.size() == 3);

    std::unordered_set<gid_t> gids;
    for (const auto &pair : ranges) {
        REQUIRE(pair.second.size() == 1);
        gids.insert(pair.second.front());
    }
    REQUIRE(gids.count(1) == 1);
    REQUIRE(gids.count(2) == 1);
    REQUIRE(gids.count(3) == 1);
}

TEST_CASE("RowCluster wildcard only affects target key") {
    RowCluster cluster;
    cluster.addKey("users.id");
    cluster.addKey("posts.id");

    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 1);
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(10), 2);
    cluster.addKeyRange("posts.id", std::make_shared<StateRange>(7), 3);
    cluster.addKeyRange("posts.id", std::make_shared<StateRange>(9), 4);

    cluster.setWildcard("users.id", true);
    cluster.mergeCluster("users.id");
    cluster.mergeCluster("posts.id");

    REQUIRE(cluster.keyMap().at("users.id").size() == 1);
    REQUIRE(cluster.keyMap().at("posts.id").size() == 2);
}

TEST_CASE("RowCluster resolveForeignKey follows chain and normalizes case") {
    std::vector<ForeignKey> foreignKeys;
    foreignKeys.emplace_back(makeFK("posts", "author", "users", "uid"));
    foreignKeys.emplace_back(makeFK("users", "uid", "accounts", "user_id"));

    REQUIRE(RowCluster::resolveForeignKey("Posts.Author", foreignKeys) == "accounts.user_id");
}

TEST_CASE("RowCluster detects related query via foreign key chain") {
    RowCluster cluster;
    cluster.addKey("accounts.user_id");

    auto range = std::make_shared<StateRange>(5);

    Query query;
    query.readSet().push_back(makeEq("posts.author", 5));

    std::vector<ForeignKey> foreignKeys;
    foreignKeys.emplace_back(makeFK("posts", "author", "users", "uid"));
    foreignKeys.emplace_back(makeFK("users", "uid", "accounts", "user_id"));

    REQUIRE(RowCluster::isQueryRelated("accounts.user_id", *range, query, foreignKeys, cluster.aliasMap()));
}

TEST_CASE("RowCluster detects related query via write set") {
    RowCluster cluster;
    auto alias = makeEq("accounts.aid", 10);
    auto real = makeEq("users.uid", 5);
    cluster.addAlias("accounts.aid", alias, real);

    Query query;
    query.writeSet().push_back(alias);

    auto range = std::make_shared<StateRange>(5);
    REQUIRE(RowCluster::isQueryRelated("users.uid", *range, query, {}, cluster.aliasMap()));
}

TEST_CASE("RowCluster ignores alias mapping when value does not match") {
    RowCluster cluster;
    auto alias = makeEq("accounts.aid", 10);
    auto real = makeEq("users.uid", 5);
    cluster.addAlias("accounts.aid", alias, real);

    Query query;
    query.readSet().push_back(makeEq("accounts.aid", 11));

    auto range = std::make_shared<StateRange>(5);
    REQUIRE_FALSE(RowCluster::isQueryRelated("users.uid", *range, query, {}, cluster.aliasMap()));
}

TEST_CASE("RowCluster handles OR expressions with mixed columns") {
    RowCluster cluster;

    Query query;
    auto orExpr = makeCondition(
        EN_CONDITION_OR,
        {makeEq("posts.id", 1), makeEq("users.id", 2)}
    );
    query.readSet().push_back(orExpr);

    auto matching = std::make_shared<StateRange>(2);
    auto nonMatching = std::make_shared<StateRange>(3);

    REQUIRE(RowCluster::isQueryRelated("users.id", *matching, query, {}, cluster.aliasMap()));
    REQUIRE_FALSE(RowCluster::isQueryRelated("users.id", *nonMatching, query, {}, cluster.aliasMap()));
}

TEST_CASE("RowCluster handles BETWEEN expressions") {
    RowCluster cluster;

    Query query;
    query.readSet().push_back(makeBetween("users.id", 10, 20));

    auto inside = std::make_shared<StateRange>(15);
    auto outside = std::make_shared<StateRange>(25);

    REQUIRE(RowCluster::isQueryRelated("users.id", *inside, query, {}, cluster.aliasMap()));
    REQUIRE_FALSE(RowCluster::isQueryRelated("users.id", *outside, query, {}, cluster.aliasMap()));
}

TEST_CASE("RowCluster getKeyRangeOf respects query content") {
    RowCluster cluster;
    cluster.addKey("users.id");
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 101);

    Transaction transaction;
    transaction.setGid(101);

    auto query = makeQuery("db", {makeEq("users.id", 1)}, {});
    transaction << query;

    auto ranges = cluster.getKeyRangeOf(transaction, "users.id", {});
    REQUIRE(ranges.size() == 1);
    REQUIRE(ranges.front().second.front() == 101);
}

TEST_CASE("RowCluster getKeyRangeOf2 ignores unrelated gid") {
    RowCluster cluster;
    cluster.addKey("users.id");
    cluster.addKeyRange("users.id", std::make_shared<StateRange>(1), 7);

    Transaction transaction;
    transaction.setGid(99);

    auto ranges = cluster.getKeyRangeOf2(transaction, "users.id", {});
    REQUIRE(ranges.empty());
}

TEST_CASE("RowCluster detects string alias mapping") {
    RowCluster cluster;
    auto alias = makeEqStr("users.handle", "alice");
    auto real = makeEq("users.id", 1);
    cluster.addAlias("users.handle", alias, real);

    Query query;
    query.readSet().push_back(makeEqStr("users.handle", "alice"));

    auto range = std::make_shared<StateRange>(1);
    REQUIRE(RowCluster::isQueryRelated("users.id", *range, query, {}, cluster.aliasMap()));
}

TEST_CASE("RowCluster handles IN expressions") {
    RowCluster cluster;

    Query query;
    query.readSet().push_back(makeIn("users.id", {1, 2, 3}));

    auto matching = std::make_shared<StateRange>(2);
    auto nonMatching = std::make_shared<StateRange>(4);

    REQUIRE(RowCluster::isQueryRelated("users.id", *matching, query, {}, cluster.aliasMap()));
    REQUIRE_FALSE(RowCluster::isQueryRelated("users.id", *nonMatching, query, {}, cluster.aliasMap()));
}

TEST_CASE("RowCluster infers implicit foreign keys via naming") {
    RowCluster cluster;

    Query query;
    query.readSet().push_back(makeEq("orders.user_id", 7));

    auto range = std::make_shared<StateRange>(7);
    std::unordered_set<std::string> implicitTables{"users"};

    REQUIRE(RowCluster::isQueryRelated("users.id", *range, query, {}, cluster.aliasMap(), &implicitTables));
}

TEST_CASE("RowCluster resolves variable cluster keys by coercion") {
    RowCluster cluster;
    auto alias = makeEqStr("users.uid_str", "000042");
    auto real = makeEq("users.id", 42);
    cluster.addAlias("users.uid_str", alias, real);

    Query query;
    query.readSet().push_back(makeEqStr("users.uid_str", "000043"));

    auto range = std::make_shared<StateRange>(43);
    REQUIRE(RowCluster::isQueryRelated("users.id", *range, query, {}, cluster.aliasMap()));
}

TEST_CASE("RowCluster matches multi-dimensional cluster keys") {
    RowCluster cluster;
    std::vector<std::string> keyColumns{"orders.product_id", "orders.user_id"};
    RowCluster::CompositeRange ranges {
            {
                StateRange { 2 },
                StateRange { 1 }
            }
    };
    cluster.addCompositeKeyRange(keyColumns, ranges, 100);
    cluster.mergeCompositeCluster(keyColumns);

    REQUIRE(cluster.compositeKeyMap().size() == 1);
    auto &clusterEntry = *cluster.compositeKeyMap().begin();
    REQUIRE(clusterEntry.second.size() == 1);

    Query matching;
    matching.readSet().push_back(makeEq("orders.user_id", 1));
    matching.readSet().push_back(makeEq("orders.product_id", 2));

    Query partial;
    partial.readSet().push_back(makeEq("orders.user_id", 1));

    const auto &storedRange = clusterEntry.second.front().first;
    REQUIRE(RowCluster::isQueryRelatedComposite(keyColumns, storedRange, matching, {}, cluster.aliasMap()));
    REQUIRE_FALSE(RowCluster::isQueryRelatedComposite(keyColumns, storedRange, partial, {}, cluster.aliasMap()));
}
