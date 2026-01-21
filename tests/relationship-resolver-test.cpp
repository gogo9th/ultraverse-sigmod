#include <string>

#include <catch2/catch_test_macros.hpp>

#include "../src/mariadb/state/new/StateChangePlan.hpp"
#include "../src/mariadb/state/new/StateChangeContext.hpp"
#include "../src/mariadb/state/new/cluster/NamingHistory.hpp"

#include "state_test_helpers.hpp"

using namespace ultraverse::state::v2;
using namespace ultraverse::state::v2::test_helpers;

TEST_CASE("RelationshipResolver resolveChain handles alias and FK mapping (mocked)") {
    MockedRelationshipResolver resolver;

    resolver.addColumnAlias("posts.uuid", "posts.id");
    resolver.addForeignKey("posts.author", "users.id");
    resolver.addColumnAlias("posts.author_name", "posts.author");

    REQUIRE(resolver.resolveChain("posts.uuid") == "posts.id");
    REQUIRE(resolver.resolveChain("posts.author") == "users.id");
    REQUIRE(resolver.resolveChain("posts.author_name") == "users.id");

    resolver.addColumnAlias("a", "b");
    resolver.addColumnAlias("b", "c");
    REQUIRE(resolver.resolveChain("a") == "b");

    REQUIRE(resolver.resolveChain("unknown.col").empty());
}

TEST_CASE("RelationshipResolver resolveRowChain maps row alias and FK chain") {
    MockedRelationshipResolver resolver;

    resolver.addRowAlias(
        makeEqStr("posts.uuid", "uuid-1"),
        makeEq("posts.id", 1)
    );

    auto resolved = resolver.resolveRowChain(makeEqStr("posts.uuid", "uuid-1"));
    REQUIRE(resolved != nullptr);
    REQUIRE(resolved->name == "posts.id");
    REQUIRE(resolved->MakeRange2() == StateRange{1});

    resolver.addRowAlias(
        makeEqStr("posts.author_str", "alice"),
        makeEq("posts.author", 1)
    );
    resolver.addForeignKey("posts.author", "users.id");

    auto chained = resolver.resolveRowChain(makeEqStr("posts.author_str", "alice"));
    REQUIRE(chained != nullptr);
    REQUIRE(chained->name == "users.id");
    REQUIRE(chained->MakeRange2() == StateRange{1});

    auto missing = resolver.resolveRowChain(makeEqStr("posts.uuid", "missing"));
    REQUIRE(missing == nullptr);
}

TEST_CASE("RelationshipResolver guards against alias/FK cycles") {
    MockedRelationshipResolver resolver;
    resolver.addColumnAlias("a", "b");
    resolver.addForeignKey("b", "a");

    REQUIRE(resolver.resolveChain("a").empty());

    resolver.addRowAlias(makeEq("a", 1), makeEq("b", 1));
    auto resolved = resolver.resolveRowChain(makeEq("a", 1));
    REQUIRE(resolved == nullptr);
}

TEST_CASE("StateRelationshipResolver resolves alias chains and FK mapping") {
    StateChangePlan plan;
    plan.columnAliases().push_back({"posts.author_name", "posts.author"});
    plan.columnAliases().push_back({"a", "b"});
    plan.columnAliases().push_back({"b", "c"});

    StateChangeContext context;
    auto posts = std::make_shared<NamingHistory>("posts");
    auto users = std::make_shared<NamingHistory>("users");
    context.tables = {posts, users};
    context.foreignKeys.push_back(ForeignKey{posts, "author", users, "id"});

    StateRelationshipResolver resolver(plan, context);

    REQUIRE(resolver.resolveColumnAlias("posts.author_name") == "posts.author");
    REQUIRE(resolver.resolveColumnAlias("A") == "c");
    REQUIRE(resolver.resolveChain("posts.author_name") == "users.id");
    REQUIRE(resolver.resolveChain("unknown.col").empty());
}

TEST_CASE("StateRelationshipResolver resolves FK + alias chain for cluster key propagation") {
    StateChangePlan plan;
    plan.columnAliases().push_back({"accounts.aid", "accounts.uid"});
    plan.columnAliases().push_back({"v_statements.aid", "statements.aid"});

    StateChangeContext context;
    auto users = std::make_shared<NamingHistory>("users");
    auto accounts = std::make_shared<NamingHistory>("accounts");
    auto statements = std::make_shared<NamingHistory>("statements");
    context.tables = {users, accounts, statements};
    context.foreignKeys.push_back(ForeignKey{accounts, "uid", users, "uid"});
    context.foreignKeys.push_back(ForeignKey{statements, "aid", accounts, "aid"});

    StateRelationshipResolver resolver(plan, context);

    REQUIRE(resolver.resolveChain("statements.aid") == "users.uid");
    REQUIRE(resolver.resolveChain("Statements.AID") == "users.uid");
    REQUIRE(resolver.resolveChain("accounts.aid") == "users.uid");
    REQUIRE(resolver.resolveChain("v_statements.aid") == "users.uid");
}

TEST_CASE("StateRelationshipResolver detects alias cycles") {
    StateChangePlan plan;
    plan.columnAliases().push_back({"a", "b"});
    plan.columnAliases().push_back({"b", "a"});

    StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);

    REQUIRE(resolver.resolveColumnAlias("a").empty());
}

TEST_CASE("StateRelationshipResolver detects foreign key cycles") {
    StateChangePlan plan;
    StateChangeContext context;

    auto t1 = std::make_shared<NamingHistory>("t1");
    auto t2 = std::make_shared<NamingHistory>("t2");
    context.tables = {t1, t2};
    context.foreignKeys.push_back(ForeignKey{t1, "id", t2, "id"});
    context.foreignKeys.push_back(ForeignKey{t2, "id", t1, "id"});

    StateRelationshipResolver resolver(plan, context);

    REQUIRE(resolver.resolveForeignKey("t1.id").empty());
}

TEST_CASE("StateRelationshipResolver addTransaction builds row alias mapping") {
    StateChangePlan plan;
    plan.columnAliases().push_back({"users.id_str", "users.id"});

    StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);

    auto txn = makeTxn(
        1,
        "test",
        {},
        {makeEqStr("users.id_str", "0001"), makeEq("users.id", 1)}
    );

    resolver.addTransaction(*txn);

    auto resolved = resolver.resolveRowAlias(makeEqStr("users.id_str", "0001"));
    REQUIRE(resolved != nullptr);
    REQUIRE(resolved->name == "users.id");
    REQUIRE(resolved->MakeRange2() == StateRange{1});
}

TEST_CASE("StateRelationshipResolver resolveRowChain follows FK even without row alias") {
    StateChangePlan plan;
    StateChangeContext context;
    auto posts = std::make_shared<NamingHistory>("posts");
    auto users = std::make_shared<NamingHistory>("users");
    context.tables = {posts, users};
    context.foreignKeys.push_back(ForeignKey{posts, "author", users, "id"});

    StateRelationshipResolver resolver(plan, context);

    auto resolved = resolver.resolveRowChain(makeEq("posts.author", 1));
    REQUIRE(resolved != nullptr);
    REQUIRE(resolved->name == "users.id");
    REQUIRE(resolved->MakeRange2() == StateRange{1});
}

TEST_CASE("StateRelationshipResolver resolveRowChain follows alias + FK chain") {
    StateChangePlan plan;
    plan.columnAliases().push_back({"accounts.aid", "accounts.uid"});

    StateChangeContext context;
    auto users = std::make_shared<NamingHistory>("users");
    auto accounts = std::make_shared<NamingHistory>("accounts");
    auto statements = std::make_shared<NamingHistory>("statements");
    context.tables = {users, accounts, statements};
    context.foreignKeys.push_back(ForeignKey{accounts, "uid", users, "uid"});
    context.foreignKeys.push_back(ForeignKey{statements, "aid", accounts, "aid"});

    StateRelationshipResolver resolver(plan, context);

    auto txn = makeTxn(
        1,
        "test",
        {},
        {makeEq("accounts.aid", 3), makeEq("accounts.uid", 42)}
    );
    resolver.addTransaction(*txn);

    auto resolved = resolver.resolveRowChain(makeEq("statements.aid", 3));
    REQUIRE(resolved != nullptr);
    REQUIRE(resolved->name == "users.uid");
    REQUIRE(resolved->MakeRange2() == StateRange{42});
}

TEST_CASE("CachedRelationshipResolver returns consistent results") {
    MockedRelationshipResolver resolver;
    resolver.addColumnAlias("posts.uuid", "posts.id");
    resolver.addForeignKey("posts.author", "users.id");
    resolver.addRowAlias(
        makeEqStr("posts.uuid", "uuid-1"),
        makeEq("posts.id", 1)
    );

    CachedRelationshipResolver cached(resolver, 4);

    REQUIRE(cached.resolveColumnAlias("posts.uuid") == "posts.id");
    REQUIRE(cached.resolveChain("posts.author") == "users.id");

    auto resolved1 = cached.resolveRowAlias(makeEqStr("posts.uuid", "uuid-1"));
    REQUIRE(resolved1 != nullptr);
    REQUIRE(resolved1->name == "posts.id");
    REQUIRE(resolved1->MakeRange2() == StateRange{1});

    auto resolved2 = cached.resolveRowChain(makeEqStr("posts.uuid", "uuid-1"));
    REQUIRE(resolved2 != nullptr);
    REQUIRE(resolved2->name == "posts.id");

    cached.clearCache();
    REQUIRE(cached.resolveChain("posts.author") == "users.id");

    REQUIRE(cached.resolveChain("unknown.col").empty());
}

TEST_CASE("CachedRelationshipResolver separates row alias and row chain caches") {
    MockedRelationshipResolver resolver;
    resolver.addRowAlias(
        makeEqStr("posts.author_str", "alice"),
        makeEq("posts.author", 1)
    );
    resolver.addForeignKey("posts.author", "users.id");

    CachedRelationshipResolver cached(resolver, 4);

    auto alias = cached.resolveRowAlias(makeEqStr("posts.author_str", "alice"));
    REQUIRE(alias != nullptr);
    REQUIRE(alias->name == "posts.author");

    auto chained = cached.resolveRowChain(makeEqStr("posts.author_str", "alice"));
    REQUIRE(chained != nullptr);
    REQUIRE(chained->name == "users.id");
}

TEST_CASE("StateRelationshipResolver resolves aliases case-insensitively") {
    StateChangePlan plan;
    plan.columnAliases().push_back({"users.id_str", "users.id"});

    StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);

    REQUIRE(resolver.resolveColumnAlias("Users.ID_Str") == "users.id");
    REQUIRE(resolver.resolveChain("Users.ID_Str") == "users.id");
}

TEST_CASE("StateRelationshipResolver resolves foreign keys case-insensitively") {
    StateChangePlan plan;
    StateChangeContext context;

    auto posts = std::make_shared<NamingHistory>("Posts");
    auto users = std::make_shared<NamingHistory>("Users");
    context.tables = {posts, users};
    context.foreignKeys.push_back(ForeignKey{posts, "Author", users, "ID"});

    StateRelationshipResolver resolver(plan, context);

    REQUIRE(resolver.resolveForeignKey("posts.author") == "users.id");
    REQUIRE(resolver.resolveChain("POSTS.AUTHOR") == "users.id");
}

TEST_CASE("StateRelationshipResolver addTransaction ignores incomplete alias mapping") {
    StateChangePlan plan;
    plan.columnAliases().push_back({"users.id_str", "users.id"});

    StateChangeContext context;
    StateRelationshipResolver resolver(plan, context);

    auto txn = makeTxn(1, "test", {}, {makeEqStr("users.id_str", "0001")});
    resolver.addTransaction(*txn);

    auto resolved = resolver.resolveRowAlias(makeEqStr("users.id_str", "0001"));
    REQUIRE(resolved == nullptr);
}
