#include <catch2/catch_test_macros.hpp>

#include "mariadb/state/new/cluster/NamingHistory.hpp"

using ultraverse::state::v2::NamingHistory;

TEST_CASE("NamingHistory returns names by timestamp ordering") {
    NamingHistory history("users");
    history.addRenameHistory("users_v2", 200);
    history.addRenameHistory("users_v1", 100);

    REQUIRE(history.getName(50) == "users");
    REQUIRE(history.getName(150) == "users_v1");
    REQUIRE(history.getName(250) == "users_v2");
    REQUIRE(history.match("users_v1", 150));
    REQUIRE_FALSE(history.match("users_v2", 150));
}

TEST_CASE("NamingHistory current name is latest rename") {
    NamingHistory history("users");
    history.addRenameHistory("users_v2", 200);
    history.addRenameHistory("users_v3", 300);

    REQUIRE(history.getCurrentName() == "users_v3");
}
