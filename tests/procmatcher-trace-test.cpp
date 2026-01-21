#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "mariadb/state/new/ProcMatcher.hpp"
#include "mariadb/state/StateItem.h"

using namespace ultraverse::state::v2;

static bool hasEqItem(const std::vector<StateItem> &items,
                      const std::string &name,
                      const StateData &data) {
    for (const auto &item : items) {
        if (item.name == name && item.function_type == FUNCTION_EQ && !item.data_list.empty()) {
            if (item.data_list.front() == data) {
                return true;
            }
        }
    }
    return false;
}

static bool hasWildcardItem(const std::vector<StateItem> &items,
                            const std::string &name) {
    for (const auto &item : items) {
        if (item.name == name && item.function_type == FUNCTION_WILDCARD) {
            return true;
        }
    }
    return false;
}

static const char *BASIC_PROC = R"SQL(
CREATE PROCEDURE test_basic()
BEGIN
    SET @x = 1;
    SELECT * FROM users WHERE id = @x;
END
)SQL";

static const char *SELECT_INTO_PROC = R"SQL(
CREATE PROCEDURE test_select_into()
BEGIN
    SELECT id INTO @result FROM users;
    UPDATE accounts SET v = 1 WHERE user_id = @result;
END
)SQL";

static const char *COMPLEX_EXPR_PROC = R"SQL(
CREATE PROCEDURE test_complex()
BEGIN
    SET @a = 1;
    SET @b = 2;
    SET @x = CONCAT(@a, @b);
    SELECT * FROM items WHERE id = @x;
END
)SQL";

static const char *ARITHMETIC_PROC = R"SQL(
CREATE PROCEDURE test_arithmetic()
BEGIN
    SET @a = 10;
    SET @b = 3;
    SET @sum = @a + @b;
    SELECT * FROM items WHERE id = @sum;
END
)SQL";

static const char *UNDEFINED_PARAM_PROC = R"SQL(
CREATE PROCEDURE test_param(IN user_id INT)
BEGIN
    SELECT * FROM users WHERE id = user_id;
END
)SQL";

TEST_CASE("ProcMatcher trace - basic variable tracking", "[procmatcher][trace]") {
    ProcMatcher matcher(BASIC_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(!result.readSet.empty());
    REQUIRE(hasEqItem(result.readSet, "users.id", StateData(int64_t{1})));
}

TEST_CASE("ProcMatcher trace - SELECT INTO marks variable unknown", "[procmatcher][trace]") {
    ProcMatcher matcher(SELECT_INTO_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(hasWildcardItem(result.readSet, "accounts.user_id"));
}

TEST_CASE("ProcMatcher trace - complex expression becomes wildcard", "[procmatcher][trace]") {
    ProcMatcher matcher(COMPLEX_EXPR_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(hasWildcardItem(result.readSet, "items.id"));
}

TEST_CASE("ProcMatcher trace - arithmetic with known variables", "[procmatcher][trace]") {
    ProcMatcher matcher(ARITHMETIC_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(hasEqItem(result.readSet, "items.id", StateData(int64_t{13})));
}

TEST_CASE("ProcMatcher trace - undefined variable in unresolvedVars", "[procmatcher][trace]") {
    ProcMatcher matcher(UNDEFINED_PARAM_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.size() == 1);
    REQUIRE(result.unresolvedVars.front() == "user_id");
}
