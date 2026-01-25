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

static bool hasOpItemNoData(const std::vector<StateItem> &items,
                            const std::string &name,
                            EN_FUNCTION_TYPE functionType) {
    for (const auto &item : items) {
        if (item.name == name && item.function_type == functionType && item.data_list.empty()) {
            return true;
        }
    }
    return false;
}

static bool hasOpItemWithValues(const std::vector<StateItem> &items,
                                const std::string &name,
                                EN_FUNCTION_TYPE functionType,
                                const std::vector<StateData> &values) {
    for (const auto &item : items) {
        if (item.name != name || item.function_type != functionType) {
            continue;
        }
        if (item.data_list.size() != values.size()) {
            continue;
        }
        bool matches = true;
        for (size_t i = 0; i < values.size(); ++i) {
            if (!(item.data_list[i] == values[i])) {
                matches = false;
                break;
            }
        }
        if (matches) {
            return true;
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

static const char *WHERE_RANGE_PROC = R"SQL(
CREATE PROCEDURE test_where_range(
    IN p_start INT,
    IN p_end INT
)
BEGIN
    UPDATE logs
    SET processed = 1
    WHERE id >= p_start AND id <= p_end;
END
)SQL";

static const char *WHERE_OPS_PROC = R"SQL(
CREATE PROCEDURE test_where_ops(
    IN p_status VARCHAR(32)
)
BEGIN
    UPDATE logs
    SET processed = 1
    WHERE status LIKE p_status
      AND code IN (1, 2, 3)
      AND kind NOT IN (4, 5);
END
)SQL";

static const char *WHERE_BETWEEN_PROC = R"SQL(
CREATE PROCEDURE test_where_between(
    IN p_start INT,
    IN p_end INT
)
BEGIN
    UPDATE logs
    SET processed = 1
    WHERE id BETWEEN p_start AND p_end;
END
)SQL";

static const char *DECLARE_DEFAULT_PROC = R"SQL(
CREATE PROCEDURE test_default()
BEGIN
    DECLARE v_limit INT DEFAULT 5;
    SELECT * FROM items WHERE id = v_limit;
END
)SQL";

static const char *LOCAL_SET_PROC = R"SQL(
CREATE PROCEDURE test_local_set()
BEGIN
    DECLARE v_id INT;
    SET v_id = 11;
    SELECT * FROM items WHERE id = v_id;
END
)SQL";

static const char *USER_VAR_CASE_PROC = R"SQL(
CREATE PROCEDURE test_user_var_case()
BEGIN
    SET @UserId = 9;
    SELECT * FROM users WHERE id = @userid;
END
)SQL";

static const char *SELECT_INTO_LOCAL_PROC = R"SQL(
CREATE PROCEDURE test_select_into_local()
BEGIN
    DECLARE v_id INT;
    SELECT id INTO v_id FROM users WHERE id = 1;
    SELECT * FROM items WHERE id = v_id;
END
)SQL";

static const char *BRANCH_UNION_PROC = R"SQL(
CREATE PROCEDURE test_branch_union(IN p_id INT)
BEGIN
    IF p_id > 0 THEN
        SELECT * FROM users WHERE id = p_id;
    ELSE
        INSERT INTO users(id, name) VALUES (p_id, 'bob');
    END IF;
END
)SQL";

static const char *MULTI_STMT_UNION_PROC = R"SQL(
CREATE PROCEDURE test_multi_stmt_union()
BEGIN
    UPDATE users SET id = 10 WHERE id = 1;
    DELETE FROM users WHERE id = 2;
END
)SQL";

static const char *MINISHOP_ADD_ITEM_PROC = R"SQL(
CREATE PROCEDURE add_item(
    IN p_id INT,
    IN p_name VARCHAR(64),
    IN p_price INT,
    IN p_stock INT
)
BEGIN
    INSERT INTO items(id, name, price, stock)
    VALUES (p_id, p_name, p_price, p_stock);
END
)SQL";

static const char *MINISHOP_RESTOCK_ITEM_PROC = R"SQL(
CREATE PROCEDURE restock_item(
    IN p_id INT,
    IN p_additional INT
)
BEGIN
    UPDATE items
    SET stock = stock + p_additional
    WHERE id = p_id;
END
)SQL";

static const char *MINISHOP_BUY_ITEM_PROC = R"SQL(
CREATE PROCEDURE buy_item(
    IN p_item_id INT,
    IN p_qty INT,
    OUT p_order_id BIGINT
)
BEGIN
    DECLARE v_price INT;

    SELECT price INTO v_price
    FROM items
    WHERE id = p_item_id;

    INSERT INTO orders(item_id, quantity, total_price, status)
    VALUES (p_item_id, p_qty, v_price * p_qty, 'PAID');

    SET p_order_id = LAST_INSERT_ID();

    UPDATE items
    SET stock = stock - p_qty
    WHERE id = p_item_id;
END
)SQL";

static const char *MINISHOP_REFUND_ITEM_PROC = R"SQL(
CREATE PROCEDURE refund_item(
    IN p_order_id BIGINT
)
BEGIN
    DECLARE v_item_id INT;
    DECLARE v_qty INT;
    DECLARE v_amount INT;

    SELECT item_id, quantity, total_price
    INTO v_item_id, v_qty, v_amount
    FROM orders
    WHERE order_id = p_order_id;

    UPDATE orders
    SET status = 'REFUNDED'
    WHERE order_id = p_order_id;

    UPDATE items
    SET stock = stock + v_qty
    WHERE id = v_item_id;

    INSERT INTO refunds(order_id, amount)
    VALUES (p_order_id, v_amount);
END
)SQL";

static const char *COLUMN_COMPARE_PROC = R"SQL(
CREATE PROCEDURE test_column_compare()
BEGIN
    SELECT * FROM customer2, frequent_flyer
    WHERE frequent_flyer.ff_c_id = customer2.c_id;
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

    REQUIRE(hasOpItemNoData(result.readSet, "accounts.user_id", FUNCTION_EQ));
}

TEST_CASE("ProcMatcher trace - SELECT INTO keeps known variable", "[procmatcher][trace]") {
    ProcMatcher matcher(SELECT_INTO_PROC);

    std::map<std::string, StateData> vars = {
        {"@result", StateData(int64_t{7})},
    };
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "accounts.user_id", StateData(int64_t{7})));
}

TEST_CASE("ProcMatcher trace - complex expression becomes wildcard", "[procmatcher][trace]") {
    ProcMatcher matcher(COMPLEX_EXPR_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(hasOpItemNoData(result.readSet, "items.id", FUNCTION_EQ));
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

TEST_CASE("ProcMatcher trace - DECLARE default value", "[procmatcher][trace]") {
    ProcMatcher matcher(DECLARE_DEFAULT_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "items.id", StateData(int64_t{5})));
}

TEST_CASE("ProcMatcher trace - local SET assigns known value", "[procmatcher][trace]") {
    ProcMatcher matcher(LOCAL_SET_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "items.id", StateData(int64_t{11})));
}

TEST_CASE("ProcMatcher trace - user variable name normalization", "[procmatcher][trace]") {
    ProcMatcher matcher(USER_VAR_CASE_PROC);

    std::map<std::string, StateData> vars;
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "users.id", StateData(int64_t{9})));
}

TEST_CASE("ProcMatcher trace - SELECT INTO keeps local variable hint", "[procmatcher][trace]") {
    ProcMatcher matcher(SELECT_INTO_LOCAL_PROC);

    std::map<std::string, StateData> vars = {
        {"v_id", StateData(int64_t{7})},
    };
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "items.id", StateData(int64_t{7})));
}

/**
 * Ensures IF/ELSE branches are both traced and their R/W sets are unioned
 * even when only one branch would execute at runtime.
 */
TEST_CASE("ProcMatcher trace - unions read/write across IF branches", "[procmatcher][trace]") {
    ProcMatcher matcher(BRANCH_UNION_PROC);

    std::map<std::string, StateData> vars = {
        {"p_id", StateData(int64_t{7})},
    };
    std::vector<std::string> keyColumns = {"users.id"};
    auto result = matcher.trace(vars, keyColumns);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "users.id", StateData(int64_t{7})));
    REQUIRE(hasEqItem(result.writeSet, "users.id", StateData(int64_t{7})));
}

/**
 * Ensures multiple statements in a procedure contribute to a single
 * unioned read/write set for dependency analysis.
 */
TEST_CASE("ProcMatcher trace - unions read/write across statements", "[procmatcher][trace]") {
    ProcMatcher matcher(MULTI_STMT_UNION_PROC);

    std::map<std::string, StateData> vars;
    std::vector<std::string> keyColumns = {"users.id"};
    auto result = matcher.trace(vars, keyColumns);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "users.id", StateData(int64_t{1})));
    REQUIRE(hasEqItem(result.readSet, "users.id", StateData(int64_t{2})));
    REQUIRE(hasEqItem(result.writeSet, "users.id", StateData(int64_t{10})));
    REQUIRE(hasEqItem(result.writeSet, "users.id", StateData(int64_t{2})));
}

TEST_CASE("ProcMatcher trace - minishop add_item", "[procmatcher][trace][minishop]") {
    ProcMatcher matcher(MINISHOP_ADD_ITEM_PROC);

    std::map<std::string, StateData> vars = {
        {"p_id", StateData(int64_t{1})},
        {"p_name", StateData(std::string("cola"))},
        {"p_price", StateData(int64_t{100})},
        {"p_stock", StateData(int64_t{10})},
    };
    std::vector<std::string> keyColumns = {"items.id", "orders.order_id"};
    auto result = matcher.trace(vars, keyColumns);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.writeSet, "items.id", StateData(int64_t{1})));
}

TEST_CASE("ProcMatcher trace - minishop restock_item", "[procmatcher][trace][minishop]") {
    ProcMatcher matcher(MINISHOP_RESTOCK_ITEM_PROC);

    std::map<std::string, StateData> vars = {
        {"p_id", StateData(int64_t{1})},
        {"p_additional", StateData(int64_t{5})},
    };
    std::vector<std::string> keyColumns = {"items.id", "orders.order_id"};
    auto result = matcher.trace(vars, keyColumns);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "items.id", StateData(int64_t{1})));
}

TEST_CASE("ProcMatcher trace - minishop buy_item", "[procmatcher][trace][minishop]") {
    ProcMatcher matcher(MINISHOP_BUY_ITEM_PROC);

    std::map<std::string, StateData> vars = {
        {"p_item_id", StateData(int64_t{1})},
        {"p_qty", StateData(int64_t{3})},
        {"p_order_id", StateData(int64_t{0})},
    };
    std::vector<std::string> keyColumns = {"items.id", "orders.order_id"};
    auto result = matcher.trace(vars, keyColumns);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "items.id", StateData(int64_t{1})));
}

TEST_CASE("ProcMatcher trace - minishop refund_item", "[procmatcher][trace][minishop]") {
    ProcMatcher matcher(MINISHOP_REFUND_ITEM_PROC);

    std::map<std::string, StateData> vars = {
        {"p_order_id", StateData(int64_t{42})},
        {"v_item_id", StateData(int64_t{36})},
        {"v_qty", StateData(int64_t{2})},
        {"v_amount", StateData(int64_t{36000})},
    };
    std::vector<std::string> keyColumns = {"items.id", "orders.order_id"};
    auto result = matcher.trace(vars, keyColumns);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "orders.order_id", StateData(int64_t{42})));
    REQUIRE(hasEqItem(result.readSet, "items.id", StateData(int64_t{36})));
}

TEST_CASE("ProcMatcher trace - minishop refund_item without hints", "[procmatcher][trace][minishop]") {
    ProcMatcher matcher(MINISHOP_REFUND_ITEM_PROC);

    std::map<std::string, StateData> vars = {
        {"p_order_id", StateData(int64_t{42})},
    };
    std::vector<std::string> keyColumns = {"items.id", "orders.order_id"};
    auto result = matcher.trace(vars, keyColumns);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasEqItem(result.readSet, "orders.order_id", StateData(int64_t{42})));
    REQUIRE(hasOpItemNoData(result.readSet, "items.id", FUNCTION_EQ));
}

TEST_CASE("ProcMatcher trace - range operators", "[procmatcher][trace]") {
    ProcMatcher matcher(WHERE_RANGE_PROC);

    std::map<std::string, StateData> vars = {
        {"p_start", StateData(int64_t{1})},
        {"p_end", StateData(int64_t{30})},
    };
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasOpItemWithValues(result.readSet, "logs.id", FUNCTION_GE, {StateData(int64_t{1})}));
    REQUIRE(hasOpItemWithValues(result.readSet, "logs.id", FUNCTION_LE, {StateData(int64_t{30})}));
}

TEST_CASE("ProcMatcher trace - IN/NOT IN/LIKE operators", "[procmatcher][trace]") {
    ProcMatcher matcher(WHERE_OPS_PROC);

    std::map<std::string, StateData> vars = {
        {"p_status", StateData(std::string("OK"))},
    };
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasOpItemWithValues(result.readSet, "logs.status", FUNCTION_EQ, {StateData(std::string("OK"))}));
    REQUIRE(hasOpItemWithValues(result.readSet, "logs.code", FUNCTION_EQ,
                                {StateData(int64_t{1}), StateData(int64_t{2}), StateData(int64_t{3})}));
    REQUIRE(hasOpItemWithValues(result.readSet, "logs.kind", FUNCTION_NE,
                                {StateData(int64_t{4}), StateData(int64_t{5})}));
}

TEST_CASE("ProcMatcher trace - BETWEEN operator (statelogd semantics)", "[procmatcher][trace]") {
    ProcMatcher matcher(WHERE_BETWEEN_PROC);

    std::map<std::string, StateData> vars = {
        {"p_start", StateData(int64_t{10})},
        {"p_end", StateData(int64_t{20})},
    };
    auto result = matcher.trace(vars);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasOpItemWithValues(result.readSet, "logs.id", FUNCTION_GE,
                                {StateData(int64_t{10})}));
    REQUIRE(hasOpItemWithValues(result.readSet, "logs.id", FUNCTION_LE,
                                {StateData(int64_t{20})}));
}

TEST_CASE("ProcMatcher trace - column-column comparison", "[procmatcher][trace]") {
    ProcMatcher matcher(COLUMN_COMPARE_PROC);

    std::map<std::string, StateData> vars;
    std::vector<std::string> keyColumns = {"customer2.c_id", "frequent_flyer.ff_c_id"};
    auto result = matcher.trace(vars, keyColumns);

    REQUIRE(result.unresolvedVars.empty());
    REQUIRE(hasWildcardItem(result.readSet, "customer2.c_id"));
    REQUIRE(hasWildcardItem(result.readSet, "frequent_flyer.ff_c_id"));
}
