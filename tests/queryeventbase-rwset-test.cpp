#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "../src/mariadb/DBEvent.hpp"
#include "../src/mariadb/state/StateItem.h"

using ultraverse::mariadb::QueryEvent;

namespace {
    struct ParsedColumns {
        std::unique_ptr<QueryEvent> event;
        std::set<std::string> readColumns;
        std::set<std::string> writeColumns;
    };

    ParsedColumns parseColumns(const std::string &sql) {
        auto event = std::make_unique<QueryEvent>("testdb", sql, 0);
        REQUIRE(event->parse());

        std::set<std::string> readColumns;
        std::set<std::string> writeColumns;
        event->columnRWSet(readColumns, writeColumns);

        return ParsedColumns{
            std::move(event),
            std::move(readColumns),
            std::move(writeColumns)
        };
    }

    const StateItem *findItem(const std::vector<StateItem> &items, const std::string &name) {
        auto it = std::find_if(items.begin(), items.end(), [&name](const StateItem &item) {
            return item.name == name;
        });
        if (it == items.end()) {
            return nullptr;
        }
        return &(*it);
    }
}

TEST_CASE("QueryEventBase columnRWSet SELECT collects selected and where columns") {
    auto parsed = parseColumns("SELECT id, name FROM users WHERE id = 42 AND status = 'active';");

    REQUIRE(parsed.readColumns.count("users.id") == 1);
    REQUIRE(parsed.readColumns.count("users.name") == 1);
    REQUIRE(parsed.readColumns.count("users.status") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet SELECT handles qualified columns") {
    auto parsed = parseColumns("SELECT posts.title FROM posts WHERE posts.author_id = 1;");

    REQUIRE(parsed.readColumns.count("posts.title") == 1);
    REQUIRE(parsed.readColumns.count("posts.author_id") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet SELECT with JOIN captures referenced columns") {
    auto parsed = parseColumns(
        "SELECT users.id, posts.title "
        "FROM users JOIN posts ON posts.author_id = users.id "
        "WHERE posts.status = 'published' OR users.active = 1;"
    );

    REQUIRE(parsed.readColumns.count("users.id") == 1);
    REQUIRE(parsed.readColumns.count("posts.title") == 1);
    REQUIRE(parsed.readColumns.count("posts.status") == 1);
    REQUIRE(parsed.readColumns.count("users.active") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet INSERT adds table wildcard and columns") {
    auto parsed = parseColumns("INSERT INTO users (id, name) VALUES (1, 'alice');");

    REQUIRE(parsed.readColumns.empty());
    REQUIRE(parsed.writeColumns.count("users.id") == 1);
    REQUIRE(parsed.writeColumns.count("users.name") == 1);
    REQUIRE(parsed.writeColumns.count("users.*") == 1);
}

TEST_CASE("QueryEventBase columnRWSet UPDATE uses write columns and where columns") {
    auto parsed = parseColumns(
        "UPDATE users SET name = 'bob', age = age + 1 WHERE id = 1 AND status = 'active';"
    );

    REQUIRE(parsed.writeColumns.count("users.name") == 1);
    REQUIRE(parsed.writeColumns.count("users.age") == 1);

    REQUIRE(parsed.readColumns.count("users.id") == 1);
    REQUIRE(parsed.readColumns.count("users.status") == 1);

    REQUIRE(parsed.readColumns.count("users.age") == 1);
}

/**
 * Ensures UPDATE RHS expressions contribute to the read set, which is required
 * for dependency analysis of self-referential updates.
 */
TEST_CASE("QueryEventBase columnRWSet UPDATE includes RHS column reads") {
    auto parsed = parseColumns("UPDATE users SET age = age + 1 WHERE id = 1;");

    REQUIRE(parsed.writeColumns.count("users.age") == 1);
    REQUIRE(parsed.readColumns.count("users.id") == 1);
    REQUIRE(parsed.readColumns.count("users.age") == 1);
}

TEST_CASE("QueryEventBase columnRWSet DELETE adds wildcard write and where read") {
    auto parsed = parseColumns("DELETE FROM users WHERE id = 7;");

    REQUIRE(parsed.writeColumns.count("users.*") == 1);
    REQUIRE(parsed.readColumns.count("users.id") == 1);
}

/**
 * Ensures DML read sets include columns from external tables referenced in
 * subqueries (e.g., FK target lookups) so dependencies are not missed.
 */
TEST_CASE("QueryEventBase columnRWSet DELETE with EXISTS subquery collects external columns") {
    auto parsed = parseColumns(
        "DELETE FROM orders "
        "WHERE EXISTS ("
        "  SELECT 1 FROM users "
        "  WHERE users.id = orders.user_id AND users.active = 1"
        ");"
    );

    REQUIRE(parsed.writeColumns.count("orders.*") == 1);
    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
    REQUIRE(parsed.readColumns.count("users.id") == 1);
    REQUIRE(parsed.readColumns.count("users.active") == 1);
}

TEST_CASE("QueryEventBase buildRWSet reflects WHERE items for DML") {
    auto parsed = parseColumns("SELECT id FROM users WHERE id = 42 AND status = 'active';");

    parsed.event->buildRWSet({});
    const auto &readItems = parsed.event->readSet();

    REQUIRE(readItems.size() == 2);

    const auto *idItem = findItem(readItems, "users.id");
    REQUIRE(idItem != nullptr);
    REQUIRE(idItem->function_type == FUNCTION_EQ);
    REQUIRE(idItem->data_list.size() == 1);
    REQUIRE(idItem->data_list[0].getAs<int64_t>() == 42);

    const auto *statusItem = findItem(readItems, "users.status");
    REQUIRE(statusItem != nullptr);
    REQUIRE(statusItem->function_type == FUNCTION_EQ);
    REQUIRE(statusItem->data_list.size() == 1);
    REQUIRE(statusItem->data_list[0].getAs<std::string>() == "active");
}

TEST_CASE("QueryEventBase buildRWSet handles IN lists") {
    auto parsed = parseColumns("SELECT id FROM users WHERE id IN (1, 2, 3);");

    parsed.event->buildRWSet({});
    const auto &readItems = parsed.event->readSet();

    const auto *idItem = findItem(readItems, "users.id");
    REQUIRE(idItem != nullptr);
    REQUIRE(idItem->function_type == FUNCTION_EQ);
    REQUIRE(idItem->data_list.size() == 3);
    REQUIRE(idItem->data_list[0].getAs<int64_t>() == 1);
    REQUIRE(idItem->data_list[1].getAs<int64_t>() == 2);
    REQUIRE(idItem->data_list[2].getAs<int64_t>() == 3);
}

TEST_CASE("QueryEventBase buildRWSet handles BETWEEN and NOT BETWEEN") {
    {
        auto parsed = parseColumns("SELECT id FROM users WHERE age BETWEEN 18 AND 30;");
        parsed.event->buildRWSet({});
        const auto &readItems = parsed.event->readSet();

        bool hasLower = false;
        bool hasUpper = false;
        for (const auto &item : readItems) {
            if (item.name != "users.age" || item.data_list.size() != 1) {
                continue;
            }
            const auto value = item.data_list[0].getAs<int64_t>();
            if (item.function_type == FUNCTION_GE && value == 18) {
                hasLower = true;
            } else if (item.function_type == FUNCTION_LE && value == 30) {
                hasUpper = true;
            }
        }
        REQUIRE(hasLower);
        REQUIRE(hasUpper);
    }

    {
        auto parsed = parseColumns("SELECT id FROM users WHERE age NOT BETWEEN 18 AND 30;");
        parsed.event->buildRWSet({});
        const auto &readItems = parsed.event->readSet();

        bool hasLower = false;
        bool hasUpper = false;
        for (const auto &item : readItems) {
            if (item.name != "users.age" || item.data_list.size() != 1) {
                continue;
            }
            const auto value = item.data_list[0].getAs<int64_t>();
            if (item.function_type == FUNCTION_LT && value == 18) {
                hasLower = true;
            } else if (item.function_type == FUNCTION_GT && value == 30) {
                hasUpper = true;
            }
        }
        REQUIRE(hasLower);
        REQUIRE(hasUpper);
    }
}

TEST_CASE("QueryEventBase buildRWSet handles NOT IN lists") {
    auto parsed = parseColumns("SELECT id FROM users WHERE status NOT IN ('banned', 'deleted');");

    parsed.event->buildRWSet({});
    const auto &readItems = parsed.event->readSet();

    const auto *statusItem = findItem(readItems, "users.status");
    REQUIRE(statusItem != nullptr);
    REQUIRE(statusItem->function_type == FUNCTION_NE);
    REQUIRE(statusItem->data_list.size() == 2);
    REQUIRE(statusItem->data_list[0].getAs<std::string>() == "banned");
    REQUIRE(statusItem->data_list[1].getAs<std::string>() == "deleted");
}

TEST_CASE("QueryEventBase columnRWSet UPDATE with LIKE and OR includes read columns") {
    auto parsed = parseColumns(
        "UPDATE users SET status = 'archived' "
        "WHERE email LIKE '%@example.com' OR id = 10;"
    );

    REQUIRE(parsed.writeColumns.count("users.status") == 1);
    REQUIRE(parsed.readColumns.count("users.email") == 1);
    REQUIRE(parsed.readColumns.count("users.id") == 1);
}

TEST_CASE("QueryEventBase buildRWSet preserves OR branches as separate items") {
    auto parsed = parseColumns("SELECT id FROM users WHERE id = 1 OR id = 2;");

    parsed.event->buildRWSet({});
    const auto &readItems = parsed.event->readSet();

    REQUIRE(readItems.size() == 2);

    bool hasFirst = false;
    bool hasSecond = false;
    for (const auto &item : readItems) {
        if (item.name != "users.id") {
            continue;
        }
        if (item.function_type != FUNCTION_EQ || item.data_list.size() != 1) {
            continue;
        }
        const auto value = item.data_list[0].getAs<int64_t>();
        if (value == 1) {
            hasFirst = true;
        } else if (value == 2) {
            hasSecond = true;
        }
    }
    REQUIRE(hasFirst);
    REQUIRE(hasSecond);
}

TEST_CASE("QueryEventBase buildRWSet handles DELETE with NOT BETWEEN") {
    auto parsed = parseColumns("DELETE FROM sessions WHERE last_seen NOT BETWEEN 100 AND 200;");

    parsed.event->buildRWSet({});
    const auto &readItems = parsed.event->readSet();

    bool hasLower = false;
    bool hasUpper = false;
    for (const auto &item : readItems) {
        if (item.name != "sessions.last_seen" || item.data_list.size() != 1) {
            continue;
        }
        const auto value = item.data_list[0].getAs<int64_t>();
        if (item.function_type == FUNCTION_LT && value == 100) {
            hasLower = true;
        } else if (item.function_type == FUNCTION_GT && value == 200) {
            hasUpper = true;
        }
    }
    REQUIRE(hasLower);
    REQUIRE(hasUpper);
}

// Subquery tests
TEST_CASE("QueryEventBase columnRWSet SELECT with IN subquery collects subquery table") {
    auto parsed = parseColumns("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);");

    // Main table should be in read
    REQUIRE(parsed.readColumns.count("users.id") == 1);
    // Subquery table columns should also be collected
    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet SELECT with IN subquery from derived table") {
    auto parsed = parseColumns(
        "SELECT * FROM users "
        "WHERE id IN (SELECT user_id FROM (SELECT user_id FROM orders WHERE status = 'paid') AS order_alias);"
    );

    REQUIRE(parsed.readColumns.count("users.id") == 1);
    // Derived subquery columns should be qualified to base table
    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
    REQUIRE(parsed.readColumns.count("orders.status") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet SELECT with EXISTS subquery collects subquery columns") {
    auto parsed = parseColumns(
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id);"
    );

    REQUIRE(parsed.readColumns.count("users.id") == 1);
    // EXISTS subquery columns should be collected
    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet SELECT with NOT IN subquery") {
    auto parsed = parseColumns(
        "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users);"
    );

    REQUIRE(parsed.readColumns.count("users.id") == 1);
    REQUIRE(parsed.readColumns.count("banned_users.user_id") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet SELECT with scalar subquery") {
    auto parsed = parseColumns(
        "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS cnt FROM users;"
    );

    REQUIRE(parsed.readColumns.count("users.name") == 1);
    // Scalar subquery columns should be collected
    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
    REQUIRE(parsed.readColumns.count("users.id") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet INSERT with subquery value") {
    auto parsed = parseColumns(
        "INSERT INTO user_stats (user_id, order_count) VALUES (1, (SELECT COUNT(*) FROM orders WHERE user_id = 1));"
    );

    REQUIRE(parsed.writeColumns.count("user_stats.user_id") == 1);
    REQUIRE(parsed.writeColumns.count("user_stats.order_count") == 1);
    REQUIRE(parsed.writeColumns.count("user_stats.*") == 1);
    // Subquery read columns
    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
}

TEST_CASE("QueryEventBase columnRWSet UPDATE with subquery in SET") {
    auto parsed = parseColumns(
        "UPDATE users SET order_count = (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) WHERE id = 1;"
    );

    REQUIRE(parsed.writeColumns.count("users.order_count") == 1);
    REQUIRE(parsed.readColumns.count("users.id") == 1);
    // Subquery read columns
    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
}

// Aggregate function tests
TEST_CASE("QueryEventBase columnRWSet SELECT with aggregate collects argument columns") {
    auto parsed = parseColumns("SELECT COUNT(*), SUM(amount), AVG(price) FROM orders;");

    // Aggregate function arguments should be in read columns
    REQUIRE(parsed.readColumns.count("orders.amount") == 1);
    REQUIRE(parsed.readColumns.count("orders.price") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet SELECT with COUNT DISTINCT") {
    auto parsed = parseColumns("SELECT COUNT(DISTINCT user_id) FROM orders;");

    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet SELECT with nested aggregate in expression") {
    auto parsed = parseColumns(
        "SELECT user_id, SUM(quantity * price) AS total FROM order_items WHERE status = 'paid';"
    );

    REQUIRE(parsed.readColumns.count("order_items.user_id") == 1);
    REQUIRE(parsed.readColumns.count("order_items.quantity") == 1);
    REQUIRE(parsed.readColumns.count("order_items.price") == 1);
    REQUIRE(parsed.readColumns.count("order_items.status") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

TEST_CASE("QueryEventBase columnRWSet UPDATE with aggregate subquery in WHERE") {
    auto parsed = parseColumns(
        "UPDATE users SET status = 'vip' WHERE id IN (SELECT user_id FROM orders GROUP BY user_id HAVING SUM(amount) > 1000);"
    );

    REQUIRE(parsed.writeColumns.count("users.status") == 1);
    REQUIRE(parsed.readColumns.count("users.id") == 1);
    // Subquery columns
    REQUIRE(parsed.readColumns.count("orders.user_id") == 1);
    REQUIRE(parsed.readColumns.count("orders.amount") == 1);
}

TEST_CASE("QueryEventBase columnRWSet SELECT with derived table") {
    auto parsed = parseColumns(
        "SELECT * FROM (SELECT id, name FROM users WHERE active = 1) AS t WHERE t.id > 10;"
    );

    // Derived table inner columns should be collected
    REQUIRE(parsed.readColumns.count("users.id") == 1);
    REQUIRE(parsed.readColumns.count("users.name") == 1);
    REQUIRE(parsed.readColumns.count("users.active") == 1);
    REQUIRE(parsed.writeColumns.empty());
}

#if 0
// TODO(DDL): 현재 QueryEventBase::processDDL()이 미지원이므로, DDL 기반 R/W set 테스트는
// 활성화하지 않는다. DDL 지원이 연결되면 아래 pseudo code를 실제 테스트로 전환한다.
//
// TEST_CASE("QueryEventBase columnRWSet CREATE TABLE includes all columns in write set") {
//     auto parsed = parseColumns("CREATE TABLE users (id INT, name VARCHAR(255));");
//     REQUIRE(parsed.writeColumns.count("users.*") == 1);
// }
//
// TEST_CASE("QueryEventBase columnRWSet ALTER TABLE ADD FOREIGN KEY reads referenced columns") {
//     auto parsed = parseColumns("ALTER TABLE posts ADD CONSTRAINT fk_author FOREIGN KEY (author_id) REFERENCES users(id);");
//     REQUIRE(parsed.readColumns.count("users.id") == 1);
//     REQUIRE(parsed.writeColumns.count("posts.*") == 1);
// }
//
// TEST_CASE("QueryEventBase columnRWSet DROP TABLE writes target and referencing FK columns") {
//     auto parsed = parseColumns("DROP TABLE users;");
//     REQUIRE(parsed.writeColumns.count("users.*") == 1);
//     // TODO: referencing FK columns of external tables should be included.
// }
#endif
