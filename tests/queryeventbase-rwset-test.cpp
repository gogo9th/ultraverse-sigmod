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

    // TODO: When RHS column reads are tracked, expect users.age in readColumns.
}

TEST_CASE("QueryEventBase columnRWSet DELETE adds wildcard write and where read") {
    auto parsed = parseColumns("DELETE FROM users WHERE id = 7;");

    REQUIRE(parsed.writeColumns.count("users.*") == 1);
    REQUIRE(parsed.readColumns.count("users.id") == 1);
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

        const auto *ageItem = findItem(readItems, "users.age");
        REQUIRE(ageItem != nullptr);
        REQUIRE(ageItem->function_type == FUNCTION_BETWEEN);
        REQUIRE(ageItem->data_list.size() == 2);
        REQUIRE(ageItem->data_list[0].getAs<int64_t>() == 18);
        REQUIRE(ageItem->data_list[1].getAs<int64_t>() == 30);
    }

    {
        auto parsed = parseColumns("SELECT id FROM users WHERE age NOT BETWEEN 18 AND 30;");
        parsed.event->buildRWSet({});
        const auto &readItems = parsed.event->readSet();

        const auto *ageItem = findItem(readItems, "users.age");
        REQUIRE(ageItem != nullptr);
        REQUIRE(ageItem->function_type == FUNCTION_NE);
        REQUIRE(ageItem->data_list.size() == 2);
        REQUIRE(ageItem->data_list[0].getAs<int64_t>() == 18);
        REQUIRE(ageItem->data_list[1].getAs<int64_t>() == 30);
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

    const auto *item = findItem(readItems, "sessions.last_seen");
    REQUIRE(item != nullptr);
    REQUIRE(item->function_type == FUNCTION_NE);
    REQUIRE(item->data_list.size() == 2);
    REQUIRE(item->data_list[0].getAs<int64_t>() == 100);
    REQUIRE(item->data_list[1].getAs<int64_t>() == 200);
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
