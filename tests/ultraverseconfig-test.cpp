//
// UltraverseConfig unit tests
//

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "config/UltraverseConfig.hpp"

using ultraverse::config::UltraverseConfig;

namespace {

class ScopedEnvVar {
public:
    ScopedEnvVar(const std::string &name, std::optional<std::string> value)
        : name_(name) {
        const char *prev = std::getenv(name_.c_str());
        if (prev != nullptr) {
            hadPrev_ = true;
            prevValue_ = std::string(prev);
        }

        if (value.has_value()) {
            ok_ = (setenv(name_.c_str(), value->c_str(), 1) == 0);
        } else {
            ok_ = (unsetenv(name_.c_str()) == 0);
        }
    }

    ~ScopedEnvVar() {
        if (hadPrev_) {
            setenv(name_.c_str(), prevValue_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    bool ok() const { return ok_; }

private:
    std::string name_;
    bool hadPrev_ = false;
    std::string prevValue_;
    bool ok_ = false;
};

struct ScopedEnvReset {
    ScopedEnvVar host{"DB_HOST", std::nullopt};
    ScopedEnvVar port{"DB_PORT", std::nullopt};
    ScopedEnvVar user{"DB_USER", std::nullopt};
    ScopedEnvVar pass{"DB_PASS", std::nullopt};
    ScopedEnvVar binlog{"BINLOG_PATH", std::nullopt};

    bool ok() const {
        return host.ok() && port.ok() && user.ok() && pass.ok() && binlog.ok();
    }
};

std::string makeMinimalConfig() {
    return R"({
        "stateLog": { "name": "test-log" },
        "keyColumns": ["users.id"],
        "database": { "name": "testdb" }
    })";
}

} // namespace

TEST_CASE("UltraverseConfig parses full JSON", "[config]") {
    ScopedEnvReset envReset;
    REQUIRE(envReset.ok());

    const std::string json = R"({
        "binlog": { "path": "/data/binlog", "indexName": "binlog.index" },
        "stateLog": { "path": "/var/log/ultra", "name": "main-log" },
        "keyColumns": ["users.id", "orders.user_id"],
        "columnAliases": {
            "users.id": ["orders.user_id", "payments.user_id"],
            "orders.id": ["shipments.order_id"]
        },
        "database": {
            "host": "db.example",
            "port": 1337,
            "name": "prod",
            "username": "app",
            "password": "secret"
        },
        "statelogd": {
            "threadCount": 4,
            "oneshotMode": true,
            "procedureLogPath": "/var/log/proc",
            "developmentFlags": ["print-gids", "print-queries"]
        },
        "stateChange": {
            "threadCount": 2,
            "backupFile": "/tmp/backup.sql",
            "keepIntermediateDatabase": true,
            "rangeComparisonMethod": "intersect"
        }
    })";

    auto config = UltraverseConfig::loadFromString(json);
    REQUIRE(config.has_value());
    CHECK(config->binlog.path == "/data/binlog");
    CHECK(config->binlog.indexName == "binlog.index");
    CHECK(config->stateLog.path == "/var/log/ultra");
    CHECK(config->stateLog.name == "main-log");
    CHECK(config->keyColumns == std::vector<std::string>{"users.id", "orders.user_id"});
    CHECK(config->columnAliases.at("users.id") ==
          std::vector<std::string>{"orders.user_id", "payments.user_id"});
    CHECK(config->columnAliases.at("orders.id") ==
          std::vector<std::string>{"shipments.order_id"});
    CHECK(config->database.host == "db.example");
    CHECK(config->database.port == 1337);
    CHECK(config->database.name == "prod");
    CHECK(config->database.username == "app");
    CHECK(config->database.password == "secret");
    CHECK(config->statelogd.threadCount == 4);
    CHECK(config->statelogd.oneshotMode);
    CHECK(config->statelogd.procedureLogPath == "/var/log/proc");
    CHECK(config->statelogd.developmentFlags ==
          std::vector<std::string>{"print-gids", "print-queries"});
    CHECK(config->stateChange.threadCount == 2);
    CHECK(config->stateChange.backupFile == "/tmp/backup.sql");
    CHECK(config->stateChange.keepIntermediateDatabase);
    CHECK(config->stateChange.rangeComparisonMethod == "intersect");
}

TEST_CASE("UltraverseConfig validates required fields", "[config]") {
    ScopedEnvReset envReset;
    REQUIRE(envReset.ok());

    SECTION("stateLog.name missing") {
        const std::string json = R"({
            "stateLog": {},
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" }
        })";
        REQUIRE_FALSE(UltraverseConfig::loadFromString(json).has_value());
    }

    SECTION("keyColumns missing") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "database": { "name": "testdb" }
        })";
        REQUIRE_FALSE(UltraverseConfig::loadFromString(json).has_value());
    }

    SECTION("keyColumns empty") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": [],
            "database": { "name": "testdb" }
        })";
        REQUIRE_FALSE(UltraverseConfig::loadFromString(json).has_value());
    }

    SECTION("database.name missing") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "host": "localhost" }
        })";
        REQUIRE_FALSE(UltraverseConfig::loadFromString(json).has_value());
    }
}

TEST_CASE("UltraverseConfig defaults optional fields", "[config]") {
    ScopedEnvReset envReset;
    REQUIRE(envReset.ok());

    auto config = UltraverseConfig::loadFromString(makeMinimalConfig());
    REQUIRE(config.has_value());
    CHECK(config->binlog.path == "/var/lib/mysql");
    CHECK(config->binlog.indexName == "mysql-bin.index");
    CHECK(config->stateLog.path == ".");
    CHECK(config->database.port == 3306);
    CHECK(config->statelogd.threadCount == 0);
    CHECK_FALSE(config->statelogd.oneshotMode);
    CHECK_FALSE(config->stateChange.keepIntermediateDatabase);
    CHECK(config->stateChange.rangeComparisonMethod == "eqonly");
}

TEST_CASE("UltraverseConfig uses environment fallbacks", "[config]") {
    ScopedEnvVar envHost("DB_HOST", std::optional<std::string>{"db-host"});
    ScopedEnvVar envPort("DB_PORT", std::optional<std::string>{"3456"});
    ScopedEnvVar envUser("DB_USER", std::optional<std::string>{"db-user"});
    ScopedEnvVar envPass("DB_PASS", std::optional<std::string>{"db-pass"});
    ScopedEnvVar envBinlog("BINLOG_PATH", std::optional<std::string>{"/env/binlog"});

    REQUIRE(envHost.ok());
    REQUIRE(envPort.ok());
    REQUIRE(envUser.ok());
    REQUIRE(envPass.ok());
    REQUIRE(envBinlog.ok());

    auto config = UltraverseConfig::loadFromString(makeMinimalConfig());
    REQUIRE(config.has_value());
    CHECK(config->binlog.path == "/env/binlog");
    CHECK(config->database.host == "db-host");
    CHECK(config->database.port == 3456);
    CHECK(config->database.username == "db-user");
    CHECK(config->database.password == "db-pass");
}

TEST_CASE("UltraverseConfig validates field types", "[config]") {
    ScopedEnvReset envReset;
    REQUIRE(envReset.ok());

    SECTION("database.port accepts numeric string") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb", "port": "3307" }
        })";
        auto config = UltraverseConfig::loadFromString(json);
        REQUIRE(config.has_value());
        CHECK(config->database.port == 3307);
    }

    SECTION("database.port rejects non-numeric string") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb", "port": "not-a-number" }
        })";
        REQUIRE_FALSE(UltraverseConfig::loadFromString(json).has_value());
    }

    SECTION("statelogd.oneshotMode rejects non-boolean") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" },
            "statelogd": { "oneshotMode": "true" }
        })";
        REQUIRE_FALSE(UltraverseConfig::loadFromString(json).has_value());
    }
}

TEST_CASE("UltraverseConfig validates range comparison method", "[config]") {
    ScopedEnvReset envReset;
    REQUIRE(envReset.ok());

    SECTION("intersect accepted") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" },
            "stateChange": { "rangeComparisonMethod": "intersect" }
        })";
        REQUIRE(UltraverseConfig::loadFromString(json).has_value());
    }

    SECTION("eqonly accepted") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" },
            "stateChange": { "rangeComparisonMethod": "eqonly" }
        })";
        REQUIRE(UltraverseConfig::loadFromString(json).has_value());
    }

    SECTION("invalid method rejected") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" },
            "stateChange": { "rangeComparisonMethod": "invalid" }
        })";
        REQUIRE_FALSE(UltraverseConfig::loadFromString(json).has_value());
    }
}

TEST_CASE("UltraverseConfig parses columnAliases", "[config]") {
    ScopedEnvReset envReset;
    REQUIRE(envReset.ok());

    SECTION("multiple aliases parsed") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" },
            "columnAliases": {
                "users.id": ["orders.user_id", "payments.user_id"],
                "orders.id": ["shipments.order_id"]
            }
        })";
        auto config = UltraverseConfig::loadFromString(json);
        REQUIRE(config.has_value());
        REQUIRE(config->columnAliases.size() == 2);
        CHECK(config->columnAliases.at("users.id") ==
              std::vector<std::string>{"orders.user_id", "payments.user_id"});
        CHECK(config->columnAliases.at("orders.id") ==
              std::vector<std::string>{"shipments.order_id"});
    }

    SECTION("alias values must be arrays") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" },
            "columnAliases": {
                "users.id": "orders.user_id"
            }
        })";
        REQUIRE_FALSE(UltraverseConfig::loadFromString(json).has_value());
    }
}

TEST_CASE("UltraverseConfig parses developmentFlags", "[config]") {
    ScopedEnvReset envReset;
    REQUIRE(envReset.ok());

    SECTION("empty array allowed") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" },
            "statelogd": { "developmentFlags": [] }
        })";
        auto config = UltraverseConfig::loadFromString(json);
        REQUIRE(config.has_value());
        CHECK(config->statelogd.developmentFlags.empty());
    }

    SECTION("multiple flags parsed") {
        const std::string json = R"({
            "stateLog": { "name": "test-log" },
            "keyColumns": ["users.id"],
            "database": { "name": "testdb" },
            "statelogd": { "developmentFlags": ["print-gids", "print-queries"] }
        })";
        auto config = UltraverseConfig::loadFromString(json);
        REQUIRE(config.has_value());
        CHECK(config->statelogd.developmentFlags ==
              std::vector<std::string>{"print-gids", "print-queries"});
    }
}
