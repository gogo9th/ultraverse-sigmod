//
// Created by cheesekun on 1/22/26.
//

#include "config/UltraverseConfig.hpp"

#include <cstdlib>
#include <fstream>
#include <limits>
#include <sstream>

#include <nlohmann/json.hpp>

#include "utils/log.hpp"

namespace ultraverse::config {

    namespace {
        LoggerPtr logger = createLogger("UltraverseConfig");

        std::string getEnvString(const char *name) {
            const char *value = std::getenv(name);
            if (value == nullptr) {
                return {};
            }
            return std::string(value);
        }

        bool parseIntString(const std::string &value, int &out) {
            try {
                size_t idx = 0;
                long long parsed = std::stoll(value, &idx);
                if (idx != value.size()) {
                    return false;
                }
                if (parsed < std::numeric_limits<int>::min() || parsed > std::numeric_limits<int>::max()) {
                    return false;
                }
                out = static_cast<int>(parsed);
                return true;
            } catch (const std::exception &) {
                return false;
            }
        }

        bool readStringField(const nlohmann::json &obj, const char *key, std::string &out,
                             const std::string &path, bool required) {
            if (!obj.contains(key)) {
                if (required) {
                    logger->error("missing required field: {}", path);
                    return false;
                }
                return true;
            }

            const auto &value = obj.at(key);
            if (value.is_null()) {
                if (required) {
                    logger->error("required field is null: {}", path);
                    return false;
                }
                return true;
            }

            if (!value.is_string()) {
                logger->error("field must be a string: {}", path);
                return false;
            }

            out = value.get<std::string>();
            return true;
        }

        bool readBoolField(const nlohmann::json &obj, const char *key, bool &out,
                           const std::string &path, bool required) {
            if (!obj.contains(key)) {
                if (required) {
                    logger->error("missing required field: {}", path);
                    return false;
                }
                return true;
            }

            const auto &value = obj.at(key);
            if (value.is_null()) {
                if (required) {
                    logger->error("required field is null: {}", path);
                    return false;
                }
                return true;
            }

            if (!value.is_boolean()) {
                logger->error("field must be a boolean: {}", path);
                return false;
            }

            out = value.get<bool>();
            return true;
        }

        bool readIntField(const nlohmann::json &obj, const char *key, int &out,
                          const std::string &path, bool required) {
            if (!obj.contains(key)) {
                if (required) {
                    logger->error("missing required field: {}", path);
                    return false;
                }
                return true;
            }

            const auto &value = obj.at(key);
            if (value.is_null()) {
                if (required) {
                    logger->error("required field is null: {}", path);
                    return false;
                }
                return true;
            }

            if (value.is_number_integer()) {
                auto parsed = value.get<long long>();
                if (parsed < std::numeric_limits<int>::min() || parsed > std::numeric_limits<int>::max()) {
                    logger->error("field out of range: {}", path);
                    return false;
                }
                out = static_cast<int>(parsed);
                return true;
            }

            if (value.is_number_unsigned()) {
                auto parsed = value.get<unsigned long long>();
                if (parsed > static_cast<unsigned long long>(std::numeric_limits<int>::max())) {
                    logger->error("field out of range: {}", path);
                    return false;
                }
                out = static_cast<int>(parsed);
                return true;
            }

            if (value.is_string()) {
                auto text = value.get<std::string>();
                if (!parseIntString(text, out)) {
                    logger->error("field must be an integer: {}", path);
                    return false;
                }
                return true;
            }

            logger->error("field must be an integer: {}", path);
            return false;
        }

        bool readStringArray(const nlohmann::json &obj, const char *key,
                             std::vector<std::string> &out,
                             const std::string &path, bool required, bool requireNonEmpty) {
            if (!obj.contains(key)) {
                if (required) {
                    logger->error("missing required field: {}", path);
                    return false;
                }
                return true;
            }

            const auto &value = obj.at(key);
            if (value.is_null()) {
                if (required) {
                    logger->error("required field is null: {}", path);
                    return false;
                }
                return true;
            }

            if (!value.is_array()) {
                logger->error("field must be an array: {}", path);
                return false;
            }

            std::vector<std::string> entries;
            for (const auto &item: value) {
                if (!item.is_string()) {
                    logger->error("array elements must be strings: {}", path);
                    return false;
                }
                entries.emplace_back(item.get<std::string>());
            }

            if (requireNonEmpty && entries.empty()) {
                logger->error("field must contain at least one entry: {}", path);
                return false;
            }

            out = std::move(entries);
            return true;
        }

        bool validateRangeComparisonMethod(const std::string &value) {
            return value == "intersect" || value == "eqonly";
        }
    } // namespace

    std::optional<UltraverseConfig> UltraverseConfig::loadFromFile(const std::string &path) {
        std::ifstream inputStream(path);
        if (!inputStream.is_open()) {
            logger->error("failed to open config file: {}", path);
            return std::nullopt;
        }

        std::stringstream buffer;
        buffer << inputStream.rdbuf();
        inputStream.close();

        return loadFromString(buffer.str());
    }

    std::optional<UltraverseConfig> UltraverseConfig::loadFromString(const std::string &jsonStr) {
        using nlohmann::json;

        auto document = json::parse(jsonStr, nullptr, false);
        if (document.is_discarded()) {
            logger->error("failed to parse config JSON");
            return std::nullopt;
        }

        if (!document.is_object()) {
            logger->error("config JSON must be an object");
            return std::nullopt;
        }

        UltraverseConfig config;

        bool binlogPathProvided = false;
        bool dbHostProvided = false;
        bool dbPortProvided = false;
        bool dbUserProvided = false;
        bool dbPassProvided = false;

        if (document.contains("binlog")) {
            const auto &binlogObj = document.at("binlog");
            if (!binlogObj.is_object()) {
                logger->error("field must be an object: binlog");
                return std::nullopt;
            }

            binlogPathProvided = binlogObj.contains("path");
            if (!readStringField(binlogObj, "path", config.binlog.path, "binlog.path", false)) {
                return std::nullopt;
            }
            if (!readStringField(binlogObj, "indexName", config.binlog.indexName, "binlog.indexName", false)) {
                return std::nullopt;
            }
        }

        if (document.contains("stateLog")) {
            const auto &stateLogObj = document.at("stateLog");
            if (!stateLogObj.is_object()) {
                logger->error("field must be an object: stateLog");
                return std::nullopt;
            }

            if (!readStringField(stateLogObj, "path", config.stateLog.path, "stateLog.path", false)) {
                return std::nullopt;
            }
            if (!readStringField(stateLogObj, "name", config.stateLog.name, "stateLog.name", true)) {
                return std::nullopt;
            }
        } else {
            logger->error("missing required field: stateLog.name");
            return std::nullopt;
        }

        if (!readStringArray(document, "keyColumns", config.keyColumns, "keyColumns", true, true)) {
            return std::nullopt;
        }

        if (document.contains("columnAliases")) {
            const auto &aliasesObj = document.at("columnAliases");
            if (!aliasesObj.is_object()) {
                logger->error("field must be an object: columnAliases");
                return std::nullopt;
            }

            for (auto it = aliasesObj.begin(); it != aliasesObj.end(); ++it) {
                const auto &aliasKey = it.key();
                const auto &aliasValues = it.value();

                if (!aliasValues.is_array()) {
                    logger->error("columnAliases entry must be an array: {}", aliasKey);
                    return std::nullopt;
                }

                std::vector<std::string> targets;
                for (const auto &entry: aliasValues) {
                    if (!entry.is_string()) {
                        logger->error("columnAliases values must be strings: {}", aliasKey);
                        return std::nullopt;
                    }
                    targets.emplace_back(entry.get<std::string>());
                }

                config.columnAliases.emplace(aliasKey, std::move(targets));
            }
        }

        if (document.contains("database")) {
            const auto &dbObj = document.at("database");
            if (!dbObj.is_object()) {
                logger->error("field must be an object: database");
                return std::nullopt;
            }

            dbHostProvided = dbObj.contains("host");
            dbPortProvided = dbObj.contains("port");
            dbUserProvided = dbObj.contains("username");
            dbPassProvided = dbObj.contains("password");

            if (!readStringField(dbObj, "host", config.database.host, "database.host", false)) {
                return std::nullopt;
            }
            if (!readIntField(dbObj, "port", config.database.port, "database.port", false)) {
                return std::nullopt;
            }
            if (!readStringField(dbObj, "name", config.database.name, "database.name", true)) {
                return std::nullopt;
            }
            if (!readStringField(dbObj, "username", config.database.username, "database.username", false)) {
                return std::nullopt;
            }
            if (!readStringField(dbObj, "password", config.database.password, "database.password", false)) {
                return std::nullopt;
            }

            if (dbObj.contains("password")) {
                const auto &pwValue = dbObj.at("password");
                if (pwValue.is_string() && !pwValue.get<std::string>().empty()) {
                    logger->warn("database.password is stored in plain text in config JSON");
                }
            }
        } else {
            logger->error("missing required field: database.name");
            return std::nullopt;
        }

        if (document.contains("statelogd")) {
            const auto &statelogdObj = document.at("statelogd");
            if (!statelogdObj.is_object()) {
                logger->error("field must be an object: statelogd");
                return std::nullopt;
            }

            if (!readIntField(statelogdObj, "threadCount", config.statelogd.threadCount, "statelogd.threadCount", false)) {
                return std::nullopt;
            }
            if (!readBoolField(statelogdObj, "oneshotMode", config.statelogd.oneshotMode, "statelogd.oneshotMode", false)) {
                return std::nullopt;
            }
            if (!readStringField(statelogdObj, "procedureLogPath", config.statelogd.procedureLogPath,
                                 "statelogd.procedureLogPath", false)) {
                return std::nullopt;
            }
            if (!readStringArray(statelogdObj, "developmentFlags", config.statelogd.developmentFlags,
                                 "statelogd.developmentFlags", false, false)) {
                return std::nullopt;
            }
        }

        if (document.contains("stateChange")) {
            const auto &stateChangeObj = document.at("stateChange");
            if (!stateChangeObj.is_object()) {
                logger->error("field must be an object: stateChange");
                return std::nullopt;
            }

            if (!readIntField(stateChangeObj, "threadCount", config.stateChange.threadCount,
                              "stateChange.threadCount", false)) {
                return std::nullopt;
            }
            if (!readStringField(stateChangeObj, "backupFile", config.stateChange.backupFile,
                                 "stateChange.backupFile", false)) {
                return std::nullopt;
            }
            if (!readBoolField(stateChangeObj, "keepIntermediateDatabase",
                               config.stateChange.keepIntermediateDatabase,
                               "stateChange.keepIntermediateDatabase", false)) {
                return std::nullopt;
            }
            if (!readStringField(stateChangeObj, "rangeComparisonMethod",
                                 config.stateChange.rangeComparisonMethod,
                                 "stateChange.rangeComparisonMethod", false)) {
                return std::nullopt;
            }

            if (!validateRangeComparisonMethod(config.stateChange.rangeComparisonMethod)) {
                logger->error("stateChange.rangeComparisonMethod must be 'intersect' or 'eqonly'");
                return std::nullopt;
            }
        }

        if (!binlogPathProvided) {
            auto envPath = getEnvString("BINLOG_PATH");
            if (!envPath.empty()) {
                config.binlog.path = envPath;
            }
        }

        if (!dbHostProvided) {
            auto envHost = getEnvString("DB_HOST");
            if (!envHost.empty()) {
                config.database.host = envHost;
            }
        }

        if (!dbPortProvided) {
            auto envPort = getEnvString("DB_PORT");
            if (!envPort.empty()) {
                int parsedPort = 0;
                if (!parseIntString(envPort, parsedPort)) {
                    logger->error("DB_PORT must be an integer");
                    return std::nullopt;
                }
                config.database.port = parsedPort;
            }
        }

        if (!dbUserProvided) {
            auto envUser = getEnvString("DB_USER");
            if (!envUser.empty()) {
                config.database.username = envUser;
            }
        }

        if (!dbPassProvided) {
            auto envPass = getEnvString("DB_PASS");
            if (!envPass.empty()) {
                config.database.password = envPass;
            }
        }

        return config;
    }

} // namespace ultraverse::config
