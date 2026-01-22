//
// Created by cheesekun on 1/22/26.
//

#ifndef ULTRAVERSE_CONFIG_ULTRAVERSECONFIG_HPP
#define ULTRAVERSE_CONFIG_ULTRAVERSECONFIG_HPP

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace ultraverse::config {

    struct BinlogConfig {
        std::string path = "/var/lib/mysql";
        std::string indexName = "mysql-bin.index";
    };

    struct StateLogConfig {
        std::string path = ".";
        std::string name;  // required
    };

    struct DatabaseConfig {
        std::string host;
        int port = 3306;
        std::string name;  // required
        std::string username;
        std::string password;
    };

    struct StatelogdConfig {
        int threadCount = 0;  // 0 = auto
        bool oneshotMode = false;
        std::string procedureLogPath;
        std::vector<std::string> developmentFlags;  // "print-gids", "print-queries"
    };

    struct StateChangeConfig {
        int threadCount = 0;  // 0 = auto
        std::string backupFile;
        bool keepIntermediateDatabase = false;
        std::string rangeComparisonMethod = "eqonly";  // "intersect" | "eqonly"
    };

    struct UltraverseConfig {
        BinlogConfig binlog;
        StateLogConfig stateLog;
        std::vector<std::string> keyColumns;  // required
        std::map<std::string, std::vector<std::string>> columnAliases;
        DatabaseConfig database;
        StatelogdConfig statelogd;
        StateChangeConfig stateChange;

        static std::optional<UltraverseConfig> loadFromFile(const std::string &path);
        static std::optional<UltraverseConfig> loadFromString(const std::string &jsonStr);
    };

} // namespace ultraverse::config

#endif // ULTRAVERSE_CONFIG_ULTRAVERSECONFIG_HPP
