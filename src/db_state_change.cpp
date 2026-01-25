#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

#include "Application.hpp"
#include "config/UltraverseConfig.hpp"
#include "db_state_change.hpp"
#include "utils/StringUtil.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

namespace ultraverse {
    using namespace ultraverse::state::v2;
    
    MakeClusterAction::MakeClusterAction() {
    
    }
    
    ActionType::Value MakeClusterAction::type() {
        return ActionType::MAKE_CLUSTER;
    }
    
    RollbackAction::RollbackAction(gid_t gid):
        _gid(gid)
    {
    
    }
    
    ActionType::Value RollbackAction::type() {
        return ActionType::ROLLBACK;
    }
    
    gid_t RollbackAction::gid() const {
        return _gid;
    }
    
    AutoRollbackAction::AutoRollbackAction(double ratio):
        _ratio(ratio)
    {
    
    }
    
    ActionType::Value AutoRollbackAction::type() {
        return ActionType::AUTO_ROLLBACK;
    }
    
    double AutoRollbackAction::ratio() const {
        return _ratio;
    }
    
    PrependAction::PrependAction(gid_t gid, std::string sqlFile):
        _gid(gid),
        _sqlFile(sqlFile)
    {
    
    }
    
    ActionType::Value PrependAction::type() {
        return ActionType::PREPEND;
    }
    
    gid_t PrependAction::gid() const {
        return _gid;
    }
    
    std::string PrependAction::sqlFile() const {
        return _sqlFile;
    }
    
    FullReplayAction::FullReplayAction() {
    
    }
    
    ActionType::Value FullReplayAction::type() {
        return ActionType::FULL_REPLAY;
    }
    
    ReplayAction::ReplayAction() {
    }
    
    ActionType::Value ReplayAction::type() {
        return ActionType::REPLAY;
    }
    
    DBStateChangeApp::DBStateChangeApp():
        _logger(createLogger("statechange"))
    {
    }
    
    std::string DBStateChangeApp::optString() {
        return "+vVh";
    }
    
    int DBStateChangeApp::main() {
        auto printHelp = []() {
            std::cout <<
            "db_state_change - database state change tool\n"
            "\n"
            "Usage: db_state_change [OPTIONS] CONFIG_JSON ACTION\n"
            "\n"
            "Options:\n"
            "    --gid-range START...END    GID range to process\n"
            "    --skip-gids GID1,GID2,...  GIDs to skip\n"
            "    --replay-from GID          Replay all transactions from GID before executing replay plan\n"
            "    --no-exec-replace-query    Do not execute replace queries; print them for manual run\n"
            "    --dry-run                  Dry run mode\n"
            "    -v                         set logger level to DEBUG\n"
            "    -V                         set logger level to TRACE\n"
            "    -h                         print this help and exit\n"
            "\n"
            "Environment:\n"
            "    ULTRAVERSE_REPORT_NAME     Report file name (optional)\n"
            "\n"
            "Actions:\n"
            "    make_cluster               Create cluster files\n"
            "    rollback=gid1,gid2,...     Rollback specified GIDs\n"
            "    auto-rollback=ratio        Auto-select rollback targets by ratio\n"
            "    prepend=gid,sqlfile        Prepend SQL file before GID\n"
            "    full-replay                Full replay\n"
            "    replay                     Replay from plan file\n";
        };

        bool showHelp = false;
        bool debugLog = false;
        bool traceLog = false;
        bool gidRangeSet = false;
        bool skipGidsSet = false;
        bool dryRun = false;
        bool replayFromSet = false;
        bool executeReplaceQuery = true;
        gid_t startGid = 0;
        gid_t endGid = 0;
        gid_t replayFromGid = 0;
        std::vector<uint64_t> skipGids;

        auto trim = [](std::string value) {
            const auto isSpace = [](unsigned char ch) { return std::isspace(ch); };
            value.erase(value.begin(), std::find_if_not(value.begin(), value.end(), isSpace));
            value.erase(std::find_if_not(value.rbegin(), value.rend(), isSpace).base(), value.end());
            return value;
        };

        opterr = 0;
        optind = 1;

        static struct option long_options[] = {
            {"gid-range", required_argument, 0, 's'},
            {"skip-gids", required_argument, 0, 'S'},
            {"replay-from", required_argument, 0, 'R'},
            {"no-exec-replace-query", no_argument, 0, 'M'},
            {"dry-run", no_argument, 0, 'D'},
            {0, 0, 0, 0}
        };

        int option = 0;
        while ((option = getopt_long(argc(), argv(), "vVh", long_options, nullptr)) != -1) {
            switch (option) {
                case 'v':
                    debugLog = true;
                    break;
                case 'V':
                    traceLog = true;
                    break;
                case 'h':
                    showHelp = true;
                    break;
                case 's': {
                    std::string rangeExpr = optarg != nullptr ? std::string(optarg) : "";
                    auto sepPos = rangeExpr.find("...");
                    if (sepPos == std::string::npos || rangeExpr.find("...", sepPos + 3) != std::string::npos) {
                        _logger->error("invalid --gid-range format, expected START...END");
                        return 1;
                    }
                    auto startStr = trim(rangeExpr.substr(0, sepPos));
                    auto endStr = trim(rangeExpr.substr(sepPos + 3));
                    if (startStr.empty() || endStr.empty()) {
                        _logger->error("invalid --gid-range format, expected START...END");
                        return 1;
                    }
                    try {
                        startGid = static_cast<gid_t>(std::stoull(startStr));
                        endGid = static_cast<gid_t>(std::stoull(endStr));
                    } catch (const std::exception &) {
                        _logger->error("invalid --gid-range value, expected numeric START...END");
                        return 1;
                    }
                    if (startGid > endGid) {
                        _logger->error("invalid --gid-range value, START must be <= END");
                        return 1;
                    }
                    gidRangeSet = true;
                    break;
                }
                case 'S': {
                    std::string gidsExpr = optarg != nullptr ? std::string(optarg) : "";
                    auto parsed = buildSkipGidList(gidsExpr);
                    skipGids.insert(skipGids.end(), parsed.begin(), parsed.end());
                    skipGidsSet = true;
                    break;
                }
                case 'D':
                    dryRun = true;
                    break;
                case 'M':
                    executeReplaceQuery = false;
                    break;
                case 'R': {
                    std::string gidExpr = optarg != nullptr ? std::string(optarg) : "";
                    if (gidExpr.empty()) {
                        _logger->error("invalid --replay-from value, expected numeric GID");
                        return 1;
                    }
                    try {
                        replayFromGid = static_cast<gid_t>(std::stoull(gidExpr));
                    } catch (const std::exception &) {
                        _logger->error("invalid --replay-from value, expected numeric GID");
                        return 1;
                    }
                    replayFromSet = true;
                    break;
                }
                case '?':
                default:
                    _logger->error("invalid option");
                    printHelp();
                    return 1;
            }
        }

        if (traceLog) {
            setLogLevel(spdlog::level::trace);
        } else if (debugLog) {
            setLogLevel(spdlog::level::debug);
        }

        if (showHelp) {
            printHelp();
            return 0;
        }

        int positionalCount = argc() - optind;
        if (positionalCount != 2) {
            _logger->error("CONFIG_JSON and ACTION must be specified");
            printHelp();
            return 1;
        }

        const std::string configPath = argv()[argc() - 2];
        const std::string actionExpr = argv()[argc() - 1];

        auto configOpt = ultraverse::config::UltraverseConfig::loadFromFile(configPath);
        if (!configOpt) {
            _logger->error("failed to load config file");
            return 1;
        }
        const auto &config = *configOpt;

        if (config.database.host.empty() ||
            config.database.username.empty() ||
            config.database.password.empty()) {
            _logger->error("Database credential not provided - check config JSON or DB_* environment variables");
            return 1;
        }

        int threadNum = config.stateChange.threadCount > 0
            ? config.stateChange.threadCount
            : static_cast<int>(std::thread::hardware_concurrency() * 2);

        DBHandlePool<mariadb::MySQLDBHandle> dbHandlePool(
            threadNum,
            config.database.host,
            config.database.port,
            config.database.username,
            config.database.password
        );
        DBHandlePoolAdapter<mariadb::MySQLDBHandle> dbHandlePoolAdapter(dbHandlePool);

        std::vector<std::shared_ptr<Action>> actions;
        try {
            actions = parseActions(actionExpr);
        } catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
            return 1;
        }
        if (actions.empty()) {
            _logger->error("no action specified");
            return 1;
        }
        
        bool makeClusterMap = std::find_if(actions.begin(), actions.end(), [](auto &action) {
            return std::dynamic_pointer_cast<MakeClusterAction>(action) != nullptr;
        }) != actions.end();
        
        bool fullReplay = std::find_if(actions.begin(), actions.end(), [](auto &action) {
            return std::dynamic_pointer_cast<FullReplayAction>(action) != nullptr;
        }) != actions.end();
        
        bool replay = std::find_if(actions.begin(), actions.end(), [](auto &action) {
            return std::dynamic_pointer_cast<ReplayAction>(action) != nullptr;
        }) != actions.end();
        
        bool autoRollback = std::find_if(actions.begin(), actions.end(), [](auto &action) {
            return std::dynamic_pointer_cast<AutoRollbackAction>(action) != nullptr;
        }) != actions.end();
        
        if (makeClusterMap && actions.size() > 1) {
            throw std::runtime_error("make_clustermap cannot be executed with other actions.");
        }
        
        /*
        if (fullReplay && actions.size() > 1) {
            throw std::runtime_error("full_replay cannot be executed with other actions.");
        }
         */
        
        StateChangePlan changePlan;

        if (!config.stateChange.backupFile.empty()) {
            changePlan.setDBDumpPath(config.stateChange.backupFile);
        } else {
            _logger->warn("database dump file is not specified!");
            _logger->warn("- this may leads to unexpected result");
            _logger->warn("- all queries will be executed until gid reaches rollback target");
        }

        changePlan.setStateLogPath(config.stateLog.path);
        changePlan.setStateLogName(config.stateLog.name);
        changePlan.setDBName(config.database.name);

        changePlan.setKeyColumnGroups(utility::parseKeyColumnGroups(config.keyColumns));

        for (const auto &entry : config.columnAliases) {
            for (const auto &alias : entry.second) {
                changePlan.columnAliases().emplace_back(entry.first, alias);
            }
        }

        changePlan.setBinlogPath(config.binlog.path);
        changePlan.setThreadNum(threadNum);
        changePlan.setDropIntermediateDB(!config.stateChange.keepIntermediateDatabase);
        changePlan.setRangeComparisonMethod(
            config.stateChange.rangeComparisonMethod == "intersect" ? INTERSECT : EQ_ONLY
        );
        changePlan.setExecuteReplaceQuery(executeReplaceQuery);

        changePlan.setDBHost(config.database.host);
        changePlan.setDBUsername(config.database.username);
        changePlan.setDBPassword(config.database.password);
        changePlan.setDryRun(dryRun);

        if (gidRangeSet) {
            changePlan.setStartGid(startGid);
            changePlan.setEndGid(endGid);
        }
        if (skipGidsSet) {
            changePlan.skipGids().insert(changePlan.skipGids().end(), skipGids.begin(), skipGids.end());
        }
        if (replayFromSet) {
            changePlan.setReplayFromGid(replayFromGid);
        }

        const char *reportEnv = std::getenv("ULTRAVERSE_REPORT_NAME");
        if (reportEnv != nullptr && *reportEnv != '\0') {
            changePlan.setReportPath(reportEnv);
        } else {
            std::time_t now = std::time(nullptr);
            std::tm timeInfo = *std::localtime(&now);
            std::ostringstream reportName;
            reportName << "statechange_" << actionExpr << "_"
                       << std::put_time(&timeInfo, "%Y%m%d_%H%M%S");
            changePlan.setReportPath(reportName.str());
        }

        for (auto &action: actions) {
            {
                auto rollbackAction = std::dynamic_pointer_cast<RollbackAction>(action);
                if (rollbackAction != nullptr) {
                    changePlan.rollbackGids().push_back(rollbackAction->gid());
                }
            }

            {
                auto prependAction = std::dynamic_pointer_cast<PrependAction>(action);
                if (prependAction != nullptr) {
                    changePlan.userQueries().insert({ prependAction->gid(), prependAction->sqlFile() });
                }
            }

            {
                auto fullReplayAction = std::dynamic_pointer_cast<FullReplayAction>(action);
                if (fullReplayAction != nullptr) {
                    changePlan.setFullReplay(true);
                }
            }

            {
                auto autoRollbackAction = std::dynamic_pointer_cast<AutoRollbackAction>(action);
                if (autoRollbackAction != nullptr) {
                    changePlan.setAutoRollbackRatio(autoRollbackAction->ratio());
                }
            }
        }

        std::sort(changePlan.rollbackGids().begin(), changePlan.rollbackGids().end());
    
        StateChanger stateChanger(dbHandlePoolAdapter, changePlan);
        
        if (makeClusterMap) {
            stateChanger.makeCluster();
        } else if (fullReplay) {
            /*
            if (!confirm("Proceed?")) {
                return 2;
            }
             */
            
            stateChanger.fullReplay();
        } else if (replay) {
            stateChanger.replay();
        } else if (autoRollback) {
            stateChanger.bench_prepareRollback();
        } else  {
            describeActions(actions);
            
            /*
            if (!confirm("Proceed?")) {
                return 2;
            }
             */
            
            stateChanger.prepare();
            // stateChanger.start();
        }
       
        return 0;
    }
    
    void DBStateChangeApp::preparePlan(std::vector<std::shared_ptr<Action>> &actions, StateChangePlan &changePlan) {
        auto fail = [this] (std::string reason) {
            _logger->error("requirements not satisfied: {}", reason);
            throw std::runtime_error("requirements not satisfied");
        };
        
        { // @start(dbdump)
            if (isArgSet('b')) {
                changePlan.setDBDumpPath(getArg('b'));
            } else {
                _logger->warn("database dump file is not specified!");
                _logger->warn("- this may leads to unexpected result");
                _logger->warn("- all queries will be executed until gid reaches rollback target");
            }
        } // @end(dbdump)
    
        /*
        { // @start(appendQuery)
            if (isArgSet('A')) {
                changePlan.setUserQueryPath(getArg('A'));
            }
        } // @end(appendQuery)
         */
        
        { // @start(statelog)
            if (!isArgSet('i')) {
                fail("ultraverse state log (.ultstatelog) must be specified");
            }
    
            changePlan.setStateLogName(getArg('i'));
        } // @end(statelog)
    
        { // @start(dbname)
            if (!isArgSet('i')) {
                fail("database name must be specified");
            }
    
            changePlan.setDBName(getArg('d'));
        } // @end(dbname)
    
        { // @start(startGid)
            if (isArgSet('s')) {
                changePlan.setStartGid(std::stoi(getArg('s')));
            }
        } // @end(startGid)
    
        { // @start(endGid)
            if (isArgSet('e')) {
                changePlan.setEndGid(std::stoi(getArg('e')));
            }
        } // @end(endGid)
        
        { // @start(keyColumns)
            if (!isArgSet('k')) {
                fail("key column(s) must be specified");
            }
        
            auto keyColumnGroups = buildKeyColumnGroups(getArg('k'));
            changePlan.setKeyColumnGroups(std::move(keyColumnGroups));
        } // @end (keyColumns)
        
        { // @start(columnAliases)
            if (isArgSet('a')) {
                auto aliases = buildColumnAliasesList(getArg('a'));
    
                changePlan.columnAliases().insert(
                    changePlan.columnAliases().begin(),
                    aliases.begin(), aliases.end()
                );
            }
        } // @end(keyColumns)
        
        { // @start(skipProcessing)
            if (isArgSet('S')) {
                auto skipGids = buildSkipGidList(getArg('S'));
                changePlan.skipGids().insert(
                    changePlan.skipGids().end(),
                    skipGids.begin(), skipGids.end()
                );
            }
        } // @end(skipProcessing)
        
        { // @start(reportPath)
            if (isArgSet('r')) {
                auto reportPath = getArg('r');
                changePlan.setReportPath(reportPath);
            }
        } // @end(reportPath)
        
        { // @start(dropIntermediateDB)
            changePlan.setDropIntermediateDB(!isArgSet('N'));
        }
    
        { // @start(writeStateLog)
            if (isArgSet('w')) {
                changePlan.setWriteStateLog(true);
            }
        } // @end(writeStateLog)

        {
            if (isArgSet('Z')) {
                changePlan.setPerformBenchInsert(true);
            }
        }
    
        { // @start(BINLOG_PATH)
            std::string binlogPath = getEnv("BINLOG_PATH");
            
            changePlan.setBinlogPath(binlogPath.empty() ? "/var/lib/mysql" : binlogPath);
        } // @end(BINLOG_PATH)
        
        { // @start(RANGE_COMP_METHOD)
            std::string rangeComparisonMethodStr = getEnv("RANGE_COMP_METHOD");
            
            if (rangeComparisonMethodStr.empty()) {
                changePlan.setRangeComparisonMethod(RangeComparisonMethod::EQ_ONLY);
            } else {
                if (rangeComparisonMethodStr == "intersect") {
                    changePlan.setRangeComparisonMethod(RangeComparisonMethod::INTERSECT);
                } else if (rangeComparisonMethodStr == "eqonly") {
                    changePlan.setRangeComparisonMethod(RangeComparisonMethod::EQ_ONLY);
                } else {
                    fail("invalid range comparison method");
                }
            }
        } // @end(RANGE_COMP_METHOD)
        
        // FIXME
        changePlan.setStateLogPath(".");
        
        for (auto &action: actions) {
            {
                auto rollbackAction = std::dynamic_pointer_cast<RollbackAction>(action);
                if (rollbackAction != nullptr) {
                    changePlan.rollbackGids().push_back(rollbackAction->gid());
                }
            }
    
            {
                auto prependAction = std::dynamic_pointer_cast<PrependAction>(action);
                if (prependAction != nullptr) {
                    changePlan.userQueries().insert({ prependAction->gid(), prependAction->sqlFile() });
                }
            }
            
            {
                auto fullReplayAction = std::dynamic_pointer_cast<FullReplayAction>(action);
                if (fullReplayAction != nullptr) {
                    changePlan.setFullReplay(true);
                }
            }
            
            {
                auto autoRollbackAction = std::dynamic_pointer_cast<AutoRollbackAction>(action);
                if (autoRollbackAction != nullptr) {
                    changePlan.setAutoRollbackRatio(autoRollbackAction->ratio());
                }
            }
        }
    
        std::sort(changePlan.rollbackGids().begin(), changePlan.rollbackGids().end());
    }
    
    bool DBStateChangeApp::confirm(std::string message) {
        std::cerr << message << " (Y/n) > ";
        std::string input;
        std::cin >> input;
        
        return input == "Y";
    }
    
    std::vector<std::string> DBStateChangeApp::split(const std::string &inputStr, char character) {
        std::vector<std::string> list;
        
        std::stringstream sstream(inputStr);
        std::string string;
        
        while (std::getline(sstream, string, character)) {
            list.push_back(string);
        }
        
        return std::move(list);
    }
    
    std::vector<std::shared_ptr<Action>> DBStateChangeApp::parseActions(std::string expression) {
        std::vector<std::shared_ptr<Action>> actions;
        auto exprs = split(expression, ':');
        
        for (auto &actionExpr: exprs) {
            auto pair = split(actionExpr, '=');
            auto action = pair[0];
            auto strArgs = pair.size() > 1 ? pair[1] : "";
            
            if (action == "make_cluster") {
                actions.emplace_back(std::make_shared<MakeClusterAction>());
            } else if (action == "rollback") {
                if (strArgs == "-") {
                    std::cin >> strArgs;
                    auto args = split(strArgs, ',');
                    for (auto &arg: args) {
                        gid_t gid = std::stoll(arg);
                        actions.emplace_back(std::make_shared<RollbackAction>(gid));
                    }
                } else {
                    auto args = split(strArgs, ',');
                    for (auto &arg: args) {
                        gid_t gid = std::stoll(arg);
                        actions.emplace_back(std::make_shared<RollbackAction>(gid));
                    }
                }
            } else if (action == "auto-rollback") {
                double ratio = std::stod(strArgs);
                actions.emplace_back(std::make_shared<AutoRollbackAction>(ratio));
            } else if (action == "prepend") {
                auto args = split(strArgs, ',');
                if (args.size() != 2) {
                    throw std::runtime_error("invalid arguments");
                }
    
                gid_t gid = std::stoll(args[0]);
                actions.emplace_back(std::make_shared<PrependAction>(gid, args[1]));
            } else if (action == "full-replay") {
                actions.emplace_back(std::make_shared<FullReplayAction>());
            } else if (action == "replay") {
                actions.emplace_back(std::make_shared<ReplayAction>());
            } else {
                throw std::runtime_error("invalid action");
            }
        }
        
        return std::move(actions);
    }
    
    void DBStateChangeApp::describeActions(const std::vector<std::shared_ptr<Action>> &actions) {
        _logger->info("== SUMMARY ==");
        
        int i = 1;
        for (const auto &action: actions) {
            if (action->type() == ActionType::ROLLBACK) {
                const auto rollbackAction = std::dynamic_pointer_cast<RollbackAction>(action);
                _logger->info("[#{}] rollback GID #{}", i++, rollbackAction->gid());
            }
            
            if (action->type() == ActionType::PREPEND) {
                const auto prependAction = std::dynamic_pointer_cast<PrependAction>(action);
                _logger->info("[#{}] prepend {} to GID #{}", i++, prependAction->sqlFile(), prependAction->gid());
            }
        }
    }
    
    std::vector<std::vector<std::string>> DBStateChangeApp::buildKeyColumnGroups(std::string expression) {
        return utility::parseKeyColumnGroups(expression);
    }
    
    std::set<std::pair<std::string, std::string>> DBStateChangeApp::buildColumnAliasesList(std::string expression) {
        std::set<std::pair<std::string, std::string>> aliases;
        
        std::stringstream sstream(expression);
        std::string pairStr;
        
        while (std::getline(sstream, pairStr, ',')) {
            std::stringstream pairStream(pairStr);
            std::string lval;
            std::string rval;
            
            std::getline(pairStream, lval, '=');
            std::getline(pairStream, rval, '=');
            
            _logger->info("creating column alias: {} <=> {}", lval, rval);
            aliases.insert({ lval, rval });
        }
        
        return aliases;
    }
    
    
    std::vector<uint64_t> DBStateChangeApp::buildSkipGidList(std::string gidsStr) {
        std::vector<uint64_t> skipGids;
        
        std::stringstream sstream(gidsStr);
        std::string gid;
        
        while (std::getline(sstream, gid, ',')) {
            _logger->debug("gid {} will be skipped", gid);
            skipGids.push_back(std::stoull(gid));
        }
        
        return skipGids;
    }
}


int main(int argc, char **argv) {
    opterr = 0;
    optind = 1;
    ultraverse::DBStateChangeApp application;
    return application.exec(argc, argv);
}
