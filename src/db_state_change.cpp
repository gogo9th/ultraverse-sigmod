#include <iostream>
#include <sstream>

#include "Application.hpp"
#include "db_state_change.hpp"

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
        return "b:i:d:s:g:e:k:a:C:S:r:NwDvVh";
    }
    
    int DBStateChangeApp::main() {
        if (isArgSet('h')) {
            std::cerr <<
            "statechange - rollback database state\n"
            "\n"
            "Usage:\n"
            "    statechange [options] action1:action2:..."
            "\n"
            "Available Actions:\n"
            "    make_cluster           creates row cluster file\n"
            "    rollback=gid           rollbacks specified transaction\n"
            "    prepend=gid,sqlfile    appends query before specified transaction\n"
            "    replay                replays transactions from replay plan\n"
            "    full_replay            full replay (replay all transactions)\n"
            "\n"
            "Options: \n"
            "    -b sqlfile     database backup\n"
            "    -i file        ultraverse state log (.ultstatelog)\n"
            "    -d database    database name\n"
            "    -s gid         start gid\n"
            "    -e gid         end gid\n"
            "    -k columns     key columns (eg. user.id,article.id)\n"
            "    -a colA=colB   column aliases (eg. user.name=user.id,...)\n"
            "    -C threadnum   concurrent processing (default = std::thread::hardware_concurrency() + 1)\n"
            "    -S gid,gid,... list of gids to skip processing\n"
            "    -r reportfile  report file\n"
            "    -N             do not drop intermediate database\n"
            "    -w             write state log which contains state changed\n"
            "    -D             dry-run\n"
            "    -v             set logger level to DEBUG\n"
            "    -V             set logger level to TRACE\n"
            "    -h             print this help and exit application\n"
            "\n"
            "Environment Variables: \n"
            "    DB_HOST           Database Host\n"
            "    DB_PORT           Database Port\n"
            "    DB_USER           Database User\n"
            "    DB_PASS           Database Password\n"
            "    BINLOG_PATH       Path to MySQL-variant binlog (default = /var/lib/mysql)\n"
            "    RANGE_COMP_METHOD Range comparison method (intersect,eqonly; default=eqonly)\n"
            "\n"
            "Notes:\n"
            "    prepare phase writes <stateLogName>.ultreplayplan in state log path\n"
            "    replay action reads the same .ultreplayplan file\n";
            
            return 0;
        }
        
        int threadNum = std::thread::hardware_concurrency() * 2;
        
        if (isArgSet('C')) {
            threadNum = std::stoi(getArg('C'));
        }
        
        if (isArgSet('v')) {
            setLogLevel(spdlog::level::debug);
        }
        
        if (isArgSet('V')) {
            setLogLevel(spdlog::level::trace);
        }
        
        if (
            getEnv("DB_HOST").empty() ||
            getEnv("DB_PORT").empty() ||
            getEnv("DB_USER").empty() ||
            getEnv("DB_PASS").empty()
        ) {
            _logger->error("Database credential not provided - see \"Environment Variables\" section in ./db_state_change -h");
            return 1;
        }
    
        DBHandlePool<mariadb::MySQLDBHandle> dbHandlePool(
            threadNum,
            getEnv("DB_HOST"),
            std::stoi(getEnv("DB_PORT")),
            getEnv("DB_USER"),
            getEnv("DB_PASS")
        );
        DBHandlePoolAdapter<mariadb::MySQLDBHandle> dbHandlePoolAdapter(dbHandlePool);
    
        auto actions = parseActions(argv()[argc() - 1]);
        if (actions.empty()) {
            throw std::runtime_error("");
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
        
        try {
            preparePlan(actions, changePlan);
        } catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
            return 1;
        }
        
        changePlan.setThreadNum(threadNum);
        
        changePlan.setDBHost(getEnv("DB_HOST"));
        changePlan.setDBUsername(getEnv("DB_USER"));
        changePlan.setDBPassword(getEnv("DB_PASS"));
    
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
        
            auto keyColumns = buildKeyColumnList(getArg('k'));
        
            changePlan.keyColumns().insert(
                keyColumns.begin(), keyColumns.end()
            );
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
    
    std::vector<std::string> DBStateChangeApp::buildKeyColumnList(std::string expression) {
        std::vector<std::string> keyColumns;
        
        std::stringstream sstream(expression);
        std::string column;
        
        while (std::getline(sstream, column, ',')) {
            keyColumns.push_back(column);
        }
        
        return keyColumns;
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
    ultraverse::DBStateChangeApp application;
    return application.exec(argc, argv);
}
