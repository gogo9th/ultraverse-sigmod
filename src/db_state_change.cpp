#include <iostream>
#include <sstream>

#include "mariadb/state/StateThreadPool.h"
#include "mariadb/state/StateTable.h"

#include "mariadb/BinaryLog.hpp"

#include "Application.hpp"
#include "db_state_change.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

namespace ultraverse {
    using namespace ultraverse::state::v2;
    
    DBStateChangeApp::DBStateChangeApp():
        _logger(createLogger("statechange"))
    {
        // FIXME: 이제 안 쓰이므로 제거해도 됩니다.
        StateThreadPool::Instance().Resize(1);
    }
    
    std::string DBStateChangeApp::optString() {
        return "b:i:d:s:g:e:k:a:C:S:wDvVh";
    }
    
    int DBStateChangeApp::main() {
        if (isArgSet('h')) {
            std::cout <<
            "statechange - rollback database state\n"
            "\n"
            "Usage:\n"
            "    statechange [options] action"
            "\n"
            "Available Actions:\n"
            "    make_cluster   creates row cluster file\n"
            "    rollback       rollbacks specified transaction and appends query (if provided)\n"
            "    append-only    appends query after specified transaction\n"
            "\n"
            "Options: \n"
            "    -b sqlfile     database backup\n"
            "    -A sqlfile     additional query to append\n"
            "    -i file        ultraverse state log (.ultstatelog)\n"
            "    -d database    database name\n"
            "    -s gid         start gid\n"
            "    -g gid         target gid\n"
            "    -e gid         end gid\n"
            "    -k columns     key columns (eg. user.id,article.id)\n"
            "    -a colA=colB   column aliases (eg. user.name=user.id,...)\n"
            "    -C threadnum   concurrent processing (default = std::thread::hardware_concurrency() + 1)\n"
            "    -S gid,gid,... list of gids to skip processing\n"
            "    -w             write state log which contains state changed\n"
            "    -D             dry-run\n"
            "    -v             set logger level to DEBUG\n"
            "    -V             set logger level to TRACE\n"
            "    -h             print this help and exit application\n"
            "\n"
            "Environment Variables: \n"
            "    DB_HOST        Database Host\n"
            "    DB_PORT        Database Port\n"
            "    DB_USER        Database User\n"
            "    DB_PASS        Database Password\n"
            "    BINLOG_PATH    Path to MySQL-variant binlog (default = /var/lib/mysql)\n";
            
            return 0;
        }
        
        int threadNum = std::thread::hardware_concurrency() + 1;
        
        if (isArgSet('C')) {
            threadNum = std::stoi(getArg('C'));
        }
    
        DBHandlePool<mariadb::DBHandle> dbHandlePool(
            threadNum,
            getEnv("DB_HOST"),
            std::stoi(getEnv("DB_PORT")),
            getEnv("DB_USER"),
            getEnv("DB_PASS")
        );
    
        std::string action(argv()[argc() - 1]);
        StateChangePlan changePlan;
        
        try {
            preparePlan(action, changePlan);
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
            return 1;
        }
    
        StateChanger stateChanger(dbHandlePool, changePlan);
        
        if (action == "make_clustermap") {
            stateChanger.prepare();
        } else if (action == "rollback") {
            if (!confirm("Proceed?")) {
                return 2;
            }
            
            changePlan.setMode(state::v2::ROLLBACK);
            stateChanger.start();
            
        } else if (action == "append-only") {
            changePlan.setMode(state::v2::APPEND_ONLY);
            stateChanger.start();
        } else {
            return 1;
        }
       
        return 0;
    }
    
    void DBStateChangeApp::preparePlan(const std::string &action, StateChangePlan &changePlan) {
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
    
        { // @start(appendQuery)
            if (isArgSet('A')) {
                changePlan.setUserQueryPath(getArg('A'));
            }
        } // @end(appendQuery)
        
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
    
        if (action != "make_clustermap") { // @start(targetGid)
            if (!isArgSet('g')) {
                fail("target gid must be specified");
            }
            
            changePlan.setRollbackGid(std::stoi(getArg('g')));
        } // @end(targetGid)
        
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
                changePlan.keyColumns().begin(),
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
    
        { // @start(writeStateLog)
            if (isArgSet('w')) {
                changePlan.setWriteStateLog(true);
            }
        } // @end(writeStateLog)
    
        { // @start(BINLOG_PATH)
            std::string binlogPath = getEnv("BINLOG_PATH");
            
            changePlan.setBinlogPath(binlogPath.empty() ? "/var/lib/mysql" : binlogPath);
        } // @end(BINLOG_PATH)
        
        // FIXME
        changePlan.setStateLogPath(".");
    }
    
    bool DBStateChangeApp::confirm(std::string message) {
        std::cout << message << " (Y/n) > ";
        std::string input;
        std::cin >> input;
        
        return input == "Y";
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
