#include <iostream>
#include <sstream>

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include <bison_parser.h>
#include <SQLParser.h>

#include "mariadb/state/StateThreadPool.h"
#include "mariadb/state/StateTable.h"
#include "mariadb/state/new/StateChanger.hpp"

#include "mariadb/BinaryLog.hpp"

#include "Application.hpp"
#include "db_state_change.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

namespace ultraverse {
    DBStateChangeApp::DBStateChangeApp():
        _logger(createLogger("statechange"))
    {
        // FIXME
        StateThreadPool::Instance().Resize(1);
    }
    
    std::string DBStateChangeApp::optString() {
        return "s:i:d:g:e:c:DvVh";
    }
    
    int DBStateChangeApp::main() {
        using namespace ultraverse::state::v2;
        
        if (isArgSet('h')) {
            std::cout <<
            "statechange - rollback database state\n"
            "\n"
            "Options: \n"
            "    -s file        database backup (.sql)\n"
            "    -i file        ultraverse state log (.ultstatelog)\n"
            "    -d database    database name\n"
            "    -g gid         gid to rollback\n"
            "    -e columns     key columns (eg. user.id article.id)"
            "    -c threadnum   concurrent processing (default = std::thread::hardware_concurrency() + 1)\n"
            "    -D             dry-run\n"
            "    -v             set logger level to DEBUG\n"
            "    -V             set logger level to TRACE\n"
            "    -h             print this help and exit application\n"
            "\n"
            "Environment Variables: \n"
            "    DB_HOST        Database Host\n"
            "    DB_PORT        Database Port\n"
            "    DB_USER        Database User\n"
            "    DB_PASS        Database Password\n";
            
            return 0;
        }
        
        int threadNum = std::thread::hardware_concurrency() + 1;
        
        if (!isArgSet('i')) {
            _logger->error("FATAL: .ultstatelog file must be specified (-i)");
            return 1;
        } else if (!isArgSet('d')) {
            _logger->error("FATAL: database name must be specified (-d)");
            return 1;
        } else if (!isArgSet('g')) {
            _logger->error("FATAL: gid must be specified (-g)");
            return 1;
        }
        
        if (isArgSet('c')) {
            threadNum = std::stoi(getArg('c'));
        }
    
        StateChangePlan changePlan;
        
        // TODO: 키 컬럼 이름만 여기서 설정하고 실질 조건은 target gid에서
        if (isArgSet('e')) {
            auto keyColumns = buildKeyColumnList(getArg('e'));
            changePlan.keyColumns().insert(
                changePlan.keyColumns().begin(),
                keyColumns.begin(), keyColumns.end()
            );
        }
    
        DBHandlePool<mariadb::DBHandle> dbHandlePool(
            threadNum,
            getEnv("DB_HOST"),
            std::stoi(getEnv("DB_PORT")),
            getEnv("DB_USER"),
            getEnv("DB_PASS")
        );
        
        if (isArgSet('s')) {
            changePlan.setDBDumpPath(getArg('s'));
        } else {
            _logger->warn("database dump file is not specified!");
            _logger->warn("- this may leads to unexpected result");
            _logger->warn("- all queries will be executed until gid reaches rollback target");
        }
        
        changePlan.setStateLogPath(".");
        changePlan.setStateLogName(getArg('i'));
        changePlan.setDBName(getArg('d'));
        changePlan.setRollbackGid(std::stoi(getArg('g')));
    
        StateChanger stateChanger(dbHandlePool, changePlan);
        std::string action(argv()[argc() - 1]);
        
        if (action == "make_clustermap") {
            stateChanger.prepare();
        } else {
            if (!confirm("Proceed?")) {
                return 2;
            }
            stateChanger.start();
            // stateChanger.findCandidateColumn();
        }
        
        return 0;
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
        
        while (std::getline(sstream, column, ' ')) {
            _logger->debug("using {} as key column", column);
            keyColumns.push_back(column);
        }
        
        return keyColumns;
    }
}


int main(int argc, char **argv) {
    ultraverse::DBStateChangeApp application;
    return application.exec(argc, argv);
}
