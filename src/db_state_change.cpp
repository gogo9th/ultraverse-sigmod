#include <iostream>

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include "mariadb/state/StateThreadPool.h"
#include "mariadb/state/StateTable.h"

#include "mariadb/BinaryLog.hpp"

#include "Application.hpp"
#include "db_state_change.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

namespace ultraverse {
    int DBStateChangeApp::exec() {
        spdlog::set_level(spdlog::level::debug);
        
        DBHandlePool<mariadb::DBHandle> dbHandlePool(
            1,
            "localhost",
            3306,
            "root",
            "mypass"
        );
        
        StateTable stateTable(dbHandlePool);
        stateTable.updateDefinitions();
        
        
        return 0;
    }
}


int main(int argc, char **argv) {
    ultraverse::DBStateChangeApp application(argc, argv);
    return application.exec();
}
