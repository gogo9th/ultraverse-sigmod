#include <iostream>

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include "mariadb/DBHandle.hpp"
#include "mariadb/BinaryLog.hpp"

#include "mariadb/state/StateThreadPool.h"

#include "Application.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

class StateLogViewer: public ultraverse::Application {
public:
    StateLogViewer(int argc, char **argv) : Application(argc, argv) {}
    
    int exec() {
        DBConnectionOptions options;
        options.host = "localhost";
        options.port = 3306;
        options.user = "root";
        options.password = "mypass";
        
        StateThreadPool::Instance().initialize(options);
        StateThreadPool::Instance().Resize(std::thread::hardware_concurrency() + 1);
    
        auto binaryLog = std::make_shared<BinaryLog>(StateThreadPool::Instance().GetMySql());
    
        binaryLog->setFileName("cheese-binlog.000017");
        binaryLog->setStartPosition(4);
    
        binaryLog->open();
    
        while (binaryLog->next()) {
            MARIADB_RPL_EVENT *event = binaryLog->currentEvent();
        
            if (event->event_type == QUERY_EVENT) {
                // std::string query = std::string(event->event.query.statement.str, event->event.query.statement.length);
                // std::printf("QUERY EXECUTED: %s\n", query.c_str());
            } else if (event->event_type == XID_EVENT) {
                std::printf("XID SET: %d\n", event->event.xid.transaction_nr);
            } else if (event->event_type == ROTATE_EVENT) {
                std::printf("LOG ROTATION: %s (%d)\n", event->event.rotate.filename.str, event->event.rotate.position);
            } else if (event->event_type == TABLE_MAP_EVENT) {
                std::string database = std::string(event->event.table_map.database.str, event->event.table_map.database.length);
                std::string table = std::string(event->event.table_map.table.str, event->event.table_map.table.length);
                std::string metadata = std::string(event->event.table_map.column_types.str, event->event.table_map.column_types.length);
                std::printf("TABLE_MAP_EVENT: %s.%s (%s)\n", database.c_str(), table.c_str(), metadata.c_str());
            } else if (event->event_type == ANNOTATE_ROWS_EVENT) {
                std::string statement = std::string(event->event.annotate_rows.statement.str, event->event.annotate_rows.statement.length);
                std::printf("ANNOTATE_ROWS_EVENT: %s\n", statement.c_str());
            } else if (event->event_type == WRITE_ROWS_EVENT_V1) {
                std::printf("WRITE_ROWS_EVENT: %d\n", event->event.rows.table_id);
            } else if (event->event_type == BINLOG_CHECKPOINT_EVENT) {
                std::printf("CHECKPOINT: %s\n", event->event.checkpoint.filename.str);
            } else {
                printf("%d, %d\n", event->event_type, event->flags);
            }
    
            mariadb_free_rpl_event(event);
        }
    
        binaryLog->close();
    
    
        return 0;
    }
};

int main(int argc, char **argv) {
    StateLogViewer application(argc, argv);
    return application.exec();
}
