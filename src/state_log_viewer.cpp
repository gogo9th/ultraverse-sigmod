#include <iostream>

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include "mariadb/DBHandle.hpp"
#include "mariadb/BinaryLog.hpp"

using namespace ultraverse::mariadb;

int main() {
    auto dbHandle = std::make_shared<DBHandle>();
    auto binaryLog = std::make_shared<BinaryLog>(dbHandle);
    
    dbHandle->connect("localhost", 3306, "root", "mypass");
    
    binaryLog->setFileName("cheese-binlog.000007");
    binaryLog->setStartPosition(4);
    
    binaryLog->open();
    
    while (binaryLog->next()) {
        auto event = binaryLog->currentEvent();
        
        std::printf("%d, %d\n", event->event_type, event->flags);
    
        if (event->event_type == QUERY_EVENT) {
            std::printf("QUERY EXECUTED: %s\n", event->event.query.statement.str);
        } else if (event->event_type == XID_EVENT) {
            std::printf("XID SET: %d\n", event->event.xid.transaction_nr);
        } else if (event->event_type == ROTATE_EVENT) {
            std::printf("LOG ROTATION: %s (%d)\n", event->event.rotate.filename.str, event->event.rotate.position);
        }
        
        
    }
    
    binaryLog->close();
    
    
    std::cout << "Hello, World!" << std::endl;
    return 0;
}
