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
    
    binaryLog->setFileName("cheese-binlog.000011");
    binaryLog->setStartPosition(4);
    
    binaryLog->open();
    
    while (binaryLog->next()) {
        auto event = binaryLog->currentEvent();
    
        if (event->event_type == QUERY_EVENT) {
            std::printf("QUERY EXECUTED: %s\n", event->event.query.statement.str);
        } else if (event->event_type == XID_EVENT) {
            std::printf("XID SET: %d\n", event->event.xid.transaction_nr);
        }
        
    }
    
    binaryLog->close();
    
    
    std::cout << "Hello, World!" << std::endl;
    return 0;
}
