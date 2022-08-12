#include <iostream>

#include <mysql/mysql.h>
#include <mysql/mariadb_rpl.h>

#include "mariadb/DBHandle.hpp"
#include "mariadb/BinaryLog.hpp"

#include "mariadb/binlog/BinaryLogReader.hpp"

#include "mariadb/state/StateThreadPool.h"

#include "Application.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

class StateLogViewer: public ultraverse::Application {
public:
    StateLogViewer(int argc, char **argv):
        Application(argc, argv),
        _logger(createLogger("StateLogViewer"))
    {
        spdlog::set_level(spdlog::level::trace);
    }
    
    int exec() {
        using namespace ultraverse;
        
        BinaryLogReader reader("cheese-binlog.000015");
        reader.open();
        reader.seek(4);
        
        while (reader.next()) {
            auto event = reader.currentEvent();
            
            if (event == nullptr) {
                continue;
            }
            
            if (event->eventType() == event_type::QUERY) {
                auto queryEvent = std::dynamic_pointer_cast<QueryEvent>(event);
                _logger->info("Query executed @ {}", queryEvent->database());
            }
            
            if (event->eventType() == event_type::TXNID) {
                auto txnIDEvent = std::dynamic_pointer_cast<TransactionIDEvent>(event);
                _logger->info("XID {} committed", txnIDEvent->transactionId());
            }
        }
    
        return 0;
    }
    
private:
    LoggerPtr _logger;
};

int main(int argc, char **argv) {
    StateLogViewer application(argc, argv);
    return application.exec();
}
