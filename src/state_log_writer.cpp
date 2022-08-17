#include <iostream>

#include "mariadb/DBHandle.hpp"
#include "mariadb/BinaryLog.hpp"

#include "mariadb/binlog/BinaryLogReader.hpp"

#include "utils/log.hpp"
#include "Application.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

class StateLogWriter: public ultraverse::Application {
public:
    StateLogWriter(int argc, char **argv):
        Application(argc, argv),
        
        _logger(createLogger("StateLogWriter"))
    {
    
    }
    
    int exec() {
        using namespace ultraverse;
        
        spdlog::set_level(spdlog::level::trace);
    
        BinaryLogSequentialReader seqReader("/var/lib/mysql/cheese-binlog.index");
    
        while (seqReader.next()) {
            auto event = seqReader.currentEvent();
        
            if (event == nullptr) {
                continue;
            }
        
            if (event->eventType() == event_type::QUERY) {
                auto queryEvent = std::dynamic_pointer_cast<QueryEvent>(event);
                queryEvent->tokenize();
            
                if (queryEvent->isDDL()) {
                
                } else if (queryEvent->isDML()) {
                
                }
                _logger->info("Query executed @ {}", queryEvent->statement());
            }
        
            if (event->eventType() == event_type::TXNID) {
                auto txnIDEvent = std::dynamic_pointer_cast<TransactionIDEvent>(event);
                _logger->info("XID {} committed", txnIDEvent->transactionId());
            }
        }
        
        /*
        BinaryLogReader reader("cheese-binlog.000021");
        reader.open();
        reader.seek(4);
    
        while (reader.next()) {
            auto event = reader.currentEvent();
        
            if (event == nullptr) {
                continue;
            }
        
            if (event->eventType() == event_type::QUERY) {
                auto queryEvent = std::dynamic_pointer_cast<QueryEvent>(event);
                queryEvent->tokenize();
                
                if (queryEvent->isDDL()) {
                
                } else if (queryEvent->isDML()) {
                
                }
                _logger->info("Query executed @ {}", queryEvent->statement());
            }
        
            if (event->eventType() == event_type::TXNID) {
                auto txnIDEvent = std::dynamic_pointer_cast<TransactionIDEvent>(event);
                _logger->info("XID {} committed", txnIDEvent->transactionId());
            }
        }
         */
    
    
    
    
        return 0;
    }
    
private:
    LoggerPtr _logger;
};

int main(int argc, char **argv) {
    StateLogWriter application(argc, argv);
    return application.exec();
}
