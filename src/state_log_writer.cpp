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
    
        BinaryLogSequentialReader seqReader("cheese-binlog.index");
        
        std::unordered_map<uint64_t, std::shared_ptr<TableMapEvent>> tableMap;
        
    
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
            
            if (event->eventType() == event_type::TABLE_MAP) {
                auto tableMapEvent = std::dynamic_pointer_cast<TableMapEvent>(event);
                _logger->info("[ROW] read table map: {}.{}", tableMapEvent->database(), tableMapEvent->table());
                
                tableMap[tableMapEvent->tableId()] = tableMapEvent;
            }
            
            if (event->eventType() == event_type::ROW_EVENT) {
                auto rowEvent = std::dynamic_pointer_cast<RowEvent>(event);
                _logger->info("[ROW] read row event: mapping table with id {}", rowEvent->tableId());
                
                auto &table = tableMap[rowEvent->tableId()];
                rowEvent->mapToTable(*table);
                _logger->info("affected rows: {}", rowEvent->affectedRows());
            }
            
            if (event->eventType() == event_type::ROW_QUERY) {
                auto rowQueryEvent = std::dynamic_pointer_cast<RowQueryEvent>(event);
                _logger->info("[ROW] query executed @ {}", rowQueryEvent->statement());
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
