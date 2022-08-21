#include <iostream>

#include "mariadb/state/new/Transaction.hpp"
#include "mariadb/state/new/StateLogWriter.hpp"
#include "mariadb/state/StateHash.hpp"
#include "mariadb/DBHandle.hpp"
#include "mariadb/BinaryLog.hpp"

#include "mariadb/binlog/BinaryLogReader.hpp"

#include "utils/log.hpp"
#include "Application.hpp"

using namespace ultraverse::mariadb;
using namespace ultraverse::state;

class StateLogWriterApp: public ultraverse::Application {
public:
    StateLogWriterApp(int argc, char **argv):
        Application(argc, argv),
        
        _logger(createLogger("StateLogWriterApp"))
    {
    
    }
    
    int exec() {
        using namespace ultraverse;
        
        spdlog::set_level(spdlog::level::trace);
    
        BinaryLogSequentialReader seqReader("cheese-binlog.index");
        
        std::unordered_map<uint64_t, std::shared_ptr<TableMapEvent>> tableMap;
        std::unordered_map<uint64_t, StateHash> stateHashMap;
        
        v2::StateLogWriter stateLogWriter("cheese-binlog.ultstate");
        stateLogWriter.open();
        
        auto pendingTxn = std::make_shared<v2::Transaction>();
        auto pendingQuery = std::make_shared<v2::Query>();
    
        while (seqReader.next()) {
            auto event = seqReader.currentEvent();
        
            if (event == nullptr) {
                continue;
            }
        
            if (event->eventType() == event_type::QUERY) {
                auto queryEvent = std::dynamic_pointer_cast<QueryEvent>(event);
                queryEvent->tokenize();
                
                pendingQuery->setDatabase(queryEvent->database());
                pendingQuery->setStatement(queryEvent->statement());
                
                *pendingTxn << pendingQuery;
                pendingTxn->setFlags(
                    pendingTxn->flags() |
                    v2::Transaction::FLAG_UNRELIABLE_HASH
                );
                
                pendingQuery = std::make_shared<v2::Query>();
                
                
            
                if (queryEvent->isDDL()) {
                
                } else if (queryEvent->isDML()) {
                
                }
                _logger->info("Query executed @ {}", queryEvent->statement());
            }
        
            if (event->eventType() == event_type::TXNID) {
                auto txnIDEvent = std::dynamic_pointer_cast<TransactionIDEvent>(event);
                _logger->info("XID {} committed", txnIDEvent->transactionId());
                
                pendingTxn->setXid(txnIDEvent->transactionId());
                stateLogWriter << *pendingTxn;
                
                pendingTxn = std::make_shared<v2::Transaction>();
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
                auto &hash = stateHashMap[rowEvent->tableId()];
                rowEvent->mapToTable(*table);
                
                pendingQuery->setBeforeHash(table->table(), hash);
    
                for (int i = 0; i < rowEvent->affectedRows(); i++) {
                    switch (rowEvent->type()) {
                        case RowEvent::INSERT:
                            hash += rowEvent->rowSet(i);
                            break;
                        case RowEvent::DELETE:
                            hash -= rowEvent->rowSet(i);
                            break;
                            
                        case RowEvent::UPDATE:
                            hash -= rowEvent->rowSet(i);
                            hash += rowEvent->changeSet(i);
                            break;
                    }
    
                    hash.hexdump();
                }
    
                pendingQuery->setAfterHash(table->table(), hash);
                
                pendingQuery->setDatabase(table->database());
                
                *pendingTxn << pendingQuery;
                pendingTxn->setFlags(
                    pendingTxn->flags() |
                    v2::Transaction::FLAG_UNRELIABLE_HASH
                );
                
                pendingQuery = std::make_shared<v2::Query>();
                
    
                _logger->info("affected rows: {}", rowEvent->affectedRows());
            }
            
            if (event->eventType() == event_type::ROW_QUERY) {
                auto rowQueryEvent = std::dynamic_pointer_cast<RowQueryEvent>(event);
    
                pendingQuery->setStatement(rowQueryEvent->statement());
                
                _logger->info("[ROW] query executed @ {}", rowQueryEvent->statement());
            }
        }
        
        stateLogWriter.close();
        
        return 0;
    }
    
private:
    LoggerPtr _logger;
};

int main(int argc, char **argv) {
    StateLogWriterApp application(argc, argv);
    return application.exec();
}
