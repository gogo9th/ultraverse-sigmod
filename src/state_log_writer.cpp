#include <iostream>

#include "mariadb/state/new/Transaction.hpp"
#include "mariadb/state/new/StateLogWriter.hpp"
#include "mariadb/state/StateHash.hpp"
#include "mariadb/DBHandle.hpp"
#include "mariadb/BinaryLog.hpp"

#include "mariadb/binlog/BinaryLogReader.hpp"

#include "utils/log.hpp"
#include "Application.hpp"

using namespace ultraverse;

class StateLogWriterApp: public ultraverse::Application {
public:
    StateLogWriterApp():
        Application(),
        
        _logger(createLogger("StateLogWriterApp"))
    {
    
    }
    
    std::string optString() override {
        return "b:o:c:r:dvVh";
    }
    
    int main() override {
        if (isArgSet('h')) {
            std::cout <<
            "statelogd - state-logging daemon\n"
            "\n"
            "Options: \n"
            "    -b file        specify MySQL-variant binlog.index file\n"
            "    -o file        specify log output (.ultstatelog)\n"
            "    -c threadnum   concurrent processing (default = std::thread::hardware_concurrency() + 1)\n"
            "    -r file        restore state and resume from given .ultchkpoint file\n"
            "    -d             force discard previous log and start over\n"
            "    -v             set logger level to DEBUG\n"
            "    -V             set logger level to TRACE\n"
            "    -h             print this help and exit application\n";

            return 0;
        }
        
        if (isArgSet('v')) {
            spdlog::set_level(spdlog::level::debug);
        } else if (isArgSet('V')) {
            spdlog::set_level(spdlog::level::trace);
        }
        
        if (!isArgSet('b')) {
            _logger->error("FATAL: binlog.index file must be specified (-b)");
            return 1;
        } else {
            _binlogIndexPath = getArg('b');
        }
        
        if (!isArgSet('o')) {
            _logger->error("FATAL: output.ultstatelog file must be specified (-o)");
            return 1;
        } else {
            _stateLogPath = getArg('o');
        }
        
        _threadNum = isArgSet('c') ?
            std::stoi(getArg('c')) :
            std::thread::hardware_concurrency() + 1;
        
        _checkpointPath = isArgSet('r') ?
            getArg('r') :
            "statelogd.ultchkpoint";
        
        
        writerMain();
        return 0;
    }
    
    [[noreturn]]
    void writerMain() {
        _binlogReader = std::make_unique<mariadb::BinaryLogSequentialReader>(_binlogIndexPath);
        _stateLogWriter = std::make_unique<state::v2::StateLogWriter>(_stateLogPath);
        
        _stateLogWriter->open();
        
        _pendingTxn = std::make_shared<state::v2::Transaction>();
        _pendingQuery = std::make_shared<state::v2::Query>();
    
        while (_binlogReader->next()) {
            auto event = _binlogReader->currentEvent();
        
            if (event == nullptr) {
                continue;
            }
            
            switch (event->eventType()) {
                case event_type::QUERY:
                    processQueryEvent(std::dynamic_pointer_cast<mariadb::QueryEvent>(event));
                    break;
                case event_type::TXNID:
                    processTransactionIDEvent(std::dynamic_pointer_cast<mariadb::TransactionIDEvent>(event));
                    break;
                    
                // row events
                case event_type::TABLE_MAP:
                    processTableMapEvent(std::dynamic_pointer_cast<mariadb::TableMapEvent>(event));
                    break;
                case event_type::ROW_EVENT:
                    processRowEvent(std::dynamic_pointer_cast<mariadb::RowEvent>(event));
                    break;
                case event_type::ROW_QUERY:
                    processRowQueryEvent(std::dynamic_pointer_cast<mariadb::RowQueryEvent>(event));
                    break;
                    
                default:
                    break;
            }
        }
    }
    
    /**
     * inserts pending query object to transaction
     */
    void finalizeQuery() {
        *_pendingTxn << _pendingQuery;
        _pendingQuery = std::make_shared<state::v2::Query>();
    }
    
    void finalizeTransaction() {
        *_stateLogWriter << *_pendingTxn;
        _pendingTxn = std::make_shared<state::v2::Transaction>();
    }
    
    void processQueryEvent(std::shared_ptr<mariadb::QueryEvent> event) {
        event->tokenize();
        
        if (event->isDDL()) {
        
        } else if (event->isDML()) {
        
        }
        
        _pendingQuery->setDatabase(event->database());
        _pendingQuery->setStatement(event->statement());
        
        finalizeQuery();
        _pendingTxn->setFlags(
            _pendingTxn->flags() |
            state::v2::Transaction::FLAG_UNRELIABLE_HASH
        );
    }
    
    void processTransactionIDEvent(std::shared_ptr<mariadb::TransactionIDEvent> event) {
        _logger->info("Transaction ID #{} processed.", event->transactionId());
        _pendingTxn->setXid(event->transactionId());
        finalizeTransaction();
    }
    
    void processTableMapEvent(std::shared_ptr<mariadb::TableMapEvent> event) {
        _logger->debug("[ROW] read row event: table id {} will be mapped with {}.{}", event->tableId(), event->database(), event->table());
        _tableMap[event->tableId()] = event;
    }
    
    void processRowEvent(std::shared_ptr<mariadb::RowEvent> event) {
        _logger->trace("[ROW] processing row event");
        
        auto &table = _tableMap[event->tableId()];
        auto &hash = _stateHashMap[event->tableId()];
        event->mapToTable(*table);
    
        _pendingQuery->setBeforeHash(table->table(), hash);
    
        for (int i = 0; i < event->affectedRows(); i++) {
            switch (event->type()) {
                case mariadb::RowEvent::INSERT:
                    hash += event->rowSet(i);
                    break;
                case mariadb::RowEvent::DELETE:
                    hash -= event->rowSet(i);
                    break;
            
                case mariadb::RowEvent::UPDATE:
                    hash -= event->rowSet(i);
                    hash += event->changeSet(i);
                    break;
            }
        
            hash.hexdump();
        }
    
        _pendingQuery->setAfterHash(table->table(), hash);
        _pendingQuery->setDatabase(table->database());
        finalizeQuery();
    }
    
    void processRowQueryEvent(std::shared_ptr<mariadb::RowQueryEvent> event) {
        _pendingQuery->setStatement(event->statement());
    }
    
private:
    LoggerPtr _logger;
    std::string _binlogIndexPath;
    std::string _stateLogPath;
    
    std::string _checkpointPath;
    bool _discardCheckpoint = false;
    
    int _threadNum = 1;
    
    
    std::unique_ptr<mariadb::BinaryLogSequentialReader> _binlogReader;
    std::unique_ptr<state::v2::StateLogWriter> _stateLogWriter;
    
    std::unordered_map<uint64_t, std::shared_ptr<mariadb::TableMapEvent>> _tableMap;
    std::unordered_map<uint64_t, state::StateHash> _stateHashMap;
    
    std::shared_ptr<state::v2::Transaction> _pendingTxn;
    std::shared_ptr<state::v2::Query> _pendingQuery;
};

int main(int argc, char **argv) {
    StateLogWriterApp application;
    return application.exec(argc, argv);
}
