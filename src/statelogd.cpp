#include <cereal/types/unordered_map.hpp>
#include <cereal/types/memory.hpp>
#include <cereal/archives/binary.hpp>
#include <cereal/types/utility.hpp>

#include <iostream>
#include <fstream>
#include <signal.h>

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

        if (isArgSet('r')) {
            _checkpointPath = getArg('r');
        }
        
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

        if (!_checkpointPath.empty()) {
            _logger->info("ultraverse state loaded: {}", _checkpointPath);
            int pos;
            std::ifstream is(_checkpointPath, std::ios::binary);
            if (is) {
                cereal::BinaryInputArchive archive(is);
                archive(_gid);
                archive(pos);
                archive(_tableMap);
                archive(_stateHashMap);
                archive(_pendingTxn);
                archive(_pendingQuery);

                _logger->info("gid: {}", _gid);
            } else {
                throw std::runtime_error(
                        fmt::format("cannot find file {}.", _checkpointPath)
                );
            }
            if (_gid >= _binlogReader->logFileListSize()) {
                _gid = _binlogReader->logFileListSize() - 1;
            }
            _binlogReader->seek(_gid, pos);
        }
    
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
            if (terminateStatus) {
                break;
            }
        }
        terminateProcess();
    }
    
    /**
     * inserts pending query object to transaction
     */
    void finalizeQuery() {
        *_pendingTxn << _pendingQuery;
        _pendingQuery = std::make_shared<state::v2::Query>();
    }
    
    void finalizeTransaction() {
        _logger->info("finalizeTransaction: {}", _gid);
        _pendingTxn->setGid(_gid++);
        *_stateLogWriter << *_pendingTxn;
        _pendingTxn = std::make_shared<state::v2::Transaction>();
    }
    
    void processQueryEvent(std::shared_ptr<mariadb::QueryEvent> event) {
        event->tokenize();
        
        if (event->isDDL()) {
            _pendingQuery->setFlags(
                _pendingQuery->flags() |
                state::v2::Query::FLAG_IS_DDL
            );
            
            _pendingTxn->setFlags(
                _pendingTxn->flags() |
                state::v2::Transaction::FLAG_CONTAINS_DDL
            );
        } else if (event->isDML()) {
            // rowset, changeset이 없으므로 해시 계산 불가능
            _pendingTxn->setFlags(
                _pendingTxn->flags() |
                state::v2::Transaction::FLAG_UNRELIABLE_HASH
            );
        }
        
        _pendingQuery->setDatabase(event->database());
        _pendingQuery->setStatement(event->statement());
        
        finalizeQuery();
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
        }
    
        _pendingQuery->setAfterHash(table->table(), hash);
        _pendingQuery->setDatabase(table->database());
        finalizeQuery();
    }
    
    void processRowQueryEvent(std::shared_ptr<mariadb::RowQueryEvent> event) {
        _pendingQuery->setStatement(event->statement());
    }

    void sigintHandler(int param) {
        terminateStatus = true;
        _binlogReader->terminate();
    }

    void terminateProcess() {
        std::string checkpointPath = _stateLogPath.substr(0, _stateLogPath.find_last_of('.')) + ".ultchkpoint";
        int pos = _binlogReader->pos();
        _logger->info("ultraverse state saved: {}", checkpointPath);
        std::ofstream os(checkpointPath, std::ios::binary);
        if (os.is_open()) {
            cereal::BinaryOutputArchive archive(os);
            archive(_gid, pos, _tableMap, _stateHashMap, _pendingTxn, _pendingQuery);
            os.close();
        }

    }
    
private:
    LoggerPtr _logger;
    std::string _binlogIndexPath;
    std::string _stateLogPath;
    
    std::string _checkpointPath;
    bool _discardCheckpoint = false;
    
    int _threadNum = 1;
    
    int _gid = 0;
    
    std::unique_ptr<mariadb::BinaryLogSequentialReader> _binlogReader;
    std::unique_ptr<state::v2::StateLogWriter> _stateLogWriter;

    std::unordered_map<uint64_t, std::shared_ptr<mariadb::TableMapEvent>> _tableMap;
    std::unordered_map<uint64_t, state::StateHash> _stateHashMap;
    
    std::shared_ptr<state::v2::Transaction> _pendingTxn;
    std::shared_ptr<state::v2::Query> _pendingQuery;

    bool terminateStatus = false;
};

StateLogWriterApp application;

void sigintHandler(int param) {
    application.sigintHandler(param);
}

int main(int argc, char **argv) {
    signal(SIGINT, sigintHandler);
    return application.exec(argc, argv);
}
