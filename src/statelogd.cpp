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

#include "base/TaskExecutor.hpp"
#include "utils/log.hpp"
#include "Application.hpp"

using namespace ultraverse;

class StateLogWriterApp: public ultraverse::Application {
public:
    StateLogWriterApp():
        Application(),
        
        _logger(createLogger("StateLogWriterApp")),
        _taskExecutor(8)
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
        
        std::shared_ptr<mariadb::RowQueryEvent> pendingRowQueryEvent;
    
        while (_binlogReader->next()) {
            auto event = _binlogReader->currentEvent();

            if (event == nullptr) {
                continue;
            }
            
            switch (event->eventType()) {
                case event_type::QUERY:
                    _pendingQueries.push(
                        _taskExecutor.post<std::shared_ptr<state::v2::Query>>([this, event = std::move(event)]() {
                            return processQueryEvent(std::dynamic_pointer_cast<mariadb::QueryEvent>(event));
                        })
                    );
                    break;
                case event_type::TXNID:
                    processTransactionIDEvent(std::dynamic_pointer_cast<mariadb::TransactionIDEvent>(event));
                    break;
                    
                // row events
                case event_type::TABLE_MAP:
                    processTableMapEvent(std::dynamic_pointer_cast<mariadb::TableMapEvent>(event));
                    break;
                case event_type::ROW_EVENT: {
                    _pendingQueries.push(
                        _taskExecutor.post<std::shared_ptr<state::v2::Query>>([this, event = std::move(event), pendingRowQueryEvent = std::move(pendingRowQueryEvent)]() {
                            auto pendingQuery = std::make_shared<state::v2::Query>();
                            processRowEvent(std::dynamic_pointer_cast<mariadb::RowEvent>(event), pendingQuery);
                            processRowQueryEvent(pendingRowQueryEvent, pendingQuery);
            
                            return pendingQuery;
                        })
                    );
                }
                    break;
                case event_type::ROW_QUERY:
                    pendingRowQueryEvent = std::dynamic_pointer_cast<mariadb::RowQueryEvent>(event);
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
    
    std::shared_ptr<state::v2::Query> processQueryEvent(std::shared_ptr<mariadb::QueryEvent> event) {
        auto pendingQuery = std::make_shared<state::v2::Query>();
    
        pendingQuery->setTimestamp(event->timestamp());
        pendingQuery->setDatabase(event->database());
        pendingQuery->setStatement(event->statement());
        
        event->tokenize();
        
        if (event->isDDL()) {
            event->parseDDL();
            
            pendingQuery->setFlags(
                pendingQuery->flags() |
                state::v2::Query::FLAG_IS_DDL
            );
    
            pendingQuery->readSet().insert(
                event->readSet().begin(), event->readSet().end()
            );
            pendingQuery->writeSet().insert(
                event->writeSet().begin(), event->writeSet().end()
            );
    
            auto transaction = std::make_shared<state::v2::Transaction>();
            transaction->setFlags(
                transaction->flags() |
                state::v2::Transaction::FLAG_CONTAINS_DDL
            );
    
            *transaction << pendingQuery;
            transaction->setGid(_gid++);
            transaction->setXid(0);
            
            
    
            *_stateLogWriter << *transaction;
            
            return nullptr;
        } else if (event->isDML()) {
            // rowset, changeset이 없으므로 해시 계산 불가능
            event->parse();
            pendingQuery->readSet().insert(
                event->readSet().begin(), event->readSet().end()
            );
            pendingQuery->writeSet().insert(
                event->writeSet().begin(), event->writeSet().end()
            );
            
            /*
            pendingTxn->setFlags(
                _pendingTxn->flags() |
                state::v2::Transaction::FLAG_UNRELIABLE_HASH
            );
             */
        }
        
        return pendingQuery;
    }
    
    void processTransactionIDEvent(std::shared_ptr<mariadb::TransactionIDEvent> event) {
        while (!_pendingQueries.empty()) {
            auto promise = std::move(_pendingQueries.front());
            _pendingQueries.pop();
            
            auto future = promise->get_future();
            future.wait();
            auto pendingQuery = future.get();
            
            if (pendingQuery == nullptr) {
                continue;
            }
            
            *_pendingTxn << pendingQuery;
        }
    
        _logger->info("Transaction ID #{} processed.", event->transactionId());
        _pendingTxn->setXid(event->transactionId());
        finalizeTransaction();
    }
    
    void processTableMapEvent(std::shared_ptr<mariadb::TableMapEvent> event) {
        std::scoped_lock<std::mutex> _scopedLock(_tableMapMutex);
        _logger->debug("[ROW] read row event: table id {} will be mapped with {}.{}", event->tableId(), event->database(), event->table());
        _tableMap[event->tableId()] = event;
    }
    
    void processRowEvent(std::shared_ptr<mariadb::RowEvent> event, std::shared_ptr<state::v2::Query> pendingQuery) {
        _logger->trace("[ROW] processing row event");
        
        _tableMapMutex.lock();
        auto table = _tableMap[event->tableId()];
        auto &hash = _stateHashMap[event->tableId()];
        
        if (!hash.isInitialized()) {
            hash.init();
        }
        
        event->mapToTable(*table);
        _tableMapMutex.unlock();
    
        pendingQuery->setTimestamp(event->timestamp());
        pendingQuery->setBeforeHash(table->table(), hash);
    
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
    
        pendingQuery->setAfterHash(table->table(), hash);
        pendingQuery->setDatabase(table->database());
    }
    
    void processRowQueryEvent(std::shared_ptr<mariadb::RowQueryEvent> event, std::shared_ptr<state::v2::Query> pendingQuery) {
        pendingQuery->setStatement(event->statement());
        
        mariadb::QueryEvent dummyEvent(pendingQuery->database(), event->statement(), 0);
        dummyEvent.parse();
        pendingQuery->readSet().insert(
            dummyEvent.readSet().begin(), dummyEvent.readSet().end()
        );
        pendingQuery->writeSet().insert(
            dummyEvent.writeSet().begin(), dummyEvent.writeSet().end()
        );
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
    TaskExecutor _taskExecutor;
    
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
    
    
    std::queue<std::shared_ptr<std::promise<std::shared_ptr<state::v2::Query>>>> _pendingQueries;
    std::mutex _tableMapMutex;

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
