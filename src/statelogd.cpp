#include <cereal/types/unordered_map.hpp>
#include <cereal/types/memory.hpp>
#include <cereal/archives/binary.hpp>
#include <cereal/types/utility.hpp>

#include <iostream>
#include <fstream>
#include <signal.h>

#include <nlohmann/json.hpp>

#include <libultparser/libultparser.h>
#include <ultparser_query.pb.h>

#include "mariadb/state/new/Transaction.hpp"
#include "mariadb/state/new/ColumnDependencyGraph.hpp"
#include "mariadb/state/new/StateLogWriter.hpp"
#include "mariadb/state/StateHash.hpp"

#include "mariadb/DBHandle.hpp"
#include "mariadb/BinaryLog.hpp"

#include "mariadb/binlog/MariaDBBinaryLogReader.hpp"
#include "mariadb/binlog/MySQLBinaryLogReader.hpp"
#include "mariadb/binlog/BinaryLogSequentialReader.hpp"

#include "mariadb/state/new/ProcLogReader.hpp"
#include "mariadb/state/new/ProcMatcher.hpp"

#include "sql/PySQLParser.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/log.hpp"
#include "utils/StringUtil.hpp"
#include "Application.hpp"



using namespace ultraverse;

class StateLogWriterApp: public ultraverse::Application {
public:
    StateLogWriterApp():
        Application(),
        
        _logger(createLogger("statelogd")),
        _taskExecutor(8)
    {
    }
    
    std::string optString() override {
        return "b:o:c:r:p:dMvVh";
    }
    
    int main() override {
        if (isArgSet('h')) {
            std::cout <<
            "statelogd - state-logging daemon\n"
            "\n"
            "Options: \n"
            "    -b file        specify MariaDB-variant binlog.index file\n"
            "    -o file        specify log output name\n"
            "    -p file        use procedure log to append additional queries (SELECT ...)\n"
            "    -c threadnum   concurrent processing (default = std::thread::hardware_concurrency() + 1)\n"
            "    -r file        restore state and resume from given .ultchkpoint file\n"
            "    -d             force discard previous log and start over\n"
            "    -M             treat server variant as MySQL"
            "    -v             set logger level to DEBUG\n"
            "    -V             set logger level to TRACE\n"
            "    -h             print this help and exit application\n";

            return 0;
        }
    
        if (isArgSet('v')) {
            setLogLevel(spdlog::level::debug);
        }
    
        if (isArgSet('V')) {
            setLogLevel(spdlog::level::trace);
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
            _stateLogName = getArg('o');
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
    
    int64_t currentThreadId() {
        auto threadId = std::this_thread::get_id();
        std::hash<std::thread::id> hasher;
        
        return hasher(threadId);
    }
    
    void writerMain() {
        if (isArgSet('M')) {
            _binlogReader = std::make_unique<mariadb::MySQLBinaryLogSequentialReader>(".", _binlogIndexPath);
        } else {
            _binlogReader = std::make_unique<mariadb::MariaDBBinaryLogSequentialReader>(".", _binlogIndexPath);
        }

        if (isArgSet('p')) {
            _procLogReader = std::make_unique<state::v2::ProcLogReader>();
            _procLogReader->open(".", getArg('p'));
        }


        _stateLogWriter = std::make_unique<state::v2::StateLogWriter>(".", _stateLogName);

        _pendingTxn = std::make_shared<state::v2::Transaction>();
        _pendingQuery = std::make_shared<state::v2::Query>();

        if (!_checkpointPath.empty()) {
            _stateLogWriter->open(std::ios::out | std::ios::binary | std::ios::app);
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
        else {
            _stateLogWriter->open(std::ios::out | std::ios::binary);
        }

        std::shared_ptr<mariadb::RowQueryEvent> pendingRowQueryEvent;

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
                    if (isArgSet('M')) {
                        processTransactionIDEvent_MySQL(std::dynamic_pointer_cast<mariadb::TransactionIDEvent>(event));
                    } else {
                        processTransactionIDEvent(std::dynamic_pointer_cast<mariadb::TransactionIDEvent>(event));
                    }
                    break;
                    
                // row events
                case event_type::TABLE_MAP:
                    processTableMapEvent(std::dynamic_pointer_cast<mariadb::TableMapEvent>(event));
                    break;
                case event_type::ROW_EVENT: {
                    _pendingQueries.push(
                        _taskExecutor.post<std::shared_ptr<state::v2::Query>>([this, event = std::move(event), pendingRowQueryEvent]() {
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
    
    void processQueryEvent(std::shared_ptr<mariadb::QueryEvent> event) {
        auto pendingQuery = std::make_shared<state::v2::Query>();
        
        if (event->statement() == "BEGIN") {
            return;
        }
        
        pendingQuery->setTimestamp(event->timestamp());
        pendingQuery->setDatabase(event->database());
        pendingQuery->setStatement(event->statement());

        event->tokenize();
        
        
        if (event->isDDL()) {
            event->parse();
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

            _pendingTxn->setFlags(
                _pendingTxn->flags() |
                state::v2::Transaction::FLAG_CONTAINS_DDL
            );

        } else if (event->isDML()) {
            // rowset, changeset이 없으므로 해시 계산 불가능
            if (!event->parse()) {
                // HACK: writeSet만이라도 건짐
                event->parseDDL(1);
            }
            
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
        
        *_pendingTxn << pendingQuery;
        _pendingTxn->setXid(0);
        finalizeTransaction();
    }
    
    void processTransactionIDEvent_MySQL(std::shared_ptr<mariadb::TransactionIDEvent> event) {
        while (!_pendingQueries.empty()) {
            auto promise = std::move(_pendingQueries.front());
            _pendingQueries.pop();
            
            auto future = promise->get_future();
            future.wait();
            auto pendingQuery = future.get();
            
            if (pendingQuery == nullptr) {
                continue;
            }
            _logger->trace("[{}]: {}", _gid, pendingQuery->statement());
            
            if (_pendingProcCall != nullptr) {
                pendingQuery->setFlags(pendingQuery->flags() | state::v2::Query::FLAG_IS_IGNORABLE);
            }
            
            *_pendingTxn << pendingQuery;
        }
        
        
        if (_pendingProcCall != nullptr) {
            auto procCallQuery = std::make_shared<state::v2::Query>();
            procCallQuery->setStatement(_pendingProcCall->statements()[0]);
            procCallQuery->setDatabase(_pendingTxn->queries()[0]->database());
            procCallQuery->setTimestamp(_pendingTxn->queries()[0]->timestamp());
            procCallQuery->setFlags(state::v2::Query::FLAG_IS_PROCCALL_QUERY);
            // procCallQuery->setFlags();
            
            *_pendingTxn << procCallQuery;
            
            
            _pendingTxn->setFlags(
                _pendingTxn->flags() | state::v2::Transaction::FLAG_IS_PROCEDURE_CALL
            );

            _pendingProcCall = nullptr;
        }
        
        _logger->info("Transaction ID #{} processed.", event->transactionId());
        _pendingTxn->setXid(event->transactionId());
        finalizeTransaction();
    }
    
    void processTransactionIDEvent(std::shared_ptr<mariadb::TransactionIDEvent> event) {
        std::unique_ptr<state::v2::ProcMatcher> procMatcher;
        int prevIndex = 1;

        if (_pendingProcCall != nullptr) {
            procMatcher = std::make_unique<state::v2::ProcMatcher>(_pendingProcCall->statements());
        }
        
        while (!_pendingQueries.empty()) {
            auto promise = std::move(_pendingQueries.front());
            _pendingQueries.pop();

            auto future = promise->get_future();
            future.wait();
            auto pendingQuery = future.get();

            if (pendingQuery == nullptr) {
                continue;
            }

            if (procMatcher != nullptr) {
                int index = procMatcher->matchForward(pendingQuery->statement(), prevIndex);

                if (index != -1) {
                    for (int i = prevIndex; i < index; i++) {
                        _logger->trace("appending query from proclog: {}", _pendingProcCall->statements()[i]);

                        auto query2 = std::make_shared<state::v2::Query>();
                        auto queryEvent = std::make_shared<mariadb::QueryEvent>(
                            pendingQuery->database(), _pendingProcCall->statements()[i], pendingQuery->timestamp()
                        );

                        // rowset, changeset이 없으므로 해시 계산 불가능
                        if (!queryEvent->parse()) {
                            // HACK: readSet이라도 건짐
                            queryEvent->parseSelect();
                        }

                        query2->setDatabase(queryEvent->database());
                        query2->setStatement(queryEvent->statement());
                        query2->setTimestamp(queryEvent->timestamp());
                        query2->setFlags(state::v2::Query::FLAG_IS_PROCCALL_RECOVERED_QUERY);

                        query2->readSet().insert(
                            queryEvent->readSet().begin(), queryEvent->readSet().end()
                        );
                        query2->writeSet().insert(
                            queryEvent->writeSet().begin(), queryEvent->writeSet().end()
                        );

                        query2->itemSet().insert(
                            query2->itemSet().end(),
                            queryEvent->itemSet().begin(), queryEvent->itemSet().end()
                        );

                        query2->whereSet().insert(
                            query2->whereSet().end(),
                            queryEvent->whereSet().begin(), queryEvent->whereSet().end()
                        );

                        *_pendingTxn << query2;
                    }

                    prevIndex = index + 1;
                }
            }

            _logger->trace("[{}]: {}", _gid, pendingQuery->statement());


            *_pendingTxn << pendingQuery;
        }


        if (_pendingProcCall != nullptr) {
            using namespace nlohmann;

            auto callInfo = json::parse(_pendingProcCall->callInfo());

            std::stringstream sstream;
            sstream << "CALL " << _pendingProcCall->procName() << "(";

            auto it = callInfo.begin();
            it++;
            it++;

            while (true) {
                if (it->is_null()) {
                    sstream << "NULL";
                } else {
                    sstream << '\'' << it->get<std::string>() << '\'';
                }

                it++;

                if (it != callInfo.end()) {
                    sstream << ",";
                } else {
                    break;
                }
            }

            sstream << ")";


            auto procCallQuery = std::make_shared<state::v2::Query>();
            auto queryEvent = std::make_shared<mariadb::QueryEvent>(
                _pendingTxn->queries()[0]->database(),
                sstream.str(),
                _pendingTxn->queries()[0]->timestamp()
            );


            procCallQuery->setDatabase(queryEvent->database());
            procCallQuery->setStatement(queryEvent->statement());
            procCallQuery->setTimestamp(queryEvent->timestamp());
            procCallQuery->setFlags(state::v2::Query::FLAG_IS_PROCCALL_QUERY);

            *_pendingTxn << procCallQuery;


            _pendingTxn->setFlags(
                _pendingTxn->flags() | state::v2::Transaction::FLAG_IS_PROCEDURE_CALL
            );
            _pendingProcCall = nullptr;
        }

        _logger->info("Transaction ID #{} processed.", event->transactionId());
        _pendingTxn->setXid(event->transactionId());
        finalizeTransaction();
    }
    
    void processTableMapEvent(std::shared_ptr<mariadb::TableMapEvent> event) {
        std::scoped_lock<std::mutex> _scopedLock(_tableMapMutex);
        _logger->debug("[ROW] read table map event: table id {} will be mapped with {}.{}", event->tableId(), event->database(), event->table());
        
        auto it = std::find_if(_tableMap.begin(), _tableMap.end(), [&event](auto &prevEvent) {
            return (
                prevEvent.second->database() == event->database() &&
                prevEvent.second->table() == event->table()
            );
        });
        
        if (it != _tableMap.end()) {
            _tableMap.erase(it);
        }
       
        _tableMap[event->tableId()] = event;
    }
    
    void processRowEvent(std::shared_ptr<mariadb::RowEvent> event, std::shared_ptr<state::v2::Query> pendingQuery) {
        _logger->trace("[ROW] processing row event");
        
        _tableMapMutex.lock();
        auto table = _tableMap[event->tableId()];
        auto &hash = _stateHashMap[table->table()];

        if (!hash.isInitialized()) {
            hash.init();
        }

        event->mapToTable(*table);
    
        for (auto &it: _tableMap) {
            if (it.second->database() == table->database()) {
                pendingQuery->setBeforeHash(it.second->table(), _stateHashMap[it.second->table()]);
            }
        }

        switch (event->type()) {
            case mariadb::RowEvent::INSERT:
                pendingQuery->setType(state::v2::Query::INSERT);
                break;
            case mariadb::RowEvent::DELETE:
                pendingQuery->setType(state::v2::Query::DELETE);
                break;
            case mariadb::RowEvent::UPDATE:
                pendingQuery->setType(state::v2::Query::UPDATE);
                break;
        }
    
        for (int i = 0; i < event->affectedRows(); i++) {
            switch (event->type()) {
                case mariadb::RowEvent::INSERT:
                    pendingQuery->rowSet().push_back(event->rowSet(i));

                    hash += event->rowSet(i);
                    break;
                case mariadb::RowEvent::DELETE:
                    pendingQuery->rowSet().push_back(event->rowSet(i));

                    hash -= event->rowSet(i);
                    break;
            
                case mariadb::RowEvent::UPDATE:
                    pendingQuery->rowSet().push_back(event->rowSet(i));
                    pendingQuery->changeSet().push_back(event->changeSet(i));

                    hash -= event->rowSet(i);
                    hash += event->changeSet(i);
                    break;
            }
        }
        
        for (auto &it: _tableMap) {
            if (it.second->database() == table->database()) {
                pendingQuery->setAfterHash(it.second->table(), _stateHashMap[it.second->table()]);
            }
        }
        _tableMapMutex.unlock();

        pendingQuery->setTimestamp(event->timestamp());
        pendingQuery->setAffectedRows(event->affectedRows());

        pendingQuery->setDatabase(table->database());

        pendingQuery->itemSet().insert(
            pendingQuery->itemSet().begin(),
            event->itemSet().begin(), event->itemSet().end()
        );
        
        pendingQuery->updateSet().insert(
            pendingQuery->updateSet().begin(),
            event->updateSet().begin(), event->updateSet().end()
        );
        
        if (!(event->flags() & 1)) {
            pendingQuery->setFlags(pendingQuery->flags() | state::v2::Query::FLAG_IS_CONTINUOUS);
        }
    }
    
    void processRowQueryEvent(std::shared_ptr<mariadb::RowQueryEvent> event, std::shared_ptr<state::v2::Query> pendingQuery) {
        pendingQuery->setStatement(event->statement());

        if ((isArgSet('M') || _procLogReader != nullptr) && isProcedureHint(event->statement())) {
            std::scoped_lock<std::mutex> _lock(_procLogMutex);

            if (_pendingProcCall != nullptr) {
                return;
            }
            
            std::string statement;
            
            if (isArgSet('M')) {
                using namespace nlohmann;
                pendingQuery->itemSet().begin()->data_list.at(0).Get(statement);
                
                auto jsonObj = json::parse(statement);
                
                uint64_t callId = jsonObj.at(0).get<uint64_t>();
                std::string procName = jsonObj.at(1).get<std::string>();
                std::vector<std::string> args;
                
                for (int i = 2; i < jsonObj.size(); i++) {
                    const auto &elem = jsonObj.at(i);
                    
                    switch (elem.type()) {
                        case json::value_t::string:
                            args.push_back(elem.get<std::string>());
                            break;
                        case json::value_t::number_integer:
                            args.push_back(std::to_string(elem.get<int>()));
                            break;
                        case json::value_t::number_unsigned:
                            args.push_back(std::to_string(elem.get<unsigned int>()));
                            break;
                        case json::value_t::number_float:
                            args.push_back(std::to_string(elem.get<float>()));
                            break;
                        default:
                            break;
                    }
                }
                
                std::stringstream sstream;
                sstream << "CALL " << procName << "(";
                
                for (int i = 0; i < args.size(); i++) {
                    sstream << args[i];
                    
                    if (i < args.size() - 1) {
                        sstream << ", ";
                    }
                }
                
                sstream << ")";
                
                _pendingProcCall = std::make_shared<ProcCall>();
                _pendingProcCall->setCallId(callId);
                _pendingProcCall->setProcName(procName);
                _pendingProcCall->statements().push_back(sstream.str());
                
                true;
            } else {
                statement = event->statement();
                
                auto pair = extractProcedureHint(statement);
                
                if (_procLogReader->matchForward(pair.second)) {
                    _pendingProcCall = _procLogReader->current();
                }
            }
            

        }


        mariadb::QueryEvent dummyEvent(pendingQuery->database(), event->statement(), 0);
        
        dummyEvent.itemSet().insert(
            dummyEvent.itemSet().begin(),
            pendingQuery->itemSet().begin(), pendingQuery->itemSet().end()
        );
        
        /*
        dummyEvent.whereSet().insert(
            dummyEvent.whereSet().begin(),
            pendingQuery->whereSet().begin(), pendingQuery->whereSet().end()
        );
         */
        
        if (!dummyEvent.parse()) {
            dummyEvent.parseDDL(1);
        }

        pendingQuery->readSet().insert(
            dummyEvent.readSet().begin(), dummyEvent.readSet().end()
        );
        pendingQuery->writeSet().insert(
            dummyEvent.writeSet().begin(), dummyEvent.writeSet().end()
        );
        
        /*
        pendingQuery->itemSet().insert(
            pendingQuery->itemSet().begin(),
            dummyEvent.itemSet().begin(), dummyEvent.itemSet().end()
        );
         */
        pendingQuery->whereSet().insert(
            pendingQuery->whereSet().begin(),
            dummyEvent.whereSet().begin(), dummyEvent.whereSet().end()
        );
        
        pendingQuery->sqlVarMap() = dummyEvent.sqlVarMap();
    }

    void sigintHandler(int param) {
        terminateStatus = true;
        _binlogReader->terminate();
    }

    void terminateProcess() {
        std::string checkpointPath = _stateLogName.substr(0, _stateLogName.find_last_of('.')) + ".ultchkpoint";
        int pos = _binlogReader->pos();
        _logger->info("ultraverse state saved: {}", checkpointPath);
        std::ofstream os(checkpointPath, std::ios::binary);
        if (os.is_open()) {
            cereal::BinaryOutputArchive archive(os);
            archive(_gid, pos, _tableMap, _stateHashMap, _pendingTxn, _pendingQuery);
            os.close();
        }

    }

    bool isProcedureHint(const std::string &statement) {
        return statement.find("INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") == 0;
    }

    std::pair<std::string, uint64_t> extractProcedureHint(const std::string &statement) {
        using namespace nlohmann;
        
        std::string procName = "unknown";
        uint64_t callId = 0;
        
        if (isArgSet('M')) {
            auto jsonObj = json::parse(statement);
            
            callId = jsonObj.at(0).get<uint64_t>();
            procName = jsonObj.at(1).get<std::string>();
            
            return std::make_pair(procName, callId);
        } else {
            int nameConstPos = statement.find("_utf8mb4");
            int nameConstRPos = statement.rfind("COLLATE");
            int lpos = statement.find('\'', nameConstPos) + 1;
            int rpos = statement.rfind('\'', nameConstRPos);
            
            std::string jsonStr = statement.substr(lpos, rpos - lpos);
            // FIXME
            jsonStr.erase(std::remove(jsonStr.begin(), jsonStr.end(), '\\'), jsonStr.end());
            
            fprintf(stderr, "JSON: %s\n", jsonStr.c_str());
            
            auto jsonObj = json::parse(jsonStr);
            
            callId = jsonObj.at(0).get<uint64_t>();
            procName = jsonObj.at(1).get<std::string>();
            
            return std::make_pair(procName, callId);
        }
    }

private:
    LoggerPtr _logger;
    TaskExecutor _taskExecutor;

    std::string _binlogIndexPath;
    std::string _stateLogName;
    
    std::string _checkpointPath;
    bool _discardCheckpoint = false;
    
    int _threadNum = 1;
    
    int _gid = 0;
    
    std::unique_ptr<mariadb::BinaryLogSequentialReader> _binlogReader;
    std::unique_ptr<state::v2::StateLogWriter> _stateLogWriter;

    std::unique_ptr<state::v2::ProcLogReader> _procLogReader;
    std::mutex _procLogMutex;

    std::unordered_map<uint64_t, std::shared_ptr<mariadb::TableMapEvent>> _tableMap;
    std::unordered_map<std::string, state::StateHash> _stateHashMap;
    
    std::shared_ptr<state::v2::Transaction> _pendingTxn;
    std::shared_ptr<state::v2::Query> _pendingQuery;
    std::shared_ptr<ProcCall> _pendingProcCall;

    std::queue<std::shared_ptr<std::promise<std::shared_ptr<state::v2::Query>>>> _pendingQueries;
    std::mutex _tableMapMutex;
    
    std::mutex _txnMutex;

    bool terminateStatus = false;
};

StateLogWriterApp application;

void sigintHandler(int param) {
    application.sigintHandler(param);
}

int main(int argc, char **argv) {
    signal(SIGINT, sigintHandler);
    
    ult_sql_parser_init();
    int retval = application.exec(argc, argv);
    
    ult_sql_parser_deinit();
    return retval;
}
