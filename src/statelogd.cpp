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

#include "mariadb/binlog/MariaDBBinaryLogReader.hpp"
#include "mariadb/binlog/MySQLBinaryLogReader.hpp"
#include "mariadb/binlog/BinaryLogSequentialReader.hpp"

#include "mariadb/state/new/ProcLogReader.hpp"
#include "mariadb/state/new/ProcMatcher.hpp"

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
        _taskExecutor(12)
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
        _binlogReader = std::make_unique<mariadb::MySQLBinaryLogSequentialReader>(".", _binlogIndexPath);

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
                    processTransactionIDEvent(std::dynamic_pointer_cast<mariadb::TransactionIDEvent>(event));
                    break;
                    
                // row events
                case event_type::TABLE_MAP:
                    processTableMapEvent(std::dynamic_pointer_cast<mariadb::TableMapEvent>(event));
                    break;
                case event_type::ROW_EVENT: {
                    auto rowEvent = std::dynamic_pointer_cast<mariadb::RowEvent>(event);
                    auto tableMapEvent = _tableMap[rowEvent->tableId()];
                    
                    auto promise = _taskExecutor.post<std::shared_ptr<state::v2::Query>>([this, rowEvent = std::move(rowEvent), tableMapEvent = std::move(tableMapEvent), pendingRowQueryEvent]() {
                        auto pendingQuery = std::make_shared<state::v2::Query>();
                        processRowEvent(rowEvent, pendingQuery, tableMapEvent);
                        processRowQueryEvent(pendingRowQueryEvent, pendingQuery);
                        
                        return pendingQuery;
                    });
                    
                    auto future = promise->get_future();
                    
                    _pendingQueries.push(future.get());
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
        while (!_pendingQueries.empty()) {
            auto pendingQuery = std::move(_pendingQueries.front());
            _pendingQueries.pop();
            
            // auto pendingQuery = promise->get_future().get();
            
            *_pendingTxn << pendingQuery;
        }
        
        _logger->info("finalizeTransaction: {}", _gid);
        _pendingTxn->setGid(_gid++);
        *_stateLogWriter << *_pendingTxn;
        _pendingTxn = std::make_shared<state::v2::Transaction>();
    }
    
    void finalizeTransaction(std::shared_ptr<ProcCall> procCall) {
        assert(procCall != nullptr);
        
        auto procMatcher = procedureDefinition(procCall->procName());
        int prevIndex = 1;
        
        if (procMatcher == nullptr) {
            _logger->error("procedure definition for {} is not available!", procCall->procName());
            
            // process as normal transaction
            finalizeTransaction();
            return;
        }
        
        while (!_pendingQueries.empty()) {
            auto pendingQuery = std::move(_pendingQueries.front());
            _pendingQueries.pop();
            
            // auto pendingQuery = promise->get_future().get();
            
            assert(pendingQuery != nullptr);
            
            {
                if (isProcedureHint(pendingQuery->statement())) {
                    continue;
                }
                
                auto index = procMatcher->matchForward(pendingQuery->statement(), prevIndex);
                
                if (index == -1) {
                    _logger->error("query not matched: {}", pendingQuery->statement());
                    goto APPEND_QUERY;
                }
                
                for (int i = prevIndex; i < index; i++) {
                    _logger->trace("filling truncated query: {}", i);
                    auto _query = procMatcher->asQuery(i, *procCall);
                    
                    _query->setDatabase(pendingQuery->database());
                    _query->setTimestamp(pendingQuery->timestamp());
                    _query->setFlags(state::v2::Query::FLAG_IS_PROCCALL_RECOVERED_QUERY);
                    
                    *_pendingTxn << _query;
                }
                
                
                prevIndex = index + 1;
            }
            
            APPEND_QUERY:
            *_pendingTxn << pendingQuery;
        }
        
        {
            auto procCallQuery = std::make_shared<state::v2::Query>();
            procCallQuery->setStatement(procCall->statements()[0]);
            procCallQuery->setDatabase(_pendingTxn->queries()[0]->database());
            procCallQuery->setTimestamp(_pendingTxn->queries()[0]->timestamp());
            procCallQuery->setFlags(state::v2::Query::FLAG_IS_PROCCALL_QUERY);
            
            *_pendingTxn << procCallQuery;
            
            
            _pendingTxn->setFlags(
                _pendingTxn->flags() | state::v2::Transaction::FLAG_IS_PROCEDURE_CALL
            );
        }
        
        _pendingTxn->variableSet() = procMatcher->variableSet(*procCall);
        
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
        _pendingTxn->setGid(_gid++);
        *_stateLogWriter << *_pendingTxn;
        _pendingTxn = std::make_shared<state::v2::Transaction>();
    }
    
    void processTransactionIDEvent(std::shared_ptr<mariadb::TransactionIDEvent> event) {
        if (_pendingProcCall != nullptr) {
            auto pendingProcCall = _pendingProcCall;
            _pendingProcCall = nullptr;
            finalizeTransaction(pendingProcCall);
        } else {
            finalizeTransaction();
        }
        
        _logger->info("Transaction ID #{} processed.", event->transactionId());
    }
    
    void processTableMapEvent(std::shared_ptr<mariadb::TableMapEvent> event) {
        // std::scoped_lock<std::mutex> _scopedLock(_tableMapMutex);
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
    
    void processRowEvent(std::shared_ptr<mariadb::RowEvent> event, std::shared_ptr<state::v2::Query> pendingQuery, std::shared_ptr<mariadb::TableMapEvent> tableMapEvent) {
        _logger->trace("[ROW] processing row event");
        
        /*
        _tableMapMutex.lock();
        auto table = _tableMap[event->tableId()];
        // auto &hash = _stateHashMap[table->table()];
         */

        /*
        if (!hash.isInitialized()) {
            hash.init();
        }
         */

        event->mapToTable(*tableMapEvent);
    
        /*
        for (auto &it: _tableMap) {
            if (it.second->database() == table->database()) {
                pendingQuery->setBeforeHash(it.second->table(), _stateHashMap[it.second->table()]);
            }
        }
         */

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

                    // hash += event->rowSet(i);
                    break;
                case mariadb::RowEvent::DELETE:
                    pendingQuery->rowSet().push_back(event->rowSet(i));

                    // hash -= event->rowSet(i);
                    break;
            
                case mariadb::RowEvent::UPDATE:
                    pendingQuery->rowSet().push_back(event->rowSet(i));
                    pendingQuery->changeSet().push_back(event->changeSet(i));

                    // hash -= event->rowSet(i);
                    // hash += event->changeSet(i);
                    break;
            }
        }
        
        /*
        for (auto &it: _tableMap) {
            if (it.second->database() == table->database()) {
                pendingQuery->setAfterHash(it.second->table(), _stateHashMap[it.second->table()]);
            }
        }
         */
        // _tableMapMutex.unlock();

        pendingQuery->setTimestamp(event->timestamp());
        pendingQuery->setAffectedRows(event->affectedRows());

        pendingQuery->setDatabase(tableMapEvent->database());

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

        if (isProcedureHint(event->statement())) {
            std::scoped_lock<std::mutex> _lock(_procLogMutex);

            /*
            if (_pendingProcCall != nullptr) {
                return;
            }
             */
            
            const auto &json = pendingQuery->itemSet().begin()->data_list.at(0).getAs<std::string>();
            _pendingProcCall = prepareProcedureCall(json);
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
        
        pendingQuery->varMap().insert(
            pendingQuery->varMap().end(),
            dummyEvent.varMap().begin(), dummyEvent.varMap().end()
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

    std::shared_ptr<ProcCall> prepareProcedureCall(const std::string &jsonStr) {
        using namespace nlohmann;
        
        _logger->debug(jsonStr);
        
        auto jsonObj = std::move(json::parse(jsonStr));
        
        uint64_t callId = jsonObj.at(0).get<uint64_t>();
        std::string procName = jsonObj.at(1).get<std::string>();
        
        std::vector<std::string> args;
        std::vector<StateData> args2;
        
        for (int i = 2; i < jsonObj.size(); i++) {
            const auto &elem = jsonObj.at(i);
            
            switch (elem.type()) {
                case json::value_t::string:
                    args.push_back(fmt::format("\"{}\"", elem.get<std::string>()));
                    args2.emplace_back(elem.get<std::string>());
                    break;
                case json::value_t::number_integer:
                    args.push_back(std::to_string(elem.get<int64_t>()));
                    args2.emplace_back(elem.get<int64_t>());
                    break;
                case json::value_t::number_unsigned:
                    args.push_back(std::to_string(elem.get<uint64_t>()));
                    args2.emplace_back(elem.get<uint64_t>());
                    break;
                case json::value_t::number_float:
                    args.push_back(std::to_string(elem.get<double>()));
                    args2.emplace_back(elem.get<double>());
                    break;
                case json::value_t::null:
                    args.emplace_back("NULL");
                    args2.emplace_back();
                    break;
                default:
                    _logger->error("unsupported type: {}", elem.type_name());
                    assert(false);
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
        
        // _logger->info("procedure call: {} [{}]", sstream.str(), jsonStr);
        
        auto procCall = std::make_shared<ProcCall>();
        procCall->setCallId(callId);
        procCall->setProcName(procName);
        procCall->statements().push_back(sstream.str());
        procCall->setParameters(args2);
        
        return procCall;
    }
    
    /**
     * returns procedure definition for given name
     */
    std::shared_ptr<state::v2::ProcMatcher> procedureDefinition(const std::string &name) {
        std::lock_guard<std::mutex> lock(_procDefMutex);
        
        const auto it = _procedureDefinitions.find(name);
        
        if (it != _procedureDefinitions.end()) {
            return it->second;
        }
        
        _logger->info("definitions for {} not found. loading from procdef/{}.sql", name, name);
        
        // read entire lines from file
        std::ifstream fstream("procdef/" + name + ".sql");
        
        if (!fstream.is_open()) {
            _logger->error("procdef/{}.sql not found", name);
            return nullptr;
        }
        
        std::string procdef((std::istreambuf_iterator<char>(fstream)), std::istreambuf_iterator<char>());
        auto matcher = std::make_shared<state::v2::ProcMatcher>(procdef);
        
        _procedureDefinitions[name] = matcher;
        
        return matcher;
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

    std::queue<std::shared_ptr<state::v2::Query>> _pendingQueries;
    std::mutex _tableMapMutex;
    
    std::unordered_map<std::string, std::shared_ptr<state::v2::ProcMatcher>> _procedureDefinitions;
    std::mutex _procDefMutex;
    
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
