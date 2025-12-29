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


struct PendingTransaction {
    std::shared_ptr<state::v2::Transaction> transaction;
    
    std::queue<
        std::shared_ptr<std::promise<
            std::shared_ptr<state::v2::Query>
        >>
    > queries;
    
    std::queue<std::shared_ptr<state::v2::Query>> queryObjs;
    
    std::unordered_map<uint64_t, std::shared_ptr<mariadb::TableMapEvent>> tableMaps;
    
    std::shared_ptr<ProcCall> procCall;
    std::mutex _procCallMutex;
    
    std::shared_ptr<mariadb::TransactionIDEvent> tidEvent;
    
    bool flag1 = false;
    std::string tmp;
};


class StateLogWriterApp: public ultraverse::Application {
public:
    StateLogWriterApp():
        Application(),
        
        _logger(createLogger("statelogd")),
        _taskExecutor(1)
    {
    }
    
    std::string optString() override {
        return "b:o:c:r:p:k:dGnQvVh";
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
            "    -k columns     key columns (eg. user.id,article.id)\n"
            "    -c threadnum   concurrent processing (default = std::thread::hardware_concurrency() + 1)\n"
            "    -r file        restore state and resume from given .ultchkpoint file\n"
            "    -d             force discard previous log and start over\n"
            "    -G             print processed transactions with GIDs\n"
            "    -Q             print query statements for processed transactions\n"
            "    -n             do not read binlog.index continuously (quit after reaching EOF)\n"
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
        
        { // @start(keyColumns)
            if (!isArgSet('k')) {
                _logger->error("key column(s) must be specified");
                return 1;
            }
            
            _keyColumns = buildKeyColumnList(getArg('k'));
        } // @end (keyColumns)

        
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
        
        if (isArgSet('G')) {
            _printTransactions = true;
        }

        if (isArgSet('Q')) {
            _printQueries = true;
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
        _binlogReader->setPollDisabled(isArgSet('n'));

        if (isArgSet('p')) {
            _procLogReader = std::make_unique<state::v2::ProcLogReader>();
            _procLogReader->open(".", getArg('p'));
        }


        _stateLogWriter = std::make_unique<state::v2::StateLogWriter>(".", _stateLogName);

        // _pendingTxn = std::make_shared<state::v2::Transaction>();
        // _pendingQuery = std::make_shared<state::v2::Query>();
        
        _writerThread = std::thread([this]() {
            while (!terminateStatus || !_pendingTransactions.empty()) {
                if (_pendingTransactions.empty()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 16));
                    continue;
                }
                
                
                _txnQueueMutex.lock();
                auto promise = _pendingTransactions.front();
                _pendingTransactions.pop();
                _txnQueueMutex.unlock();
                
                auto transaction = std::move(promise->get_future().get());
                
                if (transaction != nullptr) {
                    if (_printTransactions) {
                        _logger->info("writing transaction gid {} (queries: {})",
                                      transaction->gid(),
                                      transaction->queries().size());
                    }

                    if (_printQueries) {
                        size_t queryIndex = 0;
                        for (const auto &query: transaction->queries()) {
                            _logger->info("gid {} query[{}]: {}",
                                          transaction->gid(),
                                          queryIndex++,
                                          query->statement());
                        }
                    }
					}
                *_stateLogWriter << *transaction;
            }
        });

        if (!_checkpointPath.empty()) {
            _stateLogWriter->open(std::ios::out | std::ios::binary | std::ios::app);
            _logger->info("ultraverse state loaded: {}", _checkpointPath);
            int pos;
            std::ifstream is(_checkpointPath, std::ios::binary);
            if (is) {
                cereal::BinaryInputArchive archive(is);
                /*
                archive(_gid);
                archive(pos);
                archive(_tableMap);
                archive(_stateHashMap);
                archive(_pendingTxn);
                archive(_pendingQuery);
                 */

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
        gid_t global_gid = 0;
        std::shared_ptr<PendingTransaction> currentTransaction = std::make_shared<PendingTransaction>();

        while (_binlogReader->next()) {
            auto event = _binlogReader->currentEvent();

            if (event == nullptr) {
                continue;
            }
            
            switch (event->eventType()) {
                case event_type::QUERY:
                    /*
                    currentTransaction->queries.push(
                        _taskExecutor.post<std::shared_ptr<state::v2::Query>>([this, event = std::move(event)]() {
                            return processQueryEvent(std::dynamic_pointer_cast<mariadb::QueryEvent>(event));
                        })
                    );
                     */
                {
                    auto queryEvent = std::dynamic_pointer_cast<mariadb::QueryEvent>(event);
                    if (queryEvent->statement() == "COMMIT") {
                        currentTransaction = std::make_shared<PendingTransaction>();
                    }
                }
                    break;
                case event_type::TXNID: {
                    currentTransaction->tidEvent = std::dynamic_pointer_cast<mariadb::TransactionIDEvent>(event);
                    gid_t gid = global_gid++;
                    _pendingTransactions.push(_taskExecutor.post<std::shared_ptr<state::v2::Transaction>>(
                        [this, currentTransaction = std::move(currentTransaction), gid]() {
                            while (!currentTransaction->queries.empty()) {
                                auto promise = std::move(currentTransaction->queries.front());
                                currentTransaction->queries.pop();
                                
                                currentTransaction->queryObjs.push(
                                    promise->get_future().get()
                                );
                            }
                            
                            return processTransactionIDEvent(currentTransaction, gid);
                        }));
                    
                    
                    while (_pendingTransactions.size() > 128) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1000 / 16));
                    }
                    
                    currentTransaction = std::make_shared<PendingTransaction>();
                }
                    break;
                // row events
                case event_type::TABLE_MAP:
                    processTableMapEvent(currentTransaction, std::dynamic_pointer_cast<mariadb::TableMapEvent>(event));
                    break;
                case event_type::ROW_EVENT: {
                    auto rowEvent = std::dynamic_pointer_cast<mariadb::RowEvent>(event);
                    auto tableMapEvent = currentTransaction->tableMaps[rowEvent->tableId()];
                    
                    auto promise = std::make_shared<std::promise<std::shared_ptr<state::v2::Query>>>();
                    /*
                    auto promise = _taskExecutor.post<std::shared_ptr<state::v2::Query>>([this, currentTransaction, rowEvent = std::move(rowEvent), pendingRowQueryEvent, tableMapEvent]() {
                        auto pendingQuery = std::make_shared<state::v2::Query>();
                        
                        processRowEvent(
                            currentTransaction,
                            rowEvent,
                            pendingRowQueryEvent,
                            pendingQuery,
                            tableMapEvent
                        );
                        // processRowQueryEvent(pendingRowQueryEvent, pendingQuery);
                        
                        return pendingQuery;
                    });
                     */
                    auto pendingQuery = std::make_shared<state::v2::Query>();
                    
                    processRowEvent(
                        currentTransaction,
                        rowEvent,
                        pendingRowQueryEvent,
                        pendingQuery,
                        tableMapEvent
                    );
                    // processRowQueryEvent(pendingRowQueryEvent, pendingQuery);
                    promise->set_value(pendingQuery);
                    
                    currentTransaction->queries.push(promise);
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
        
        terminateStatus = true;
        
        if (_writerThread.joinable()) {
            _writerThread.join();
        }
        
        _stateLogWriter->close();
    }
    
    /**
     * inserts pending query object to transaction
     */
    void finalizeQuery() {
    }
    
    std::shared_ptr<state::v2::Transaction> finalizeTransaction(std::shared_ptr<PendingTransaction> transaction) {
        auto transactionObj = std::make_shared<state::v2::Transaction>();
        
        auto &queries = transaction->queryObjs;
        
        while (!queries.empty()) {
            auto pendingQuery = std::move(queries.front());
            queries.pop();
            
            if (pendingQuery == nullptr) {
                continue;
            }
            
            // auto pendingQuery = promise->get_future().get();
            
            *transactionObj << pendingQuery;
        }
        
        return std::move(transactionObj);
    }
    
    std::shared_ptr<state::v2::Transaction> finalizeTransaction(std::shared_ptr<PendingTransaction> transaction, std::shared_ptr<ProcCall> procCall) {
        assert(procCall != nullptr);
        
        auto transactionObj = std::make_shared<state::v2::Transaction>();
        auto &queries = transaction->queryObjs;
        
        auto procMatcher = procedureDefinition(procCall->procName());
        int prevIndex = 1;
        
        if (procMatcher == nullptr) {
            _logger->error("procedure definition for {} is not available!", procCall->procName());
            
            // process as normal transaction
            return finalizeTransaction(transaction);
        }
        
        while (!queries.empty()) {
            auto pendingQuery = std::move(queries.front());
            queries.pop();
            
            if (pendingQuery == nullptr) {
                continue;
            }
            
            {
                if (isProcedureHint(pendingQuery->statement())) {
                    continue;
                }
                
                auto index = procMatcher->matchForward(pendingQuery->statement(), prevIndex);
                
                if (index == -1) {
                    _logger->error("query not matched: {} at index {}", pendingQuery->statement(), prevIndex);
                    goto APPEND_QUERY;
                }
                
                for (int i = prevIndex; i < index; i++) {
                    auto queries = procMatcher->asQuery(i, *procCall, _keyColumns);
                    for (auto &query : queries) {
                        query->setDatabase(pendingQuery->database());
                        query->setTimestamp(pendingQuery->timestamp());
                        query->setFlags(state::v2::Query::FLAG_IS_PROCCALL_RECOVERED_QUERY);
                        *transactionObj << query;
                    }
                }
                
                
                prevIndex = index;
            }
            
            APPEND_QUERY:
            *transactionObj << pendingQuery;
        }
        
        {
            auto procCallQuery = std::make_shared<state::v2::Query>();
            procCallQuery->setStatement(procCall->statements()[0]);
            procCallQuery->setDatabase(transactionObj->queries()[0]->database());
            procCallQuery->setTimestamp(transactionObj->queries()[0]->timestamp());
            procCallQuery->setFlags(state::v2::Query::FLAG_IS_PROCCALL_QUERY);
            
            *transactionObj << procCallQuery;
            
            
            transactionObj->setFlags(
                transactionObj->flags() | state::v2::Transaction::FLAG_IS_PROCEDURE_CALL
            );
        }
        
        // _pendingTxn->variableSet() = procMatcher->variableSet(*procCall);
        
        // transactionObj->setGid(_gid++);
        
        return std::move(transactionObj);
    }
    
    std::shared_ptr<state::v2::Query> processQueryEvent(std::shared_ptr<mariadb::QueryEvent> event) {
        auto pendingQuery = std::make_shared<state::v2::Query>();
        
        pendingQuery->setTimestamp(event->timestamp());
        pendingQuery->setDatabase(event->database());
        pendingQuery->setStatement(event->statement());

        event->tokenize();
        
        
        if (event->isDDL()) {
            event->parse();
            event->parseDDL();
            event->buildRWSet(_keyColumns);

            pendingQuery->setFlags(
                pendingQuery->flags() |
                state::v2::Query::FLAG_IS_DDL
            );

            pendingQuery->readSet().insert(
                pendingQuery->readSet().end(),
                event->readSet().begin(),event->readSet().end()
            );
            pendingQuery->writeSet().insert(
                pendingQuery->writeSet().end(),
                event->writeSet().begin(), event->writeSet().end()
            );

            /*
            tr->setFlags(
                _pendingTxn->flags() |
                state::v2::Transaction::FLAG_CONTAINS_DDL
            );
             */

        } else if (event->isDML()) {
            // rowset, changeset이 없으므로 해시 계산 불가능
            if (!event->parse()) {
                // HACK: writeSet만이라도 건짐
                event->parseDDL(1);
            }
            
            event->buildRWSet(_keyColumns);
            
            pendingQuery->readSet().insert(
                pendingQuery->readSet().end(),
                event->readSet().begin(), event->readSet().end()
            );
            pendingQuery->writeSet().insert(
                pendingQuery->writeSet().end(),
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
        /*
        
        *_pendingTxn << pendingQuery;
        _pendingTxn->setXid(0);
        _pendingTxn->setGid(_gid++);
        *_stateLogWriter << *_pendingTxn;
        _pendingTxn = std::make_shared<state::v2::Transaction>();
         */
    }
    
    std::shared_ptr<state::v2::Transaction> processTransactionIDEvent(std::shared_ptr<PendingTransaction> transaction, gid_t gid) {
        if (transaction->tidEvent == nullptr) {
            _logger->error("Transaction ID event is not available: {}", gid);
        } else {
            _logger->info("Transaction ID #{} processed.", transaction->tidEvent->transactionId());
        }
        
        if (transaction->procCall != nullptr) {
            auto transactionObj = finalizeTransaction(transaction, transaction->procCall);
            transactionObj->setGid(gid);

            if (_printTransactions) {
                if (transaction->tidEvent == nullptr) {
                    _logger->info("processed transaction gid {}", gid);
                } else {
                    _logger->info("processed transaction gid {} (xid {})", gid, transaction->tidEvent->transactionId());
                }
            }
            
            return transactionObj;
        } else {
            auto transactionObj = finalizeTransaction(transaction);
            transactionObj->setGid(gid);

            if (_printTransactions) {
                if (transaction->tidEvent == nullptr) {
                    _logger->info("processed transaction gid {}", gid);
                } else {
                    _logger->info("processed transaction gid {} (xid {})", gid, transaction->tidEvent->transactionId());
                }
            }
            
            return transactionObj;
        }
    }
    
    void processTableMapEvent(std::shared_ptr<PendingTransaction> transaction, std::shared_ptr<mariadb::TableMapEvent> event) {
        // std::scoped_lock<std::mutex> _scopedLock(_tableMapMutex);
        // _logger->debug("[ROW] read table map event: table id {} will be mapped with {}.{}", event->tableId(), event->database(), event->table());
        
        auto it = std::find_if(transaction->tableMaps.begin(), transaction->tableMaps.end(), [&event](auto &prevEvent) {
            return (
                prevEvent.second->database() == event->database() &&
                prevEvent.second->table() == event->table()
            );
        });
        
        if (it != transaction->tableMaps.end()) {
            transaction->tableMaps.erase(it);
        }
       
        transaction->tableMaps[event->tableId()] = event;
    }
    
    void processRowEvent(std::shared_ptr<PendingTransaction> transaction,
                         std::shared_ptr<mariadb::RowEvent> event,
                         std::shared_ptr<mariadb::RowQueryEvent> rowQueryEvent,
                         std::shared_ptr<state::v2::Query> pendingQuery,
                         std::shared_ptr<mariadb::TableMapEvent> tableMapEvent) {
        
        event->mapToTable(*tableMapEvent);
    
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

        pendingQuery->setTimestamp(event->timestamp());
        pendingQuery->setAffectedRows(event->affectedRows());
        pendingQuery->setStatement(rowQueryEvent->statement());
        pendingQuery->setDatabase(tableMapEvent->database());
        
        
        mariadb::QueryEvent dummyEvent(pendingQuery->database(), rowQueryEvent->statement(), 0);
        
        if (!(event->flags() & 1)) {
            pendingQuery->setFlags(pendingQuery->flags() | state::v2::Query::FLAG_IS_CONTINUOUS);
        }
        
        
        dummyEvent.itemSet().insert(
            dummyEvent.itemSet().end(),
            event->itemSet().begin(), event->itemSet().end()
        );
        
        dummyEvent.itemSet().insert(
            dummyEvent.itemSet().end(),
            event->updateSet().begin(), event->updateSet().end()
        );
        
        if (!dummyEvent.parse()) {
            dummyEvent.parseDDL(1);
        }
        dummyEvent.buildRWSet(_keyColumns);
        
        pendingQuery->readSet().insert(
            pendingQuery->readSet().end(),
            dummyEvent.readSet().begin(), dummyEvent.readSet().end()
        );
        
        pendingQuery->writeSet().insert(
            pendingQuery->writeSet().end(),
            dummyEvent.writeSet().begin(), dummyEvent.writeSet().end()
        );
        
        pendingQuery->varMap().insert(
            pendingQuery->varMap().end(),
            dummyEvent.variableSet().begin(), dummyEvent.variableSet().end()
        );
        
        if (isProcedureHint(rowQueryEvent->statement())) {
            std::scoped_lock lock(transaction->_procCallMutex);
            
            assert(transaction->procCall == nullptr);
            
            const auto &json = pendingQuery->writeSet().begin()->data_list.at(0).getAs<std::string>();
            transaction->procCall = prepareProcedureCall(json);
        }
    }
    
    void sigintHandler(int param) {
        terminateStatus = true;
        
        if (_writerThread.joinable()) {
            _writerThread.join();
        }
        
        _binlogReader->terminate();
    }

    void terminateProcess() {
        std::string checkpointPath = _stateLogName.substr(0, _stateLogName.find_last_of('.')) + ".ultchkpoint";
        int pos = _binlogReader->pos();
        _logger->info("ultraverse state saved: {}", checkpointPath);
        std::ofstream os(checkpointPath, std::ios::binary);
        if (os.is_open()) {
            cereal::BinaryOutputArchive archive(os);
            // archive(_gid, pos, _tableMap, _stateHashMap, _pendingTxn, _pendingQuery);
            os.close();
        }

    }

    bool isProcedureHint(const std::string &statement) {
        return statement.find("INSERT INTO __ULTRAVERSE_PROCEDURE_HINT") == 0;
    }

    std::shared_ptr<ProcCall> prepareProcedureCall(const std::string &jsonStr) {
        using namespace nlohmann;
        
        // _logger->debug(jsonStr);
        
        auto jsonObj = std::move(json::parse(jsonStr));
        
        uint64_t callId = jsonObj.at(0).get<uint64_t>();
        std::string procName = jsonObj.at(1).get<std::string>();
        
        std::vector<std::string> args;
        std::vector<StateData> args2;
        
        for (int i = 2; i < jsonObj.size(); i++) {
            const auto &elem = jsonObj.at(i);
            
            switch (elem.type()) {
                case json::value_t::string: {
                    auto strval = elem.get<std::string>();
                    // strval = utility::replaceAll(strval, "\"", "\\\"");
                    
                    args.push_back(fmt::format("'{}'", strval));
                    args2.emplace_back(elem.get<std::string>());
                }
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
    
    std::vector<std::string> buildKeyColumnList(std::string expression) {
        std::vector<std::string> keyColumns;
        
        std::stringstream sstream(expression);
        std::string column;
        
        while (std::getline(sstream, column, ',')) {
            keyColumns.push_back(column);
        }
        
        return keyColumns;
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
    bool _printTransactions = false;
    bool _printQueries = false;
    
    std::thread _writerThread;
    std::mutex _txnQueueMutex;
    
    std::unique_ptr<mariadb::BinaryLogSequentialReader> _binlogReader;
    std::unique_ptr<state::v2::StateLogWriter> _stateLogWriter;

    std::unique_ptr<state::v2::ProcLogReader> _procLogReader;
    std::mutex _procLogMutex;
    
    std::queue<std::shared_ptr<std::promise<
        std::shared_ptr<state::v2::Transaction>>
    >> _pendingTransactions;

    std::unordered_map<uint64_t, std::shared_ptr<mariadb::TableMapEvent>> _tableMap;
    std::unordered_map<std::string, state::StateHash> _stateHashMap;
    
    
    std::unordered_map<std::string, std::shared_ptr<state::v2::ProcMatcher>> _procedureDefinitions;
    std::mutex _procDefMutex;
    
    std::mutex _txnMutex;
    
    std::vector<std::string> _keyColumns;

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
