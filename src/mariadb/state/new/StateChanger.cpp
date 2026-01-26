//
// Created by cheesekun on 8/29/22.
//

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <sstream>

#include <fmt/color.h>

#include "StateLogWriter.hpp"
#include "GIDIndexWriter.hpp"
#include "cluster/RowCluster.hpp"

#include "cluster/StateCluster.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/StringUtil.hpp"

#include "StateChangeReport.hpp"
#include "StateLogReader.hpp"

#include "StateChanger.hpp"

namespace ultraverse::state::v2 {
    
    const std::string StateChanger::QUERY_TAG_STATECHANGE = "/* STATECHANGE_QUERY */ ";

    namespace {
        std::string hexEncode(const std::string &data) {
            static const char kHex[] = "0123456789ABCDEF";
            std::string out;
            out.reserve(data.size() * 2);
            for (unsigned char c : data) {
                out.push_back(kHex[c >> 4]);
                out.push_back(kHex[c & 0x0F]);
            }
            return out;
        }

        std::string quoteUserVarName(const std::string &name) {
            std::string out;
            out.reserve(name.size() + 2);
            out.push_back('`');
            for (char ch : name) {
                if (ch == '`') {
                    out.push_back('`');
                }
                out.push_back(ch);
            }
            out.push_back('`');
            return out;
        }

        uint64_t readUint64LE(const std::string &data) {
            uint64_t value = 0;
            size_t len = std::min<size_t>(data.size(), 8);
            for (size_t i = 0; i < len; i++) {
                value |= (static_cast<uint64_t>(static_cast<unsigned char>(data[i])) << (8 * i));
            }
            return value;
        }

        std::string decodeDecimalUserVar(const std::string &data) {
            if (data.size() < 2) {
                return "0";
            }

            uint8_t precision = static_cast<uint8_t>(data[0]);
            uint8_t scale = static_cast<uint8_t>(data[1]);
            const uint8_t *raw = reinterpret_cast<const uint8_t *>(data.data() + 2);
            size_t rawSize = data.size() - 2;

            size_t size = (precision + 1) / 2;
            if (size > rawSize) {
                size = rawSize;
            }

            bool sign = true;
            uint64_t high = 0;
            uint64_t low = 0;

            for (size_t i = 0; i < size; i++) {
                uint8_t value = raw[i];
                if (i == 0) {
                    sign = (value & 0x80) != 0;
                    value ^= 0x80;
                }

                if (i < ((precision - scale) + 1) / 2) {
                    high = (high << 8) + value;
                } else {
                    low = (low << 8) + value;
                }
            }

            std::ostringstream stream;
            if (!sign) {
                stream << "-";
            }
            stream << high;
            if (scale > 0) {
                stream << "." << std::setfill('0') << std::setw(scale) << low;
            }
            return stream.str();
        }

        std::string formatUserVarValue(const Query::UserVar &userVar) {
            if (userVar.isNull) {
                return "NULL";
            }

            switch (userVar.type) {
                case Query::UserVar::STRING: {
                    // TODO: charset / collation mapping
                    return "_binary 0x" + hexEncode(userVar.value);
                }
                case Query::UserVar::REAL: {
                    uint64_t bits = readUint64LE(userVar.value);
                    double value = 0.0;
                    std::memcpy(&value, &bits, sizeof(value));
                    std::ostringstream stream;
                    stream << std::setprecision(17) << value;
                    return stream.str();
                }
                case Query::UserVar::INT: {
                    uint64_t raw = readUint64LE(userVar.value);
                    if (userVar.isUnsigned) {
                        return std::to_string(raw);
                    }
                    return std::to_string(static_cast<int64_t>(raw));
                }
                case Query::UserVar::DECIMAL:
                    return decodeDecimalUserVar(userVar.value);
            }

            return "NULL";
        }

        StateChangerIO makeDefaultIO(const StateChangePlan &plan) {
            StateChangerIO io;
            io.stateLogReader = std::make_unique<StateLogReader>(plan.stateLogPath(), plan.stateLogName());
            io.clusterStore = std::make_unique<FileStateClusterStore>(plan.stateLogPath(), plan.stateLogName());
            io.backupLoader = std::make_unique<MySQLBackupLoader>(plan.dbHost(), plan.dbUsername(), plan.dbPassword());
            io.closeStandardFds = true;
            return io;
        }
    }
    
    StateChanger::StateChanger(mariadb::DBHandlePoolBase &dbHandlePool, const StateChangePlan &plan):
        StateChanger(dbHandlePool, plan, makeDefaultIO(plan))
    {
    }

    StateChanger::StateChanger(mariadb::DBHandlePoolBase &dbHandlePool, const StateChangePlan &plan, StateChangerIO io):
        _logger(createLogger("StateChanger")),
        _dbHandlePool(dbHandlePool),
        _mode(OperationMode::NORMAL),
        _plan(plan),
        _intermediateDBName(fmt::format("ult_intermediate_{}_{}", (int) time(nullptr), getpid())), // FIXME
        _reader(std::move(io.stateLogReader)),
        _clusterStore(std::move(io.clusterStore)),
        _backupLoader(std::move(io.backupLoader)),
        _closeStandardFds(io.closeStandardFds),
        _columnGraph(std::make_unique<ColumnDependencyGraph>()),
        _tableGraph(std::make_unique<TableDependencyGraph>()),
        _context(new StateChangeContext),
        _replayedQueries(0)
    {
        if (_reader == nullptr) {
            _reader = std::make_unique<StateLogReader>(plan.stateLogPath(), plan.stateLogName());
        }

        if (_clusterStore == nullptr) {
            _clusterStore = std::make_unique<FileStateClusterStore>(plan.stateLogPath(), plan.stateLogName());
        }

        if (_backupLoader == nullptr) {
            _backupLoader = std::make_unique<MySQLBackupLoader>(plan.dbHost(), plan.dbUsername(), plan.dbPassword());
        }
    }
    
    void StateChanger::fullReplay() {
        _mode = OperationMode::FULL_REPLAY;
        StateChangeReport report(StateChangeReport::EXECUTE, _plan);
        
        createIntermediateDB();
        report.setIntermediateDBName(_intermediateDBName);
        
        if (!_plan.dbDumpPath().empty()) {
            auto load_backup_start = std::chrono::steady_clock::now();
            loadBackup(_intermediateDBName, _plan.dbDumpPath());
            
            auto dbHandle = _dbHandlePool.take();
            updatePrimaryKeys(dbHandle->get(), 0);
            updateForeignKeys(dbHandle->get(), 0);
            auto load_backup_end = std::chrono::steady_clock::now();
            
            std::chrono::duration<double> time = load_backup_end - load_backup_start;
            _logger->info("LOAD BACKUP END: {}s elapsed", time.count());
            report.setSQLLoadTime(time.count());
        }
        
        _logger->info("opening state log");
        _reader->open();
        
        _isRunning = true;
        
        auto phase_main_start = std::chrono::steady_clock::now();
        
        while (_reader->nextHeader()) {
            auto transactionHeader = _reader->txnHeader();
            auto pos = _reader->pos() - sizeof(TransactionHeader);
            
            _reader->nextTransaction();
            auto transaction = _reader->txnBody();
            auto gid = transactionHeader->gid;
            auto flags = transactionHeader->flags;
            
            if (_plan.isRollbackGid(gid)) {
                _logger->info("skipping rollback transaction #{}", gid);
                continue;
            }
            
            auto dbHandle = _dbHandlePool.take();
            auto &handle = dbHandle->get();
 
            // _logger->info("replaying transaction #{}", gid);
            
            handle.executeQuery("USE " + _intermediateDBName);
            handle.executeQuery("START TRANSACTION");
            
            bool isProcedureCall = transaction->flags() & Transaction::FLAG_IS_PROCEDURE_CALL;
            
            try {
                for (const auto &query: transaction->queries()) {
                    bool isProcedureCallQuery = query->flags() & Query::FLAG_IS_PROCCALL_QUERY;
                    if (isProcedureCall && !isProcedureCallQuery) {
                        goto NEXT_QUERY;
                    }
                    
                    applyStatementContext(handle, *query);
                    if (handle.executeQuery(query->statement()) != 0) {
                        _logger->error("query execution failed: {}", handle.lastError());
                    }
                    
                    // 프로시저에서 반환한 result를 소모하지 않으면 commands out of sync 오류가 난다
                    handle.consumeResults();
                    
                    NEXT_QUERY:
                    continue;
                }
            } catch (std::exception &e) {
                _logger->error("exception occurred while replaying transaction #{}: {}", gid, e.what());
                handle.executeQuery("ROLLBACK");
                continue;
            }
            
            handle.executeQuery("COMMIT");
        }
        
        
        {
            auto phase_main_end = std::chrono::steady_clock::now();
            std::chrono::duration<double> time = phase_main_end - phase_main_start;
            _phase2Time = time.count();
        }

        
        _logger->trace("== FULL REPLAY FINISHED ==");
        
        std::stringstream queryBuilder;
        queryBuilder << fmt::format("NEXT STEP:\n")
                     << fmt::format("    - RENAME DATABASE: {} to {}\n", _intermediateDBName, _plan.dbName())
                     << std::endl;
       
        _logger->info(queryBuilder.str());
        
        _logger->info("total {} queries replayed", (int) _replayedQueries);
        _logger->info("main phase {}s", _phase2Time);
        
        report.setExecutionTime(_phase2Time);
        
        if (!_plan.reportPath().empty()) {
            report.writeToJSON(_plan.reportPath());
        }
        
        if (_plan.dropIntermediateDB()) {
            dropIntermediateDB();
        }
    }

    
    void StateChanger::createIntermediateDB() {
        _logger->info("creating intermediate database: {}", _intermediateDBName);
        
        auto query = QUERY_TAG_STATECHANGE + fmt::format("CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", _intermediateDBName);
        auto dbHandleLease = _dbHandlePool.take();
        auto &dbHandle = dbHandleLease->get();
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot create intermediate database: {}", dbHandle.lastError());
            throw std::runtime_error(dbHandle.lastError());
        }
        dbHandle.executeQuery("COMMIT");
    }
    
    void StateChanger::dropIntermediateDB() {
         _logger->info("dropping intermediate database: {}", _intermediateDBName);
        
        auto query = QUERY_TAG_STATECHANGE + fmt::format("DROP DATABASE IF EXISTS {}", _intermediateDBName);
        auto dbHandleLease = _dbHandlePool.take();
        auto &dbHandle = dbHandleLease->get();
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot drop intermediate database: {}", dbHandle.lastError());
            throw std::runtime_error(dbHandle.lastError());
        }
        dbHandle.executeQuery("COMMIT");
    }
    
    void StateChanger::updatePrimaryKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp, std::string schemaName) {
        std::scoped_lock _lock(_context->contextLock);
    
        // TODO: LOCK
        std::unordered_set<std::string> primaryKeys;

        if (schemaName.empty()) {
            schemaName = _intermediateDBName;
        }
    
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{}' AND CONSTRAINT_NAME = 'PRIMARY'", schemaName);
    
    
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch foreign key information: {}", dbHandle.lastError());
            throw std::runtime_error(dbHandle.lastError());
        }

        auto result = dbHandle.storeResult();
        if (result == nullptr) {
            throw std::runtime_error("failed to read primary keys: empty result");
        }

        std::vector<std::string> row;
        while (result->next(row)) {
            if (row.size() < 2) {
                continue;
            }

            std::string table(std::move(utility::toLower(row[0])));
            std::string column(std::move(utility::toLower(row[1])));

            _logger->trace("updatePrimaryKeys(): adding primary key: {}.{}", table, column);
        
            primaryKeys.insert(table + "." + column);
        }
    
        _context->primaryKeys = primaryKeys;
    }
    
    void StateChanger::updateForeignKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp, std::string schemaName) {
        std::scoped_lock _lock(_context->contextLock);
    
        // TODO: LOCK
        std::vector<ForeignKey> foreignKeys;

        if (schemaName.empty()) {
            schemaName = _intermediateDBName;
        }
        
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{}' AND REFERENCED_TABLE_NAME IS NOT NULL", schemaName);
        
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch foreign key information: {}", dbHandle.lastError());
            throw std::runtime_error(dbHandle.lastError());
        }

        auto result = dbHandle.storeResult();
        if (result == nullptr) {
            throw std::runtime_error("failed to read foreign keys: empty result");
        }

        std::vector<std::string> row;
        while (result->next(row)) {
            if (row.size() < 4) {
                continue;
            }

            std::string fromTable(std::move(utility::toLower(row[0])));
            std::string fromColumn(std::move(utility::toLower(row[1])));
            
            std::string toTable(std::move(utility::toLower(row[2])));
            std::string toColumn(std::move(utility::toLower(row[3])));
            
            _logger->debug("updateForeignKeys(): adding foreign key: {}.{} -> {}.{}", fromTable, fromColumn, toTable, toColumn);
            
            ForeignKey foreignKey {
                _context->findTable(fromTable, timestamp), fromColumn,
                _context->findTable(toTable, timestamp), toColumn
            };
            
            foreignKeys.push_back(foreignKey);
        }
        
        _context->foreignKeys = foreignKeys;
    }

    void StateChanger::applyStatementContext(mariadb::DBHandle &dbHandle, const Query &query) {
        const auto &context = query.statementContext();
        if (query.timestamp() > 0) {
            dbHandle.executeQuery(fmt::format("SET TIMESTAMP={}", query.timestamp()));
        }

        if (context.hasLastInsertId) {
            dbHandle.executeQuery(fmt::format("SET LAST_INSERT_ID={}", context.lastInsertId));
        }
        if (context.hasInsertId) {
            dbHandle.executeQuery(fmt::format("SET INSERT_ID={}", context.insertId));
        }
        if (context.hasRandSeed) {
            dbHandle.executeQuery(fmt::format("SET @@RAND_SEED1={}, @@RAND_SEED2={}",
                                              context.randSeed1, context.randSeed2));
        }

        for (const auto &userVar : context.userVars) {
            std::string name = quoteUserVarName(userVar.name);
            std::string value = formatUserVarValue(userVar);
            dbHandle.executeQuery(fmt::format("SET @{} := {}", name, value));
        }
    }
    
    int64_t StateChanger::getAutoIncrement(mariadb::DBHandle &dbHandle, std::string table) {
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
                        _intermediateDBName, table);
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch auto increment: {}", dbHandle.lastError());
            throw std::runtime_error(dbHandle.lastError());
        }

        auto result = dbHandle.storeResult();
        if (result == nullptr || result->rowCount() == 0) {
            return -1;
        }

        std::vector<std::string> row;
        if (!result->next(row) || row.empty() || row[0].empty()) {
            return -1;
        }

        // TODO: support for 64-bit integer
        return std::atoi(row[0].c_str());
    }
    
    void StateChanger::setAutoIncrement(mariadb::DBHandle &dbHandle, std::string table, int64_t value) {
        if (value == -1) {
            return;
        }
        
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("ALTER TABLE {} AUTO_INCREMENT = {}", table, value);
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot set auto increment: {}", dbHandle.lastError());
            throw std::runtime_error(dbHandle.lastError());
        }
    }
}
