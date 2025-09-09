//
// Created by cheesekun on 8/29/22.
//

#include <algorithm>
#include <sstream>

#include <fmt/color.h>

#include <bison_parser.h>
#include <SQLParser.h>
#include <SQLParserResult.h>

#include "StateLogWriter.hpp"
#include "GIDIndexWriter.hpp"
#include "GIDIndexReader.hpp"
#include "cluster/RowCluster.hpp"

#include "cluster/StateCluster.hpp"

#include "base/TaskExecutor.hpp"
#include "utils/StringUtil.hpp"

#include "StateChangeReport.hpp"

#include "StateChanger.hpp"

namespace ultraverse::state::v2 {
    
    const std::string StateChanger::QUERY_TAG_STATECHANGE = "/* STATECHANGE_QUERY */ ";
    
    StateChanger::StateChanger(DBHandlePool<mariadb::DBHandle> &dbHandlePool, const StateChangePlan &plan):
        _logger(createLogger("StateChanger")),
        _dbHandlePool(dbHandlePool),
        _mode(OperationMode::NORMAL),
        _plan(plan),
        _intermediateDBName(fmt::format("ult_intermediate_{}_{}", (int) time(nullptr), getpid())), // FIXME
        _reader(plan.stateLogPath(), plan.stateLogName()),
        _columnGraph(std::make_unique<ColumnDependencyGraph>()),
        _tableGraph(std::make_unique<TableDependencyGraph>()),
        _context(new StateChangeContext),
        _replayedQueries(0)
    {
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
            updatePrimaryKeys(dbHandle.get(), 0);
            updateForeignKeys(dbHandle.get(), 0);
            auto load_backup_end = std::chrono::steady_clock::now();
            
            std::chrono::duration<double> time = load_backup_end - load_backup_start;
            _logger->info("LOAD BACKUP END: {}s elapsed", time.count());
            report.setSQLLoadTime(time.count());
        }
        
        _logger->info("opening state log");
        _reader.open();
        
        _isRunning = true;
        
        auto phase_main_start = std::chrono::steady_clock::now();
        
        while (_reader.nextHeader()) {
            auto transactionHeader = _reader.txnHeader();
            auto pos = _reader.pos() - sizeof(TransactionHeader);
            
            _reader.nextTransaction();
            auto transaction = _reader.txnBody();
            auto gid = transactionHeader->gid;
            auto flags = transactionHeader->flags;
            
            if (_plan.isRollbackGid(gid)) {
                _logger->info("skipping rollback transaction #{}", gid);
                continue;
            }
            
            auto dbHandle = _dbHandlePool.take();
 
            // _logger->info("replaying transaction #{}", gid);
            
            dbHandle.get().executeQuery("USE " + _intermediateDBName);
            dbHandle.get().executeQuery("START TRANSACTION");
            
            bool isProcedureCall = transaction->flags() & Transaction::FLAG_IS_PROCEDURE_CALL;
            
            try {
                for (const auto &query: transaction->queries()) {
                    bool isProcedureCallQuery = query->flags() & Query::FLAG_IS_PROCCALL_QUERY;
                    if (isProcedureCall && !isProcedureCallQuery) {
                        goto NEXT_QUERY;
                    }
                    
                    if (dbHandle.get().executeQuery(query->statement()) != 0) {
                        _logger->error("query execution failed: {}", mysql_error(dbHandle.get()));
                    }
                    
                    // 프로시저에서 반환한 result를 소모하지 않으면 commands out of sync 오류가 난다
                    do {
                        auto result = mysql_store_result(dbHandle.get());
                        if (result != nullptr) {
                            mysql_free_result(result);
                        }
                    } while (mysql_next_result(dbHandle.get()) == 0);
                    
                    NEXT_QUERY:
                    continue;
                }
            } catch (std::exception &e) {
                _logger->error("exception occurred while replaying transaction #{}: {}", gid, e.what());
                dbHandle.get().executeQuery("ROLLBACK");
                continue;
            }
            
            dbHandle.get().executeQuery("COMMIT");
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
        auto &dbHandle = dbHandleLease.get();
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot create intermediate database: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
        dbHandle.executeQuery("COMMIT");
    }
    
    void StateChanger::dropIntermediateDB() {
         _logger->info("dropping intermediate database: {}", _intermediateDBName);
        
        auto query = QUERY_TAG_STATECHANGE + fmt::format("DROP DATABASE IF EXISTS {}", _intermediateDBName);
        auto dbHandleLease = _dbHandlePool.take();
        auto &dbHandle = dbHandleLease.get();
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot drop intermediate database: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
        dbHandle.executeQuery("COMMIT");
    }
    
    void StateChanger::updatePrimaryKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp) {
        std::scoped_lock _lock(_context->contextLock);
    
        // TODO: LOCK
        std::unordered_set<std::string> primaryKeys;
    
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{}' AND CONSTRAINT_NAME = 'PRIMARY'", _intermediateDBName);
    
    
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch foreign key information: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
    
        MYSQL_RES *result = mysql_store_result(dbHandle);
        MYSQL_ROW row;
    
        while ((row = mysql_fetch_row(result)) != nullptr) {
            std::string table(std::move(utility::toLower(row[0])));
            std::string column(std::move(utility::toLower(row[1])));

            _logger->trace("updatePrimaryKeys(): adding primary key: {}.{}", table, column);
        
            primaryKeys.insert(table + "." + column);
        }
        mysql_free_result(result);
    
        _context->primaryKeys = primaryKeys;
    }
    
    void StateChanger::updateForeignKeys(mariadb::DBHandle &dbHandle, uint64_t timestamp) {
        std::scoped_lock _lock(_context->contextLock);
    
        // TODO: LOCK
        std::vector<ForeignKey> foreignKeys;
        
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{}' AND REFERENCED_TABLE_NAME IS NOT NULL", _intermediateDBName);
        
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch foreign key information: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
        
        MYSQL_RES *result = mysql_store_result(dbHandle);
        MYSQL_ROW row;
        
        while ((row = mysql_fetch_row(result)) != nullptr) {
            std::string fromTable(std::move(utility::toLower(row[0])));
            std::string fromColumn(std::move(utility::toLower(row[1])));
            
            std::string toTable(std::move(utility::toLower(row[2])));
            std::string toColumn(std::move(utility::toLower(row[3])));
            
            // _logger->trace("updateForeignKeys(): adding foreign key: {}.{} -> {}.{}", fromTable, fromColumn, toTable, toColumn);
            
            ForeignKey foreignKey {
                _context->findTable(fromTable, timestamp), fromColumn,
                _context->findTable(toTable, timestamp), toColumn
            };
            
            foreignKeys.push_back(foreignKey);
        }
        mysql_free_result(result);
        
        _context->foreignKeys = foreignKeys;
    }
    
    int64_t StateChanger::getAutoIncrement(mariadb::DBHandle &dbHandle, std::string table) {
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
                        _intermediateDBName, table);
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot fetch auto increment: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
        
        MYSQL_RES *result = mysql_store_result(dbHandle);
        bool isAvailable = mysql_num_rows(result) != 0;
        
        if (!isAvailable) {
            return -1;
        }
        
        MYSQL_ROW row = mysql_fetch_row(result);
        
        if (row[0] == nullptr) {
            return -1;
        }
        
        // TODO: support for 64-bit integer
        return std::atoi(row[0]);
    }
    
    void StateChanger::setAutoIncrement(mariadb::DBHandle &dbHandle, std::string table, int64_t value) {
        if (value == -1) {
            return;
        }
        
        const auto query =
            QUERY_TAG_STATECHANGE +
            fmt::format("ALTER TABLE {} AUTO_INCREMENT = {}", table, value);
        
        if (dbHandle.executeQuery(query) != 0) {
            _logger->error("cannot set auto increment: {}", mysql_error(dbHandle));
            throw std::runtime_error(mysql_error(dbHandle));
        }
    }
}
