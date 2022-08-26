//
// Created by cheesekun on 8/10/22.
//

#include <cstdlib>

#include <cereal/types/vector.hpp>
#include <cereal/types/unordered_map.hpp>

#include "DBEvent.hpp"


namespace ultraverse::mariadb {
    TransactionIDEvent::TransactionIDEvent(const MARIADB_RPL_EVENT *rplEvent):
        _timestamp(rplEvent->timestamp),
        _transactionId(rplEvent->event.xid.transaction_nr)
    {
    }
    
    TransactionIDEvent::TransactionIDEvent(uint64_t xid, uint64_t timestamp):
        _transactionId(xid),
        _timestamp(timestamp)
    {
    
    }
    
    uint64_t TransactionIDEvent::timestamp() {
        return _timestamp;
    }

    uint64_t TransactionIDEvent::transactionId() {
        return _transactionId;
    }
    
   
    QueryEvent::QueryEvent(const MARIADB_RPL_EVENT *rplEvent):
        _timestamp(rplEvent->timestamp),
        
        _error((int64_t) rplEvent->event.query.errornr),
        
        _statement(std::string(rplEvent->event.query.statement.str, rplEvent->event.query.statement.length - 2)),
        _database(std::string(rplEvent->event.query.database.str, rplEvent->event.query.database.length))
    {
    
    }
    
    QueryEvent::QueryEvent(
        const std::string &schema,
        const std::string &statement,
        uint64_t timestamp
    ):
        _database(schema),
        _statement(statement),
        
        _timestamp(timestamp),
        _error(0)
    {
    
    }
    
    
    uint64_t QueryEvent::timestamp() {
        return _timestamp;
    }
    
    const int64_t QueryEvent::error() {
        return _error;
    }
    
    const std::string &QueryEvent::statement() {
        return _statement;
    }
    
    const std::string &QueryEvent::database() {
        return _database;
    }
    
    TableMapEvent::TableMapEvent(uint64_t tableId, std::string database, std::string table, std::vector<int> columns, uint64_t timestamp):
        _timestamp(timestamp),
        _tableId(tableId),
        _database(database),
        _table(table),
        _columns(columns)
    {
    
    }
    
    uint64_t TableMapEvent::timestamp() {
        return _timestamp;
    }
    
    uint64_t TableMapEvent::tableId() const {
        return _tableId;
    }
    
    std::string TableMapEvent::database() const {
        return _database;
    }
    
    std::string TableMapEvent::table() const {
        return _table;
    }
    
    int TableMapEvent::sizeOf(int columnIndex) const {
        return _columns[columnIndex];
    }
    
    RowQueryEvent::RowQueryEvent(const std::string &statement, uint64_t timestamp):
        _statement(statement),
        _timestamp(timestamp)
    {
    
    }
    
    uint64_t RowQueryEvent::timestamp() {
        return _timestamp;
    }
    
    std::string RowQueryEvent::statement() {
        return _statement;
    }
    
    RowEvent::RowEvent(Type type, uint64_t tableId, int columns,
                       std::shared_ptr<uint8_t> rowData, int dataSize,
                       uint64_t timestamp):
        _timestamp(timestamp),
       
        _type(type),
        _tableId(tableId),
        _columns(columns),
        _rowData(std::move(rowData)),
        _dataSize(dataSize)
    {
        
    }
    
    uint64_t RowEvent::timestamp() {
        return _timestamp;
    }
    
    RowEvent::Type RowEvent::type() const {
        return _type;
    }
    
    uint64_t RowEvent::tableId() const {
        return _tableId;
    }
    
    void RowEvent::mapToTable(TableMapEvent &tableMapEvent) {
        int pos = 0;
        
        while (pos < _dataSize) {
            {
                auto rowSize = calculateRowSize(tableMapEvent, pos);
                auto block = std::string((char *) _rowData.get() + pos, rowSize);
                _rowSet.push_back(block);
                pos += rowSize;
            }
            
            if (_type == UPDATE) {
                auto rowSize = calculateRowSize(tableMapEvent, pos);
                auto block = std::string((char *) _rowData.get() + pos, rowSize);
                _changeSet.push_back(block);
                pos += rowSize;
            }
        }
        
        _affectedRows = _rowSet.size();
    }
    
    int RowEvent::calculateRowSize(TableMapEvent &tableMapEvent, int basePos) {
        uint64_t nullFields = 0;
        int nullFieldsSize = (_columns + 7) / 8;
        
        memcpy(&nullFields, _rowData.get() + basePos, (_columns + 7) / 8);
        
        int rowSize = 0;
    
        for (int i = 0; i < _columns; i++) {
            int columnSize = tableMapEvent.sizeOf(i);
            
            if ((nullFields & (1 << i)) != 0) {
                continue;
            }
        
            if (columnSize == -1) {
                int strLength = (_rowData.get()[basePos + nullFieldsSize + rowSize]) + 1;
                rowSize += strLength;
            } else {
                rowSize += columnSize;
            }
        }
        
        return nullFieldsSize + rowSize;
    }
    
    int RowEvent::affectedRows() const {
        return _affectedRows;
    }
    
    std::string RowEvent::rowSet(int at) {
        return _rowSet[at];
    }
    
    std::string RowEvent::changeSet(int at) {
        return _changeSet[at];
    }
}