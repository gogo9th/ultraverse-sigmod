//
// Created by cheesekun on 8/10/22.
//

#include <cstdlib>
#include <sstream>

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
    
    TableMapEvent::TableMapEvent(uint64_t tableId, std::string database, std::string table, std::vector<std::pair<column_type::Value, int>> columns, std::vector<std::string> columnNames, uint64_t timestamp):
        _timestamp(timestamp),
        _tableId(tableId),
        _database(database),
        _table(table),
        _columns(columns),
        _columnNames(columnNames)
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
    
    column_type::Value TableMapEvent::typeOf(int columnIndex) const {
        return _columns[columnIndex].first;
    }
    
    int TableMapEvent::sizeOf(int columnIndex) const {
        return _columns[columnIndex].second;
    }
    
    std::string TableMapEvent::nameOf(int columnIndex) const {
        return _columnNames[columnIndex];
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
                auto retval = readRow(tableMapEvent, pos);
                auto &rowData = retval.first;
                auto rowSize = retval.second;
                _rowSet.push_back(rowData);
                pos += rowSize;
            }
            
            if (_type == UPDATE) {
                auto retval = readRow(tableMapEvent, pos);
                auto &rowData = retval.first;
                auto rowSize = retval.second;
                _changeSet.push_back(rowData);
                pos += rowSize;
            }
        }
        
        _affectedRows = _rowSet.size();
    }
    
    std::pair<std::string, int> RowEvent::readRow(TableMapEvent &tableMapEvent, int basePos) {
        uint64_t nullFields = 0;
        int nullFieldsSize = (_columns + 7) / 8;
        
        memcpy(&nullFields, _rowData.get() + basePos, (_columns + 7) / 8);
        
        std::stringstream sstream;
        
        int rowSize = 0;
    
        for (int i = 0; i < _columns; i++) {
            auto columnType = tableMapEvent.typeOf(i);
            int columnSize = tableMapEvent.sizeOf(i);
            auto columnName = tableMapEvent.nameOf(i);
            
            auto offset = basePos + nullFieldsSize + rowSize;
            
            
            if ((nullFields & (1 << i)) != 0) {
                // NULL
                sstream << columnName << "=";
            } else if (columnSize == -1) {
                // length + [string content]
                int strLength = (_rowData.get()[offset]);
                
                std::unique_ptr<uint8_t> rawValue(new uint8_t[strLength]);
                memcpy(rawValue.get(), _rowData.get() + offset + 1, strLength);
                
                std::string strValue((char *) rawValue.get(), strLength);
                sstream << columnName << "=" << strValue;
                
                rowSize += strLength + 1;
            } else {
                if (columnType == column_type::INTEGER) {
                    switch (columnSize) {
                        case 8:
                            sstream << columnName << "=" << readValue<int64_t>(offset);
                            break;
                        case 4:
                            sstream << columnName << "=" << readValue<int32_t>(offset);
                            break;
                        case 2:
                            sstream << columnName << "=" << readValue<int16_t>(offset);
                            break;
                        case 1:
                            sstream << columnName << "=" << readValue<int8_t>(offset);
                            break;
                    }
                } else if (columnType == column_type::FLOAT) {
                    switch (columnSize) {
                        case 8:
                            sstream << columnName << "=" << readValue<double>(offset);
                            break;
                        case 4:
                            sstream << columnName << "=" << readValue<float>(offset);
                            break;
                    }
                }
                
                rowSize += columnSize;
            }
    
            if (i + 1 < _columns) {
                sstream << ":";
            }
        }
        
        return std::make_pair(sstream.str(), nullFieldsSize + rowSize);
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