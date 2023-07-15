//
// Created by cheesekun on 8/10/22.
//

#include <cstdlib>
#include <sstream>

#include <cereal/types/vector.hpp>
#include <cereal/types/unordered_map.hpp>
#include <iomanip>

#include "DBEvent.hpp"

#include "mariadb/state/StateItem.h"


namespace ultraverse::mariadb {
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
                       uint64_t timestamp, uint16_t flags):
        _timestamp(timestamp),
       
        _type(type),
        _tableId(tableId),
        _columns(columns),
        _rowData(std::move(rowData)),
        _dataSize(dataSize),
        _flags(flags)
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
    
    uint16_t RowEvent::flags() const {
        return _flags;
    }
    
    void RowEvent::mapToTable(TableMapEvent &tableMapEvent) {
        int pos = 0;
        
        while (pos < _dataSize) {
            {
                // type이 UPDATE면 변경 전 row를 updateSet에 넣는다.
                // (항상 변경된 row를 itemSet에 넣도록 한다)
                auto retval = readRow(tableMapEvent, pos, (_type == UPDATE) ? true : false);
                auto &rowData = retval.first;
                auto rowSize = retval.second;
                _rowSet.push_back(rowData);
                pos += rowSize;
            }
            
            if (_type == UPDATE) {
                auto retval = readRow(tableMapEvent, pos, false);
                auto &rowData = retval.first;
                auto rowSize = retval.second;
                _changeSet.push_back(rowData);
                pos += rowSize;
            }
        }
        
        _affectedRows = _rowSet.size();
    }
    
    std::pair<std::string, int> RowEvent::readRow(TableMapEvent &tableMapEvent, int basePos, bool isUpdate) {
        uint64_t nullFields = 0;
        int nullFieldsSize = (_columns + 7) / 8;

        memcpy(&nullFields, _rowData.get() + basePos, (_columns + 7) / 8);

        // FIXME: 이거 제거
        std::stringstream sstream;
        
        int rowSize = 0;
    
        for (int i = 0; i < _columns; i++) {
            auto columnType = tableMapEvent.typeOf(i);
            int columnSize = tableMapEvent.sizeOf(i);
            auto columnName = tableMapEvent.nameOf(i);
            
            auto offset = basePos + nullFieldsSize + rowSize;
            
            
            if ((nullFields & (((int64_t) 1) << i)) != 0) {
                // NULL
                sstream << columnName << "=";
                
                if (columnType == column_type::INTEGER) {
                    if (columnSize == 2 || columnSize == 1) {
                        // HACK
                        rowSize += columnSize;
                    }
                }
            } else if (columnType == column_type::STRING) {
                // length + [string content]
                uint64_t strLength = 0;
                size_t strLengthSize = 1;
                
                if (columnSize == -1) {
                    strLength = (_rowData.get()[offset]);
                } else if (columnSize == -2) {
                    strLength = (uint16_t) *reinterpret_cast<uint16_t *>(_rowData.get() + offset);
                    strLengthSize = 2;
                } else if (columnSize == -4) {
                    strLength = (uint32_t) *reinterpret_cast<uint32_t *>(_rowData.get() + offset);
                    strLengthSize = 4;
                } else if (columnSize == -8) {
                    strLength = (uint64_t) *reinterpret_cast<uint64_t *>(_rowData.get() + offset);
                    strLengthSize = 8;
                } else {
                    strLength = columnSize;
                    strLengthSize = 0;
                }

                std::unique_ptr<uint8_t> rawValue(new uint8_t[strLength]);
                memcpy(rawValue.get(), _rowData.get() + offset + strLengthSize, strLength);

                std::string strValue((char *) rawValue.get(), strLength);
                sstream << columnName << "=" << strValue;
    
                {
                    StateItem candidateItem;
                    StateData data;
                    
                    data.Set(strValue.c_str(), strLength);
                    candidateItem.data_list.emplace_back(std::move(data));
                    candidateItem.function_type = FUNCTION_EQ;
                    candidateItem.name = tableMapEvent.table() + "." + columnName;
                    
                    if (isUpdate) {
                        _updateSet.emplace_back(std::move(candidateItem));
                    } else {
                        _itemSet.emplace_back(std::move(candidateItem));
                    }
                }
                
                rowSize += strLength + strLengthSize;
            } else if (columnType == column_type::DATETIME) {
                std::unique_ptr<uint8_t> rawValue(new uint8_t[columnSize]);
                memcpy(rawValue.get(), _rowData.get() + offset, columnSize);
    
                std::string strValue((char *) rawValue.get(), columnSize);
                sstream << columnName << "=" << strValue;
    
                {
                    StateItem candidateItem;
                    StateData data;
        
                    data.Set(strValue.c_str(), columnSize);
                    candidateItem.data_list.emplace_back(std::move(data));
                    candidateItem.function_type = FUNCTION_EQ;
                    candidateItem.name = tableMapEvent.table() + "." + columnName;
                    
                    if (isUpdate) {
                        _updateSet.emplace_back(std::move(candidateItem));
                    } else {
                        _itemSet.emplace_back(std::move(candidateItem));
                    }
                }
    
                rowSize += columnSize;
            } else if (columnType == column_type::DECIMAL) {
                StateItem candidateItem;
                StateData data;
                
                uint8_t precision = columnSize & 0xff;
                uint8_t scale     = columnSize >> 8;
                
                uint8_t size = (precision + 1) / 2;
                
                std::unique_ptr<uint8_t> rawValue(new uint8_t[size]);
                memcpy(rawValue.get(), _rowData.get() + offset, size);
                
                
                bool sign = false;
                uint64_t high = 0;
                uint64_t low = 0;
                
                int i = 0;
                while (i < size) {
                    uint8_t value = rawValue.get()[i];
                    
                    if (i == 0) {
                        sign = value & 0x80;
                        value = value ^ 0x80;
                    }
                    
                    if (i < ((precision - scale) + 1) / 2) {
                        high = (high << 8) + value;
                    } else {
                        low = (low << 8) + value;
                    }
                    
                    i++;
                }
                
                
                // FIXME: maybe inaccurate
                std::stringstream replStream;
                
                if (!sign) {
                    replStream << '-';
                }
                
                replStream << high << "."
                           << std::setfill('0') << std::setw(scale) << low;
                
                std::string replVal = replStream.str();
                
                data.Set(replVal.c_str(), replVal.size());
    
                candidateItem.data_list.emplace_back(std::move(data));
                candidateItem.function_type = FUNCTION_EQ;
                candidateItem.name = tableMapEvent.table() + "." + columnName;
    
                if (isUpdate) {
                    _updateSet.emplace_back(std::move(candidateItem));
                } else {
                    _itemSet.emplace_back(std::move(candidateItem));
                }
    
                sstream << columnName << "=" << replVal;
                
                rowSize += size;
            } else {
                StateItem candidateItem;
                StateData data;
                
                if (columnType == column_type::INTEGER) {
                    int64_t value;
                    switch (columnSize) {
                        case 8:
                            value = readValue<int64_t>(offset);
                            sstream << columnName << "=" << "I64!" << value;
                            break;
                        case 4:
                            value = readValue<int32_t>(offset);
                            sstream << columnName << "=" << "I32!" << value;
                            break;
                        case 3:
                            throw std::runtime_error("unsupported format");
                            break;
                        case 2:
                            value = readValue<int16_t>(offset);
                            sstream << columnName << "=" << "I16!" << value;
                            break;
                        case 1:
                            value = readValue<int8_t>(offset);
                            sstream << columnName << "=" << "I8!" << value;
                            break;
                    }
    
                    data.Set(value);
                } else if (columnType == column_type::FLOAT) {
                    double value;
                    switch (columnSize) {
                        case 8:
                            value = readValue<double>(offset);
                            sstream << columnName << "=" << "F64!" << value;
                            break;
                        case 4:
                            value = readValue<float>(offset);
                            sstream << columnName << "=" << "F32!" << value;
                            break;
                    }
    
                    data.Set(value);
                }
                
                candidateItem.data_list.emplace_back(std::move(data));
                candidateItem.function_type = FUNCTION_EQ;
                candidateItem.name = tableMapEvent.table() + "." + columnName;
                
                if (isUpdate) {
                    _updateSet.emplace_back(std::move(candidateItem));
                } else {
                    _itemSet.emplace_back(std::move(candidateItem));
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
    
    const std::vector<StateItem> &RowEvent::itemSet() const {
        return _itemSet;
    }
    
    const std::vector<StateItem> &RowEvent::updateSet() const {
        return _updateSet;
    }
}