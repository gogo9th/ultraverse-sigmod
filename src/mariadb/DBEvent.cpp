//
// Created by cheesekun on 8/10/22.
//

#include <cstdlib>
#include <sstream>
#include <utility>

#include <my_byteorder.h>
#include <mysql/binlog/event/export/binary_log_funcs.h>
#include "DBEvent.hpp"

#include "mariadb/state/StateItem.h"
#include "utils/log.hpp"


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

    QueryEvent::QueryEvent(
        const std::string &schema,
        const ultparser::Query &pbStatement,
        uint64_t timestamp
    ):
        _database(schema),
        _statement(pbStatement.dml().statement()),

        _timestamp(timestamp),
        _error(0)
    {
        assert(pbStatement.has_dml());

        // TODO: warn unless processDML()
        processDML(pbStatement.dml());
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

    IntVarEvent::IntVarEvent(Type type, uint64_t value, uint64_t timestamp):
        _type(type),
        _value(value),
        _timestamp(timestamp)
    {
    }

    uint64_t IntVarEvent::timestamp() {
        return _timestamp;
    }

    IntVarEvent::Type IntVarEvent::type() const {
        return _type;
    }

    uint64_t IntVarEvent::value() const {
        return _value;
    }

    RandEvent::RandEvent(uint64_t seed1, uint64_t seed2, uint64_t timestamp):
        _seed1(seed1),
        _seed2(seed2),
        _timestamp(timestamp)
    {
    }

    uint64_t RandEvent::timestamp() {
        return _timestamp;
    }

    uint64_t RandEvent::seed1() const {
        return _seed1;
    }

    uint64_t RandEvent::seed2() const {
        return _seed2;
    }

    UserVarEvent::UserVarEvent(std::string name,
                               ValueType type,
                               bool isNull,
                               bool isUnsigned,
                               uint32_t charset,
                               std::string value,
                               uint64_t timestamp):
        _name(std::move(name)),
        _type(type),
        _isNull(isNull),
        _isUnsigned(isUnsigned),
        _charset(charset),
        _value(std::move(value)),
        _timestamp(timestamp)
    {
    }

    uint64_t UserVarEvent::timestamp() {
        return _timestamp;
    }

    const std::string &UserVarEvent::name() const {
        return _name;
    }

    UserVarEvent::ValueType UserVarEvent::type() const {
        return _type;
    }

    bool UserVarEvent::isNull() const {
        return _isNull;
    }

    bool UserVarEvent::isUnsigned() const {
        return _isUnsigned;
    }

    uint32_t UserVarEvent::charset() const {
        return _charset;
    }

    const std::string &UserVarEvent::value() const {
        return _value;
    }
    
    TableMapEvent::TableMapEvent(uint64_t tableId,
                                 std::string database,
                                 std::string table,
                                 std::vector<std::pair<column_type::Value, int>> columns,
                                 std::vector<std::string> columnNames,
                                 std::vector<uint8_t> unsignedFlags,
                                 std::vector<enum_field_types> mysqlTypes,
                                 std::vector<uint16_t> mysqlMetadata,
                                 uint64_t timestamp):
        _timestamp(timestamp),
        _tableId(tableId),
        _database(std::move(database)),
        _table(std::move(table)),
        _columns(std::move(columns)),
        _columnNames(std::move(columnNames)),
        _unsignedFlags(std::move(unsignedFlags)),
        _mysqlTypes(std::move(mysqlTypes)),
        _mysqlMetadata(std::move(mysqlMetadata))
    {
        if (_unsignedFlags.size() != _columns.size()) {
            _unsignedFlags.resize(_columns.size(), 0);
        }
        if (_mysqlTypes.size() != _columns.size()) {
            _mysqlTypes.resize(_columns.size(), MYSQL_TYPE_NULL);
        }
        if (_mysqlMetadata.size() != _columns.size()) {
            _mysqlMetadata.resize(_columns.size(), 0);
        }
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

    int TableMapEvent::columnCount() const {
        return static_cast<int>(_columns.size());
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

    bool TableMapEvent::isUnsigned(int columnIndex) const {
        if (columnIndex < 0 || static_cast<size_t>(columnIndex) >= _unsignedFlags.size()) {
            return false;
        }
        return _unsignedFlags[columnIndex] != 0;
    }

    enum_field_types TableMapEvent::mysqlTypeOf(int columnIndex) const {
        if (columnIndex < 0 || static_cast<size_t>(columnIndex) >= _mysqlTypes.size()) {
            return MYSQL_TYPE_NULL;
        }
        return _mysqlTypes[columnIndex];
    }

    uint16_t TableMapEvent::mysqlMetadataOf(int columnIndex) const {
        if (columnIndex < 0 || static_cast<size_t>(columnIndex) >= _mysqlMetadata.size()) {
            return 0;
        }
        return _mysqlMetadata[columnIndex];
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
        _flags(flags),
        _columnsBeforeImage(),
        _columnsAfterImage(),
        _columnsBeforeCount(columns),
        _columnsAfterCount(columns)
    {
        _columnsBeforeImage.assign((columns + 7) / 8, 0xff);
        if (columns % 8 != 0 && !_columnsBeforeImage.empty()) {
            _columnsBeforeImage.back() = static_cast<uint8_t>((1u << (columns % 8)) - 1);
        }
        _columnsAfterImage = _columnsBeforeImage;
    }

    RowEvent::RowEvent(Type type, uint64_t tableId, int columns,
                       std::vector<uint8_t> columnsBeforeImage,
                       std::vector<uint8_t> columnsAfterImage,
                       std::shared_ptr<uint8_t> rowData, int dataSize,
                       uint64_t timestamp, uint16_t flags):
        _timestamp(timestamp),
        _type(type),
        _tableId(tableId),
        _columns(columns),
        _rowData(std::move(rowData)),
        _dataSize(dataSize),
        _flags(flags),
        _columnsBeforeImage(std::move(columnsBeforeImage)),
        _columnsAfterImage(std::move(columnsAfterImage)),
        _columnsBeforeCount(0),
        _columnsAfterCount(0)
    {
        auto countBits = [](const std::vector<uint8_t> &bitmap, int maxBits) {
            int count = 0;
            for (int i = 0; i < maxBits; i++) {
                if (bitmap[i / 8] & (1u << (i % 8))) {
                    count++;
                }
            }
            return count;
        };
        if (_columns > 0 && !_columnsBeforeImage.empty()) {
            _columnsBeforeCount = countBits(_columnsBeforeImage, _columns);
        }
        if (_columns > 0 && !_columnsAfterImage.empty()) {
            _columnsAfterCount = countBits(_columnsAfterImage, _columns);
        }
        if (_columnsBeforeCount == 0 && _columns > 0) {
            _columnsBeforeCount = _columns;
        }
        if (_columnsAfterCount == 0 && _columns > 0) {
            _columnsAfterCount = _columns;
        }
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
        if (_columns != tableMapEvent.columnCount()) {
            _affectedRows = 0;
            return;
        }

        int pos = 0;
        
        while (pos < _dataSize) {
            {
                // type이 UPDATE면 변경 전 row를 updateSet에 넣는다.
                // (항상 변경된 row를 itemSet에 넣도록 한다)
                auto retval = readRow(
                    tableMapEvent,
                    pos,
                    _columnsBeforeImage.empty() ? _columnsAfterImage : _columnsBeforeImage,
                    _columnsBeforeCount,
                    (_type == UPDATE)
                );
                auto &rowData = retval.first;
                auto rowSize = retval.second;
                _rowSet.push_back(rowData);
                pos += rowSize;
            }
            
            if (_type == UPDATE) {
                auto retval = readRow(
                    tableMapEvent,
                    pos,
                    _columnsAfterImage.empty() ? _columnsBeforeImage : _columnsAfterImage,
                    _columnsAfterCount,
                    false
                );
                auto &rowData = retval.first;
                auto rowSize = retval.second;
                _changeSet.push_back(rowData);
                pos += rowSize;
            }
        }
        
        _affectedRows = _rowSet.size();
    }
    
    std::pair<std::string, int> RowEvent::readRow(TableMapEvent &tableMapEvent, int basePos,
                                                  const std::vector<uint8_t> &columnsBitmap,
                                                  int columnsBitmapCount, bool isUpdate) {
        int usedColumns = columnsBitmapCount > 0 ? columnsBitmapCount : _columns;
        int nullFieldsSize = (usedColumns + 7) / 8;

        std::vector<uint8_t> nullFields(static_cast<size_t>(nullFieldsSize), 0);
        if (nullFieldsSize > 0) {
            memcpy(nullFields.data(), _rowData.get() + basePos, nullFieldsSize);
        }

        std::stringstream sstream;

        int rowSize = 0;
        int usedIndex = 0;
        bool first = true;

        auto isColumnUsed = [&](int columnIndex) -> bool {
            if (columnsBitmap.empty()) {
                return true;
            }
            return (columnsBitmap[columnIndex / 8] & (1u << (columnIndex % 8))) != 0;
        };

        auto isNull = [&](int usedColumnIndex) -> bool {
            if (nullFields.empty()) {
                return false;
            }
            return (nullFields[usedColumnIndex / 8] & (1u << (usedColumnIndex % 8))) != 0;
        };

        auto readLengthLE = [](const uchar *raw, size_t lenBytes) -> uint32_t {
            switch (lenBytes) {
                case 1:
                    return raw[0];
                case 2:
                    return static_cast<uint32_t>(uint2korr(raw));
                case 3:
                    return static_cast<uint32_t>(uint3korr(raw));
                case 4:
                    return static_cast<uint32_t>(uint4korr(raw));
                default:
                    return 0;
            }
        };

        const size_t dataSize = static_cast<size_t>(_dataSize);
        const size_t basePosSize = static_cast<size_t>(basePos);

        for (int i = 0; i < _columns; i++) {
            if (!isColumnUsed(i)) {
                continue;
            }

            auto columnName = tableMapEvent.nameOf(i);
            auto fullName = tableMapEvent.table() + "." + columnName;
            auto mysqlType = tableMapEvent.mysqlTypeOf(i);
            auto mysqlMeta = tableMapEvent.mysqlMetadataOf(i);
            auto columnSize = tableMapEvent.sizeOf(i);

            auto offset = static_cast<size_t>(basePos + nullFieldsSize + rowSize);
            if (offset >= dataSize) {
                warning("RowEvent: row offset out of range (offset=%zu, size=%zu)", offset, dataSize);
                return std::make_pair(sstream.str(), static_cast<int>(dataSize - basePosSize));
            }
            size_t remaining = dataSize - offset;
            const uchar *raw = reinterpret_cast<const uchar *>(_rowData.get() + offset);

            if (!first) {
                sstream << ":";
            }
            first = false;

            if (isNull(usedIndex)) {
                sstream << columnName << "=";
                usedIndex++;
                continue;
            }
            usedIndex++;

            uint32_t fieldLen = calc_field_size(static_cast<unsigned char>(mysqlType), raw, mysqlMeta);
            bool fieldLenUnknown = (fieldLen == UINT_MAX);
            if (fieldLenUnknown) {
                fieldLen = 0;
            }

            auto pushItem = [&](StateData &&data) {
                StateItem candidateItem;
                candidateItem.data_list.emplace_back(std::move(data));
                candidateItem.function_type = FUNCTION_EQ;
                candidateItem.name = fullName;
                if (isUpdate) {
                    _updateSet.emplace_back(std::move(candidateItem));
                } else {
                    _itemSet.emplace_back(std::move(candidateItem));
                }
            };

            bool handled = false;
            switch (mysqlType) {
                case MYSQL_TYPE_NEWDECIMAL: {
                    StateData data;
                    data.SetDecimal(reinterpret_cast<const char *>(raw), static_cast<size_t>(fieldLen));
                    pushItem(std::move(data));

                    std::string hex;
                    hex.reserve(static_cast<size_t>(fieldLen) * 2);
                    static const char kHex[] = "0123456789ABCDEF";
                    for (uint32_t j = 0; j < fieldLen; j++) {
                        uint8_t value = raw[j];
                        hex.push_back(kHex[(value >> 4) & 0x0F]);
                        hex.push_back(kHex[value & 0x0F]);
                    }
                    sstream << columnName << "=X'" << hex << "'";
                    handled = true;
                }
                    break;
                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE: {
                    StateData data;
                    double value = 0.0;
                    if (fieldLen == 4) {
                        value = float4get(raw);
                        sstream << columnName << "=" << "F32!" << value;
                    } else if (fieldLen == 8) {
                        value = float8get(raw);
                        sstream << columnName << "=" << "F64!" << value;
                    } else {
                        sstream << columnName << "=";
                    }
                    data.Set(value);
                    pushItem(std::move(data));
                    handled = true;
                }
                    break;
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_YEAR:
                case MYSQL_TYPE_BOOL: {
                    StateData data;
                    const bool isUnsigned = tableMapEvent.isUnsigned(i);
                    if (isUnsigned) {
                        uint64_t value = 0;
                        switch (fieldLen) {
                            case 8:
                                value = static_cast<uint64_t>(uint8korr(raw));
                                sstream << columnName << "=" << "U64!" << value;
                                break;
                            case 4:
                                value = static_cast<uint64_t>(uint4korr(raw));
                                sstream << columnName << "=" << "U32!" << value;
                                break;
                            case 3:
                                value = static_cast<uint64_t>(uint3korr(raw));
                                sstream << columnName << "=" << "U24!" << value;
                                break;
                            case 2:
                                value = static_cast<uint64_t>(uint2korr(raw));
                                sstream << columnName << "=" << "U16!" << value;
                                break;
                            case 1:
                                value = static_cast<uint64_t>(raw[0]);
                                sstream << columnName << "=" << "U8!" << value;
                                break;
                            default:
                                sstream << columnName << "=";
                                break;
                        }
                        data.Set(value);
                    } else {
                        int64_t value = 0;
                        switch (fieldLen) {
                            case 8:
                                value = static_cast<int64_t>(sint8korr(raw));
                                sstream << columnName << "=" << "I64!" << value;
                                break;
                            case 4:
                                value = static_cast<int64_t>(sint4korr(raw));
                                sstream << columnName << "=" << "I32!" << value;
                                break;
                            case 3:
                                value = static_cast<int64_t>(sint3korr(raw));
                                sstream << columnName << "=" << "I24!" << value;
                                break;
                            case 2:
                                value = static_cast<int64_t>(sint2korr(raw));
                                sstream << columnName << "=" << "I16!" << value;
                                break;
                            case 1:
                                value = static_cast<int64_t>(static_cast<int8_t>(raw[0]));
                                sstream << columnName << "=" << "I8!" << value;
                                break;
                            default:
                                sstream << columnName << "=";
                                break;
                        }
                        data.Set(value);
                    }
                    pushItem(std::move(data));
                    handled = true;
                }
                    break;
                default:
                    break;
            }

            if (!handled) {
                uint32_t dataLen = fieldLen;
                size_t prefixLen = 0;
                const uchar *dataPtr = raw;
                bool treatAsInteger = false;
                bool hasLengthPrefix = false;
                switch (mysqlType) {
                    case MYSQL_TYPE_VARCHAR:
                    case MYSQL_TYPE_VAR_STRING:
                        if (mysqlMeta == 0 && columnSize < 0) {
                            prefixLen = static_cast<size_t>(-columnSize);
                        } else {
                            prefixLen = (mysqlMeta > 255) ? 2 : 1;
                        }
                        hasLengthPrefix = true;
                        break;
                    case MYSQL_TYPE_STRING: {
                        uint8_t realType = static_cast<uint8_t>(mysqlMeta >> 8);
                        uint8_t packLen = static_cast<uint8_t>(mysqlMeta & 0xFF);
                        if (realType == MYSQL_TYPE_ENUM || realType == MYSQL_TYPE_SET) {
                            treatAsInteger = true;
                            dataLen = packLen;
                            dataPtr = raw;
                        } else {
                            if (mysqlMeta == 0 && columnSize < 0) {
                                prefixLen = static_cast<size_t>(-columnSize);
                            } else {
                                prefixLen = (max_display_length_for_field(MYSQL_TYPE_STRING, mysqlMeta) > 255) ? 2 : 1;
                            }
                            hasLengthPrefix = true;
                        }
                    }
                        break;
                    case MYSQL_TYPE_TINY_BLOB:
                    case MYSQL_TYPE_BLOB:
                    case MYSQL_TYPE_MEDIUM_BLOB:
                    case MYSQL_TYPE_LONG_BLOB:
                    case MYSQL_TYPE_GEOMETRY:
                    case MYSQL_TYPE_JSON:
                        if (mysqlMeta == 0 && columnSize < 0) {
                            prefixLen = static_cast<size_t>(-columnSize);
                        } else {
                            prefixLen = mysqlMeta;
                        }
                        hasLengthPrefix = true;
                        break;
                    case MYSQL_TYPE_BIT:
                        dataLen = fieldLen;
                        dataPtr = raw;
                        break;
                    case MYSQL_TYPE_DATE:
                    case MYSQL_TYPE_TIME:
                    case MYSQL_TYPE_DATETIME:
                    case MYSQL_TYPE_TIMESTAMP:
                    case MYSQL_TYPE_TIME2:
                    case MYSQL_TYPE_DATETIME2:
                    case MYSQL_TYPE_TIMESTAMP2:
                        dataLen = fieldLen;
                        dataPtr = raw;
                        break;
                    default:
                        dataLen = fieldLen;
                        dataPtr = raw;
                        break;
                }

                if (hasLengthPrefix) {
                    if (prefixLen > remaining) {
                        warning("RowEvent: length prefix exceeds remaining bytes (prefix=%zu, remaining=%zu)",
                                prefixLen, remaining);
                        return std::make_pair(sstream.str(), static_cast<int>(dataSize - basePosSize));
                    }
                    dataLen = readLengthLE(raw, prefixLen);
                    dataPtr = raw + prefixLen;
                    size_t maxData = remaining - prefixLen;
                    if (dataLen > maxData) {
                        warning("RowEvent: data length exceeds remaining bytes (len=%u, remaining=%zu)",
                                dataLen, remaining);
                        dataLen = static_cast<uint32_t>(maxData);
                    }
                    fieldLen = static_cast<uint32_t>(prefixLen) + dataLen;
                } else if (fieldLen == 0 && fieldLenUnknown && mysqlType != MYSQL_TYPE_NULL) {
                    warning("RowEvent: unknown field length for type %u", static_cast<unsigned int>(mysqlType));
                    return std::make_pair(sstream.str(), static_cast<int>(dataSize - basePosSize));
                } else if (fieldLen > remaining) {
                    warning("RowEvent: field length exceeds remaining bytes (len=%u, remaining=%zu)",
                            fieldLen, remaining);
                    fieldLen = static_cast<uint32_t>(remaining);
                    dataLen = fieldLen;
                } else if (fieldLen >= prefixLen && dataLen > (fieldLen - prefixLen)) {
                    dataLen = fieldLen - static_cast<uint32_t>(prefixLen);
                }

                if (treatAsInteger) {
                    StateData data;
                    uint64_t value = 0;
                    switch (dataLen) {
                        case 8:
                            value = static_cast<uint64_t>(uint8korr(dataPtr));
                            sstream << columnName << "=" << "U64!" << value;
                            break;
                        case 4:
                            value = static_cast<uint64_t>(uint4korr(dataPtr));
                            sstream << columnName << "=" << "U32!" << value;
                            break;
                        case 3:
                            value = static_cast<uint64_t>(uint3korr(dataPtr));
                            sstream << columnName << "=" << "U24!" << value;
                            break;
                        case 2:
                            value = static_cast<uint64_t>(uint2korr(dataPtr));
                            sstream << columnName << "=" << "U16!" << value;
                            break;
                        case 1:
                            value = static_cast<uint64_t>(dataPtr[0]);
                            sstream << columnName << "=" << "U8!" << value;
                            break;
                        default:
                            sstream << columnName << "=";
                            break;
                    }
                    data.Set(value);
                    pushItem(std::move(data));
                } else {
                    std::string strValue(reinterpret_cast<const char *>(dataPtr), dataLen);
                    sstream << columnName << "=" << strValue;
                    StateData data;
                    data.Set(strValue.c_str(), dataLen);
                    pushItem(std::move(data));
                }
            }

            rowSize += static_cast<int>(fieldLen);
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
