//
// Created by cheesekun on 8/12/22.
//

#include <stdexcept>

#include <fmt/core.h>

#include "../DBEvent.hpp"

#include "MySQLBinaryLogReader.hpp"
#include "ProtocolBinary.hpp"

namespace ultraverse::mariadb {
    MySQLBinaryLogReader::MySQLBinaryLogReader(const std::string &filename):
        BinaryLogReaderBase(filename),
    
        _logger(createLogger("MySQLBinaryLogReader")),
        _filename(filename),
        
        _pos(0),
        _hasChecksum(false)
    {
    
    }
    
    void MySQLBinaryLogReader::open() {
        _logger->info("opening binary log: {}", _filename);
        
        _stream = std::ifstream(
            _filename,
            std::ios::in | std::ios::binary
        );
        _pos = 0;
    
        if (!_stream.good()) {
            throw std::runtime_error(fmt::format(
                "could not open log file {}: {}",
                _filename, strerror(errno)
            ));
        }
    }
    
    void MySQLBinaryLogReader::close() {
        _logger->info("closing binary log: {}", _filename);
        _stream.close();
    }
    
    bool MySQLBinaryLogReader::seek(int64_t position) {
        _logger->trace("seeking offset: {}", position);
        _stream.seekg(position);
        _pos = position;
        
        return _stream.good();
    }
    
    bool MySQLBinaryLogReader::next() {
        auto header = readHeader();
        
        _currentEvent = nullptr;
        
        if (!_stream.good()) {
            return false;
        }
        
        auto &eventType = header->event_type;
        
        if (eventType == internal::START_EVENT_V3) {
            throw std::runtime_error("unsupported version");
        } else if (eventType == internal::FORMAT_DESCRIPTION_EVENT) {
            readFormatDescriptionEvent(header);
        } else if (eventType == internal::QUERY_EVENT) {
            _currentEvent = readQueryEvent(header);
        } else if (eventType == internal::XID_EVENT) {
            _currentEvent = readXIDEvent(header);
        } else if (eventType == internal::TABLE_MAP_EVENT) {
            _currentEvent = readTableMapEvent(header);
        } else if (eventType == internal::ROWS_QUERY_LOG_EVENT) {
            _currentEvent = readRowAnnotationEvent(header);
        } else if (eventType == internal::WRITE_ROWS_EVENT) {
            _currentEvent = readRowEvent(header, RowEvent::INSERT, false);
        } else if (eventType == internal::UPDATE_ROWS_EVENT) {
            _currentEvent = readRowEvent(header, RowEvent::UPDATE, false);
        } else if (eventType == internal::DELETE_ROWS_EVENT) {
            _currentEvent = readRowEvent(header, RowEvent::DELETE, false);
        } else {
            _logger->trace("unsupported event type: {}", (int) eventType);
        }
        
        seek(header->log_pos);
        
        return true;
    }
    
    int MySQLBinaryLogReader::pos() {
        return _pos;
    }
    
    std::shared_ptr<base::DBEvent> MySQLBinaryLogReader::currentEvent() {
        return _currentEvent;
    }
    
    std::shared_ptr<internal::EventHeader> MySQLBinaryLogReader::readHeader() {
        auto header = std::make_shared<internal::EventHeader>();
        _stream.read((char *) header.get(), sizeof(internal::EventHeader));
    
        _logger->trace("header read: {}", header->event_type);
        
        return header;
    }
    
    void
    MySQLBinaryLogReader::readFormatDescriptionEvent(std::shared_ptr<internal::EventHeader> header) {
        auto eventSize = header->event_size;
        auto formatDescriptionEvent = std::make_shared<internal::FormatDescriptionEvent>();
        
        auto restBodyLength = eventSize - sizeof(internal::EventHeader) - sizeof(internal::FormatDescriptionEvent);
        auto eventLengths = std::unique_ptr<uint8_t>(new uint8_t[restBodyLength]);
        
        _stream.read((char *) formatDescriptionEvent.get(), sizeof(internal::FormatDescriptionEvent));
        _stream.read((char *) eventLengths.get(), restBodyLength);
        
        auto fdEventSize = eventLengths.get()[internal::FORMAT_DESCRIPTION_EVENT - 1];
        
        // FIXME
        bool hasChecksum = (eventSize - sizeof(internal::EventHeader)) > fdEventSize;
        
        if (hasChecksum) {
            uint8_t checksumMethod = eventLengths.get()[restBodyLength - 5];
            
            if (checksumMethod != 0) {
                _hasChecksum = true;
                _logger->warn("the log file includes CRC32 checksum, but this will be ignored");
            }
        }
    }
    
    std::shared_ptr<base::QueryEventBase>
    MySQLBinaryLogReader::readQueryEvent(std::shared_ptr<internal::EventHeader> header) {
        auto postHeader = std::make_shared<internal::QueryEventPostHeader>();
        _stream.read((char *) postHeader.get(), sizeof(internal::QueryEventPostHeader));
        
        auto statusVarsLength = postHeader->status_vars_length;
        auto statusVars = std::shared_ptr<uint8_t>(new uint8_t[statusVarsLength]);
        _stream.read((char *) statusVars.get(), statusVarsLength);
        
        auto schemaLength = postHeader->schema_length;
        auto schemaCStr = std::shared_ptr<uint8_t>(new uint8_t[schemaLength + 1]);
        _stream.read((char *) schemaCStr.get(), schemaLength + 1);
        
        auto queryLength =
            header->event_size - (
                (sizeof(internal::EventHeader) + sizeof(internal::QueryEventPostHeader)) +
                (statusVarsLength + schemaLength + 1) +
                (_hasChecksum ? 4 : 0)
            );
        
        auto queryCStr = std::shared_ptr<uint8_t>(new uint8_t[queryLength]);
        _stream.read((char *) queryCStr.get(), queryLength);
    
        std::string schema((char *) schemaCStr.get(), (int) schemaLength);
        std::string query((char *) queryCStr.get(), (int) queryLength);
        
        auto timestamp = header->timestamp;
        
        return std::make_shared<QueryEvent>(schema, query, timestamp);
    }
    
    std::shared_ptr<base::TransactionIDEventBase>
    MySQLBinaryLogReader::readXIDEvent(std::shared_ptr<internal::EventHeader> header) {
        auto xidEvent = std::make_shared<internal::XIDEvent>();
        _stream.read((char *) xidEvent.get(), sizeof(internal::XIDEvent));
    
        auto timestamp = header->timestamp;
        auto xid = xidEvent->xid;
        
        return std::make_shared<TransactionIDEvent>(xid, timestamp);
    }
    
    std::shared_ptr<TableMapEvent> MySQLBinaryLogReader::readTableMapEvent(std::shared_ptr<internal::EventHeader> header) {
        auto postHeader = std::make_shared<internal::TableMapEventPostHeader>();
        _stream.read((char *) postHeader.get(), sizeof(internal::TableMapEventPostHeader));
    
        uint64_t tableId = (postHeader->table_id_high << 4) | postHeader->table_id_low;
        
        uint8_t schemaNameLength = 0;
        uint8_t tableNameLength = 0;
        _stream.read((char *) &schemaNameLength, sizeof(uint8_t));
        
        auto schemaNameCStr = std::shared_ptr<uint8_t>(new uint8_t[schemaNameLength + 1]);
        _stream.read((char *) schemaNameCStr.get(), schemaNameLength + 1);
        
        _stream.read((char *) &tableNameLength, sizeof(uint8_t));
    
        auto tableNameCStr = std::shared_ptr<uint8_t>(new uint8_t[tableNameLength + 1]);
        _stream.read((char *) tableNameCStr.get(), tableNameLength + 1);
    
        uint8_t columns = 0;
        _stream.read((char *) &columns, sizeof(uint8_t));
        
        auto columnTypeDef = std::shared_ptr<uint8_t>(new uint8_t[columns]);
        _stream.read((char *) columnTypeDef.get(), columns);
        
        std::string schemaName((char *) schemaNameCStr.get(), schemaNameLength);
        std::string tableName((char *) tableNameCStr.get(), tableNameLength);
        
        std::vector<std::pair<column_type::Value, int>> columnTypeDef2;
        columnTypeDef2.reserve(columns);
    
        uint8_t metadataLength = 0;
        _stream.read((char *) &metadataLength, sizeof(uint8_t));
        
        std::unique_ptr<uint8_t> metadata(new uint8_t[metadataLength]);
        _stream.read((char *) metadata.get(), metadataLength);
        
        int metadataPos = 0;
        
        for (int i = 0; i < columns; i++) {
            using namespace internal;
            switch (columnTypeDef.get()[i]) {
                case MYSQL_TYPE_DECIMAL:
                    columnTypeDef2.emplace_back(column_type::STRING, -1);
                    break;
                    
                case MYSQL_TYPE_STRING: {
                    uint8_t size = metadata.get()[metadataPos + 2];
                    metadataPos += 2;
                    
                    columnTypeDef2.emplace_back(column_type::STRING, size);
                }
                    break;
                case MYSQL_TYPE_GEOMETRY:
                case MYSQL_TYPE_BLOB: {
                    uint8_t size = metadata.get()[metadataPos];
                    metadataPos += 1;
    
                    columnTypeDef2.emplace_back(column_type::STRING, size);
                }
                    break;
                
                case MYSQL_TYPE_VAR_STRING:
                case MYSQL_TYPE_VARCHAR: {
                    uint16_t maximumLength = *reinterpret_cast<uint16_t *>(metadata.get() + metadataPos);
                    metadataPos += 2;
    
                    columnTypeDef2.emplace_back(
                        column_type::STRING,
                        (maximumLength <= UINT8_MAX) ? 1 : 2
                    );
                }
                    break;
    
                case MYSQL_TYPE_BIT:
                case MYSQL_TYPE_ENUM:
                case MYSQL_TYPE_SET:
                case MYSQL_TYPE_NEWDECIMAL:
                {
                    metadataPos += 2;
    
                    columnTypeDef2.emplace_back(column_type::STRING, -1);
                }
                    break;
    

                
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_TINY_BLOB:
                    metadataPos += 1;
                    columnTypeDef2.emplace_back(column_type::STRING, -1);
                    break;
                    
                case MYSQL_TYPE_LONGLONG:
                    columnTypeDef2.emplace_back(column_type::INTEGER, 8);
                    break;
                    
                case MYSQL_TYPE_LONG:
                    columnTypeDef2.emplace_back(column_type::INTEGER, 4);
                    break;
    
                case MYSQL_TYPE_INT24:
                    columnTypeDef2.emplace_back(column_type::INTEGER, 3);
                    
                case MYSQL_TYPE_SHORT:
                    columnTypeDef2.emplace_back(column_type::INTEGER, 2);
                    break;
    
                case MYSQL_TYPE_YEAR:
                case MYSQL_TYPE_TINY:
                    columnTypeDef2.emplace_back(column_type::INTEGER, 1);
                    break;
    
                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE: {
                    uint8_t size = metadata.get()[metadataPos];
                    metadataPos += 1;
                    
                    columnTypeDef2.emplace_back(column_type::FLOAT, size);
                    break;
                }
                
                // TODO: https://mariadb.com/kb/en/rows_event_v1v2/
                case MYSQL_TYPE_DATE:
                    columnTypeDef2.emplace_back(column_type::DATETIME, 3);
                    break;
                case MYSQL_TYPE_DATETIME:
                    columnTypeDef2.emplace_back(column_type::DATETIME, 8);
                    break;
                case MYSQL_TYPE_TIMESTAMP:
                    columnTypeDef2.emplace_back(column_type::DATETIME, 4);
                    break;
                case MYSQL_TYPE_TIME:
                    columnTypeDef2.emplace_back(column_type::DATETIME, 3);
                    break;
                case MYSQL_TYPE_DATETIME2: {
                    uint8_t fractionalLength = *reinterpret_cast<uint8_t *>(metadata.get() + metadataPos);
                    metadataPos += 1;
                    columnTypeDef2.emplace_back(column_type::DATETIME, 5);
                }
                    break;
                case MYSQL_TYPE_TIME2: {
                    uint8_t fractionalLength = *reinterpret_cast<uint8_t *>(metadata.get() + metadataPos);
                    metadataPos += 1;
                    columnTypeDef2.emplace_back(column_type::DATETIME, 3);
                }
                    break;
                case MYSQL_TYPE_TIMESTAMP2: {
                    uint8_t fractionalLength = *reinterpret_cast<uint8_t *>(metadata.get() + metadataPos);
                    metadataPos += 1;
                    columnTypeDef2.emplace_back(column_type::DATETIME, 4 + ((fractionalLength + 1) / 2));
                }
                    break;
                    
                default:
                    throw std::runtime_error(
                        fmt::format("unsupported field type {}.", columnTypeDef.get()[i])
                    );
            }
        }
        
        
        uint8_t skipBytes = ((columns + 7) / 8);
        std::unique_ptr<uint8_t> unused(new uint8_t[skipBytes]);
        _stream.read((char *) unused.get(), skipBytes);
        
        const int pos = _stream.tellg();
        int optionalMetadataLength = header->event_size - (
            sizeof(internal::TableMapEventPostHeader) +
            sizeof(uint8_t) + schemaNameLength +
            sizeof(uint8_t) + tableNameLength +
            sizeof(uint8_t) + columns +
            sizeof(uint8_t) + metadataLength +
            skipBytes
        ) - (_hasChecksum ? 4 : 0);
        
        std::vector<std::string> columnNames;
        
        while (_stream.good() && _stream.tellg() < (pos + optionalMetadataLength)) {
            uint8_t type = 0;
            _stream.read((char *) &type, sizeof(uint8_t));
            
            uint8_t size = 0;
            _stream.read((char *) &size, sizeof(uint8_t));
            
            if (type == internal::COLUMN_NAME) {
                uint8_t readBytes = 0;
                while (readBytes < size) {
                    uint8_t columnNameLength = 0;
                    _stream.read((char *) &columnNameLength, sizeof(uint8_t));
                    readBytes += 1;
    
                    std::unique_ptr<uint8_t> columnNameCStr(new uint8_t[columnNameLength]);
                    _stream.read((char *) columnNameCStr.get(), columnNameLength);
                    readBytes += columnNameLength;
                    
                    columnNames.emplace_back((char *) columnNameCStr.get(), columnNameLength);
                }
            } else {
                std::unique_ptr<uint8_t> _unused(new uint8_t[size]);
                _stream.read((char *) _unused.get(), size);
            }
        }
        
        
        auto timestamp = header->timestamp;
        
        return std::make_shared<TableMapEvent>(
            tableId,
            schemaName, tableName,
            columnTypeDef2,
            columnNames,
            timestamp
        );
    }
    
    std::shared_ptr<RowQueryEvent>
    MySQLBinaryLogReader::readRowAnnotationEvent(std::shared_ptr<internal::EventHeader> header) {
        auto queryLength = header->event_size - sizeof(internal::EventHeader) - 1;
        uint8_t unused = 0;
        _stream.read((char *) &unused, sizeof(uint8_t));

        auto queryCStr = std::shared_ptr<uint8_t>(new uint8_t[queryLength]);
        _stream.read((char *) queryCStr.get(), (int) queryLength);
        
        std::string query((char *) queryCStr.get(), (int) queryLength);
        auto timestamp = header->timestamp;
        
        return std::make_shared<RowQueryEvent>(query, timestamp);
    }
    
    std::shared_ptr<RowEvent>
    MySQLBinaryLogReader::readRowEvent(std::shared_ptr<internal::EventHeader> header, RowEvent::Type eventType, bool isV2) {
        int totalRead = sizeof(internal::EventHeader);
        
        auto postHeader = std::make_shared<internal::RowEventPostHeader>();
        _stream.read((char *) postHeader.get(), sizeof(internal::RowEventPostHeader));
        totalRead += sizeof(internal::RowEventPostHeader);
        
        
        if (isV2) {
            auto postHeaderV2 = std::make_shared<internal::RowEventPostHeaderV2>();
            _stream.read((char *) postHeaderV2.get(), sizeof(internal::RowEventPostHeaderV2));
            totalRead += sizeof(internal::RowEventPostHeaderV2);
    
            auto extraData = std::shared_ptr<uint8_t>(new uint8_t[postHeaderV2->extra_data_length]);
            _stream.read((char *) extraData.get(), postHeaderV2->extra_data_length);
            totalRead += postHeaderV2->extra_data_length;
        }
        
        uint64_t tableId = (postHeader->table_id_high << 4) | postHeader->table_id_low;
        _logger->trace("tableId: {}", (int) tableId);
    
        uint16_t FIXME_UNKNOWN_FIELD = 0;
        _stream.read((char *) &FIXME_UNKNOWN_FIELD, sizeof(uint16_t));
        totalRead += sizeof(uint16_t);
        
        uint8_t columns = 0;
        _stream.read((char *) &columns, sizeof(uint8_t));
        totalRead += sizeof(uint8_t);
        
        auto colsSize = (int) ((columns + 7) / 8);
        
        auto bitmapBefore = std::unique_ptr<uint8_t>(new uint8_t[colsSize]);
        auto bitmapAfter  = std::unique_ptr<uint8_t>(new uint8_t[colsSize]);
        
        _stream.read((char *) bitmapBefore.get(), colsSize);
        totalRead += colsSize;
        
        if (eventType == RowEvent::UPDATE) {
            _stream.read((char *) bitmapAfter.get(), colsSize);
            totalRead += colsSize;
        }
        
        auto dataSize = header->event_size - totalRead;
        auto eventSize = header->event_size;
        auto data = std::shared_ptr<uint8_t>(new uint8_t[dataSize]);
        _stream.read((char *) data.get(), dataSize);
        
        auto timestamp = header->timestamp;
        uint16_t flags = postHeader->flags;
        
        return std::make_shared<RowEvent>(
            eventType,
            tableId, columns,
            data, dataSize,
            timestamp, flags
        );
    }
}