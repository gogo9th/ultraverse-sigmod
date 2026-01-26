//
// Created by ultraverse on 1/21/26.
//

#include "MySQLBinaryLogReaderV2.hpp"

#include <cerrno>
#include <cstring>
#include <utility>

#include <fmt/core.h>

#include <mysql/mysql.h>

namespace ultraverse::mariadb {
    namespace {
        constexpr const char *kDefaultServerVersion = "9.6.0";
        constexpr uint16_t kBinlogVersion = BINLOG_VERSION;
        constexpr size_t kLogEventMinimalHeaderLen = LOG_EVENT_MINIMAL_HEADER_LEN;
        constexpr size_t kEventTypeOffset = EVENT_TYPE_OFFSET;
        constexpr size_t kEventLenOffset = EVENT_LEN_OFFSET;
        constexpr size_t kLogPosOffset = LOG_POS_OFFSET;
        constexpr size_t kBinlogChecksumLen = BINLOG_CHECKSUM_LEN;

        uint16_t readUint16LE(const unsigned char *ptr) {
            return static_cast<uint16_t>(ptr[0]) |
                   (static_cast<uint16_t>(ptr[1]) << 8);
        }

        uint32_t readUint32LE(const unsigned char *ptr) {
            return static_cast<uint32_t>(ptr[0]) |
                   (static_cast<uint32_t>(ptr[1]) << 8) |
                   (static_cast<uint32_t>(ptr[2]) << 16) |
                   (static_cast<uint32_t>(ptr[3]) << 24);
        }

        uint64_t readUint48LE(const unsigned char *ptr) {
            uint64_t value = 0;
            for (int i = 0; i < 6; i++) {
                value |= (static_cast<uint64_t>(ptr[i]) << (i * 8));
            }
            return value;
        }

        mariadb::IntVarEvent::Type mapIntVarType(uint8_t type) {
            switch (type) {
                case mysql::binlog::event::Intvar_event::LAST_INSERT_ID_EVENT:
                    return mariadb::IntVarEvent::LAST_INSERT_ID;
                case mysql::binlog::event::Intvar_event::INSERT_ID_EVENT:
                    return mariadb::IntVarEvent::INSERT_ID;
                default:
                    return mariadb::IntVarEvent::INVALID;
            }
        }

        mariadb::UserVarEvent::ValueType mapUserVarType(uint8_t type) {
            switch (type) {
                case STRING_RESULT:
                    return mariadb::UserVarEvent::STRING;
                case REAL_RESULT:
                    return mariadb::UserVarEvent::REAL;
                case INT_RESULT:
                    return mariadb::UserVarEvent::INT;
                case DECIMAL_RESULT:
                    return mariadb::UserVarEvent::DECIMAL;
                default:
                    return mariadb::UserVarEvent::STRING;
            }
        }

        bool hasSignednessInfo(enum_field_types type) {
            switch (type) {
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_YEAR:
                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL:
                    return true;
                default:
                    return false;
            }
        }

        bool readNetFieldLength(const unsigned char *&ptr, const unsigned char *end, uint64_t &out) {
            if (ptr >= end) {
                return false;
            }
            uint8_t value1 = *ptr++;
            if (value1 < 0xfb) {
                out = value1;
                return true;
            }
            if (value1 == 0xfb) {
                return false;
            }
            if (value1 == 0xfc) {
                if (ptr + 2 > end) return false;
                out = readUint16LE(ptr);
                ptr += 2;
                return true;
            }
            if (value1 == 0xfd) {
                if (ptr + 3 > end) return false;
                out = static_cast<uint64_t>(ptr[0]) |
                      (static_cast<uint64_t>(ptr[1]) << 8) |
                      (static_cast<uint64_t>(ptr[2]) << 16);
                ptr += 3;
                return true;
            }
            if (value1 == 0xfe) {
                if (ptr + 8 > end) return false;
                out = static_cast<uint64_t>(ptr[0]) |
                      (static_cast<uint64_t>(ptr[1]) << 8) |
                      (static_cast<uint64_t>(ptr[2]) << 16) |
                      (static_cast<uint64_t>(ptr[3]) << 24) |
                      (static_cast<uint64_t>(ptr[4]) << 32) |
                      (static_cast<uint64_t>(ptr[5]) << 40) |
                      (static_cast<uint64_t>(ptr[6]) << 48) |
                      (static_cast<uint64_t>(ptr[7]) << 56);
                ptr += 8;
                return true;
            }
            return false;
        }

        uint64_t eventTimestamp(const std::vector<unsigned char> &buffer) {
            if (buffer.size() < kLogEventMinimalHeaderLen) {
                return 0;
            }
            return readUint32LE(buffer.data());
        }
    } // namespace

    MySQLBinaryLogReaderV2::MySQLBinaryLogReaderV2(const std::string &filename):
        BinaryLogReaderBase(filename),
        _logger(createLogger("MySQLBinaryLogReaderV2")),
        _filename(filename),
        _pos(0),
        _checksumAlg(mysql::binlog::event::BINLOG_CHECKSUM_ALG_UNDEF)
    {
        ensureDefaultFde();
    }

    void MySQLBinaryLogReaderV2::ensureDefaultFde() {
        if (_fde == nullptr) {
            _fde = std::make_unique<mysql::binlog::event::Format_description_event>(
                kBinlogVersion,
                kDefaultServerVersion
            );
            _checksumAlg = _fde->footer()->checksum_alg;
        }
    }

    void MySQLBinaryLogReaderV2::open() {
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

    void MySQLBinaryLogReaderV2::close() {
        _logger->info("closing binary log: {}", _filename);
        _stream.close();
    }

    bool MySQLBinaryLogReaderV2::seek(int64_t position) {
        _logger->trace("seeking offset: {}", position);
        _stream.seekg(position);
        _pos = position;

        return _stream.good();
    }

    bool MySQLBinaryLogReaderV2::readNextEventBuffer(std::vector<unsigned char> &buffer) {
        buffer.clear();
        unsigned char header[kLogEventMinimalHeaderLen];

        _stream.read(reinterpret_cast<char *>(header), sizeof(header));
        if (!_stream.good()) {
            return false;
        }

        uint32_t eventSize = readUint32LE(header + kEventLenOffset);
        if (eventSize < kLogEventMinimalHeaderLen) {
            _logger->warn("invalid event size: {}", eventSize);
            return false;
        }

        buffer.resize(eventSize);
        std::memcpy(buffer.data(), header, sizeof(header));

        auto bodySize = eventSize - kLogEventMinimalHeaderLen;
        _stream.read(reinterpret_cast<char *>(buffer.data() + sizeof(header)), bodySize);
        if (!_stream.good()) {
            _logger->warn("failed to read event body (size={})", eventSize);
            return false;
        }

        uint32_t logPos = readUint32LE(buffer.data() + kLogPosOffset);
        auto tellPos = static_cast<int>(_stream.tellg());
        _pos = (logPos != 0) ? static_cast<int>(logPos) : tellPos;

        return true;
    }

    bool MySQLBinaryLogReaderV2::next() {
        _currentEvent = nullptr;

        if (!_payloadEventQueue.empty()) {
            auto buffer = std::move(_payloadEventQueue.front());
            _payloadEventQueue.pop_front();
            _currentEvent = decodeEventBuffer(buffer, true);
            return true;
        }

        std::vector<unsigned char> buffer;
        if (!readNextEventBuffer(buffer)) {
            return false;
        }

        if (buffer.size() < kLogEventMinimalHeaderLen) {
            _logger->warn("skipping truncated event");
            return true;
        }

        auto eventType = static_cast<mysql::binlog::event::Log_event_type>(
            buffer[kEventTypeOffset]
        );

        if (eventType == mysql::binlog::event::TRANSACTION_PAYLOAD_EVENT) {
            if (!handleTransactionPayloadEvent(buffer)) {
                _logger->warn("failed to decode transaction payload event, skipping");
            }
            return true;
        }

        _currentEvent = decodeEventBuffer(buffer, false);
        return true;
    }

    int MySQLBinaryLogReaderV2::pos() {
        return _pos;
    }

    std::shared_ptr<base::DBEvent> MySQLBinaryLogReaderV2::currentEvent() {
        return _currentEvent;
    }

    std::shared_ptr<base::DBEvent> MySQLBinaryLogReaderV2::decodeEventBuffer(
        const std::vector<unsigned char> &buffer,
        bool fromPayload
    ) {
        if (buffer.size() < kLogEventMinimalHeaderLen) {
            return nullptr;
        }

        ensureDefaultFde();

        auto eventType = static_cast<mysql::binlog::event::Log_event_type>(
            buffer[kEventTypeOffset]
        );

        auto eventSize = readUint32LE(buffer.data() + kEventLenOffset);
        if (eventSize != buffer.size()) {
            eventSize = buffer.size();
        }

        if (!fromPayload &&
            _checksumAlg == mysql::binlog::event::BINLOG_CHECKSUM_ALG_CRC32 &&
            mysql::binlog::event::Log_event_footer::event_checksum_test(
                const_cast<unsigned char *>(buffer.data()),
                eventSize,
                _checksumAlg)) {
            _logger->warn("checksum mismatch, skipping event type {}", static_cast<int>(eventType));
            return nullptr;
        }

        if (eventType == mysql::binlog::event::FORMAT_DESCRIPTION_EVENT) {
            auto alg = mysql::binlog::event::Log_event_footer::get_checksum_alg(
                reinterpret_cast<const char *>(buffer.data()),
                eventSize
            );
            if (alg != mysql::binlog::event::BINLOG_CHECKSUM_ALG_OFF &&
                alg != mysql::binlog::event::BINLOG_CHECKSUM_ALG_UNDEF &&
                mysql::binlog::event::Log_event_footer::event_checksum_test(
                    const_cast<unsigned char *>(buffer.data()),
                    eventSize,
                    alg)) {
                _logger->warn("checksum mismatch for format description event");
                return nullptr;
            }

            auto nextFde = std::make_unique<mysql::binlog::event::Format_description_event>(
                reinterpret_cast<const char *>(buffer.data()),
                _fde.get()
            );

            if (!nextFde->header()->get_is_valid()) {
                _logger->warn("invalid format description event, skipping");
                return nullptr;
            }

            _fde = std::move(nextFde);
            _checksumAlg = _fde->footer()->checksum_alg;

            return nullptr;
        }

        mysql::binlog::event::Format_description_event fdeForEvent = *_fde;
        if (fromPayload && fdeForEvent.footer()->checksum_alg == mysql::binlog::event::BINLOG_CHECKSUM_ALG_CRC32) {
            fdeForEvent.footer()->checksum_alg = mysql::binlog::event::BINLOG_CHECKSUM_ALG_OFF;
        }

        switch (eventType) {
            case mysql::binlog::event::QUERY_EVENT: {
                mysql::binlog::event::Query_event event(
                    reinterpret_cast<const char *>(buffer.data()),
                    &fdeForEvent,
                    eventType
                );
                if (!event.header()->get_is_valid()) {
                    _logger->warn("invalid query event, skipping");
                    return nullptr;
                }

                std::string schema(event.db, event.db_len);
                std::string statement(event.query, event.q_len);
                return std::make_shared<QueryEvent>(schema, statement, event.header()->when.tv_sec);
            }
            case mysql::binlog::event::XID_EVENT: {
                mysql::binlog::event::Xid_event event(
                    reinterpret_cast<const char *>(buffer.data()),
                    &fdeForEvent
                );
                if (!event.header()->get_is_valid()) {
                    _logger->warn("invalid xid event, skipping");
                    return nullptr;
                }
                return std::make_shared<TransactionIDEvent>(event.xid, event.header()->when.tv_sec);
            }
            case mysql::binlog::event::INTVAR_EVENT: {
                mysql::binlog::event::Intvar_event event(
                    reinterpret_cast<const char *>(buffer.data()),
                    &fdeForEvent
                );
                if (!event.header()->get_is_valid()) {
                    _logger->warn("invalid intvar event, skipping");
                    return nullptr;
                }
                return std::make_shared<IntVarEvent>(
                    mapIntVarType(event.type),
                    event.val,
                    event.header()->when.tv_sec
                );
            }
            case mysql::binlog::event::RAND_EVENT: {
                mysql::binlog::event::Rand_event event(
                    reinterpret_cast<const char *>(buffer.data()),
                    &fdeForEvent
                );
                if (!event.header()->get_is_valid()) {
                    _logger->warn("invalid rand event, skipping");
                    return nullptr;
                }
                return std::make_shared<RandEvent>(
                    event.seed1,
                    event.seed2,
                    event.header()->when.tv_sec
                );
            }
            case mysql::binlog::event::USER_VAR_EVENT: {
                mysql::binlog::event::User_var_event event(
                    reinterpret_cast<const char *>(buffer.data()),
                    &fdeForEvent
                );
                if (!event.header()->get_is_valid()) {
                    _logger->warn("invalid user var event, skipping");
                    return nullptr;
                }

                std::string name;
                if (event.name != nullptr && event.name_len > 0) {
                    name.assign(event.name, event.name_len);
                }

                std::string value;
                if (!event.is_null && event.val != nullptr && event.val_len > 0) {
                    value.assign(event.val, event.val_len);
                }

                bool isUnsigned = (event.flags & mysql::binlog::event::User_var_event::UNSIGNED_F) != 0;

                return std::make_shared<UserVarEvent>(
                    std::move(name),
                    mapUserVarType(static_cast<uint8_t>(event.type)),
                    event.is_null,
                    isUnsigned,
                    event.charset_number,
                    std::move(value),
                    event.header()->when.tv_sec
                );
            }
            case mysql::binlog::event::TABLE_MAP_EVENT: {
                mysql::binlog::event::Table_map_event event(
                    reinterpret_cast<const char *>(buffer.data()),
                    &fdeForEvent
                );
                if (!event.header()->get_is_valid()) {
                    _logger->warn("invalid table map event, skipping");
                    return nullptr;
                }
                return decodeTableMapEvent(event);
            }
            case mysql::binlog::event::ROWS_QUERY_LOG_EVENT:
                return decodeRowsQueryEvent(buffer, fromPayload);
            case mysql::binlog::event::OBSOLETE_WRITE_ROWS_EVENT_V1:
            case mysql::binlog::event::OBSOLETE_UPDATE_ROWS_EVENT_V1:
            case mysql::binlog::event::OBSOLETE_DELETE_ROWS_EVENT_V1:
            case mysql::binlog::event::WRITE_ROWS_EVENT:
            case mysql::binlog::event::UPDATE_ROWS_EVENT:
            case mysql::binlog::event::DELETE_ROWS_EVENT:
                return decodeRowsEvent(buffer, eventType, fromPayload);
            case mysql::binlog::event::PARTIAL_UPDATE_ROWS_EVENT:
                _logger->warn("partial update rows event is not supported, skipping");
                return nullptr;
            default:
                _logger->trace("unsupported event type: {}", static_cast<int>(eventType));
                return nullptr;
        }
    }

    std::shared_ptr<TableMapEvent> MySQLBinaryLogReaderV2::decodeTableMapEvent(
        mysql::binlog::event::Table_map_event &event
    ) {
        if (event.m_colcnt == 0) {
            _logger->warn("table map event has zero columns, skipping");
            return nullptr;
        }

        std::vector<std::string> columnNames;
        std::vector<bool> signednessBits;
        if (event.m_optional_metadata_len > 0) {
            mysql::binlog::event::Table_map_event::Optional_metadata_fields metadata(
                event.m_optional_metadata,
                event.m_optional_metadata_len
            );
            if (!metadata.is_valid) {
                _logger->warn("invalid optional metadata in table map event, skipping");
                return nullptr;
            }
            columnNames = metadata.m_column_name;
            signednessBits = metadata.m_signedness;
        }

        if (columnNames.size() != event.m_colcnt) {
            _logger->warn("column names missing in table map event (got {}, expected {}), skipping",
                          columnNames.size(), event.m_colcnt);
            return nullptr;
        }

        std::vector<std::pair<column_type::Value, int>> columnDefs;
        columnDefs.reserve(event.m_colcnt);
        std::vector<uint8_t> unsignedFlags;
        unsignedFlags.reserve(event.m_colcnt);
        std::vector<enum_field_types> mysqlTypes;
        mysqlTypes.reserve(event.m_colcnt);
        std::vector<uint16_t> mysqlMetadata;
        mysqlMetadata.reserve(event.m_colcnt);
        size_t numericIndex = 0;

        const unsigned char *metadata = event.m_field_metadata;
        size_t metadataSize = event.m_field_metadata_size;
        size_t metadataPos = 0;

        for (unsigned long i = 0; i < event.m_colcnt; i++) {
            auto binlogType = static_cast<enum_field_types>(event.m_coltype[i]);
            bool isUnsigned = false;
            if (hasSignednessInfo(binlogType)) {
                if (numericIndex < signednessBits.size()) {
                    isUnsigned = signednessBits[numericIndex];
                }
                numericIndex++;
            }
            unsignedFlags.push_back(isUnsigned ? 1 : 0);
            mysqlTypes.push_back(binlogType);

            auto requireMetadata = [&](size_t need) -> bool {
                if (metadataPos + need > metadataSize) {
                    _logger->warn("insufficient field metadata for table map event");
                    return false;
                }
                return true;
            };
            auto readMetadataBE2 = [&]() -> uint16_t {
                uint16_t value =
                    static_cast<uint16_t>(metadata[metadataPos]) << 8 |
                    static_cast<uint16_t>(metadata[metadataPos + 1]);
                metadataPos += 2;
                return value;
            };
            auto readMetadataLE2 = [&]() -> uint16_t {
                uint16_t value = readUint16LE(metadata + metadataPos);
                metadataPos += 2;
                return value;
            };

            uint16_t fieldMetadata = 0;

            switch (binlogType) {
                case MYSQL_TYPE_BOOL:
                case MYSQL_TYPE_TINY:
                    columnDefs.emplace_back(column_type::INTEGER, 1);
                    break;
                case MYSQL_TYPE_SHORT:
                    columnDefs.emplace_back(column_type::INTEGER, 2);
                    break;
                case MYSQL_TYPE_INT24:
                    columnDefs.emplace_back(column_type::INTEGER, 3);
                    break;
                case MYSQL_TYPE_LONG:
                    columnDefs.emplace_back(column_type::INTEGER, 4);
                    break;
                case MYSQL_TYPE_LONGLONG:
                    columnDefs.emplace_back(column_type::INTEGER, 8);
                    break;
                case MYSQL_TYPE_YEAR:
                    columnDefs.emplace_back(column_type::INTEGER, 1);
                    break;
                case MYSQL_TYPE_FLOAT: {
                    if (!requireMetadata(1)) {
                        return nullptr;
                    }
                    fieldMetadata = metadata[metadataPos];
                    metadataPos += 1;
                    columnDefs.emplace_back(column_type::FLOAT, fieldMetadata == 8 ? 8 : 4);
                }
                    break;
                case MYSQL_TYPE_DOUBLE: {
                    if (!requireMetadata(1)) {
                        return nullptr;
                    }
                    fieldMetadata = metadata[metadataPos];
                    metadataPos += 1;
                    columnDefs.emplace_back(column_type::FLOAT, fieldMetadata == 4 ? 4 : 8);
                }
                    break;
                case MYSQL_TYPE_NEWDECIMAL: {
                    if (!requireMetadata(2)) {
                        return nullptr;
                    }
                    fieldMetadata = readMetadataBE2();
                    columnDefs.emplace_back(column_type::DECIMAL, fieldMetadata);
                }
                    break;
                case MYSQL_TYPE_DECIMAL:
                    columnDefs.emplace_back(column_type::STRING, -1);
                    break;
                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_VAR_STRING: {
                    if (!requireMetadata(2)) {
                        return nullptr;
                    }
                    fieldMetadata = readMetadataLE2();
                    uint16_t maxLen = fieldMetadata;
                    int lenBytes = (maxLen <= UINT8_MAX) ? 1 : 2;
                    columnDefs.emplace_back(column_type::STRING, -lenBytes);
                }
                    break;
                case MYSQL_TYPE_STRING: {
                    if (!requireMetadata(2)) {
                        return nullptr;
                    }
                    fieldMetadata = readMetadataBE2();
                    uint8_t byte0 = static_cast<uint8_t>(fieldMetadata >> 8);
                    uint8_t byte1 = static_cast<uint8_t>(fieldMetadata & 0xFF);

                    if (byte0 == MYSQL_TYPE_ENUM || byte0 == MYSQL_TYPE_SET) {
                        columnDefs.emplace_back(column_type::INTEGER, byte1 == 0 ? 1 : byte1);
                    } else {
                        uint16_t len = static_cast<uint16_t>((((byte0 & 0x30) ^ 0x30) << 4) | byte1);
                        columnDefs.emplace_back(column_type::STRING, static_cast<int>(len));
                    }
                }
                    break;
                case MYSQL_TYPE_BIT: {
                    if (!requireMetadata(2)) {
                        return nullptr;
                    }
                    fieldMetadata = static_cast<uint16_t>(metadata[metadataPos]) |
                                    (static_cast<uint16_t>(metadata[metadataPos + 1]) << 8);
                    uint8_t bits = static_cast<uint8_t>(fieldMetadata & 0xFF);
                    uint8_t bytes = static_cast<uint8_t>((fieldMetadata >> 8) & 0xFF);
                    uint16_t totalBits = static_cast<uint16_t>(bytes) * 8 + bits;
                    int lengthBytes = (totalBits + 7) / 8;
                    columnDefs.emplace_back(column_type::STRING, lengthBytes);
                }
                    break;
                case MYSQL_TYPE_TINY_BLOB:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_GEOMETRY:
                case MYSQL_TYPE_JSON: {
                    if (!requireMetadata(1)) {
                        return nullptr;
                    }
                    fieldMetadata = metadata[metadataPos];
                    metadataPos += 1;
                    uint8_t lengthBytes = static_cast<uint8_t>(fieldMetadata);
                    if (lengthBytes == 0 || lengthBytes > 4) {
                        _logger->warn("invalid blob length bytes: {}", lengthBytes);
                        return nullptr;
                    }
                    columnDefs.emplace_back(column_type::STRING, -static_cast<int>(lengthBytes));
                }
                    break;
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_TIME:
                    columnDefs.emplace_back(column_type::DATETIME, 3);
                    break;
                case MYSQL_TYPE_DATETIME:
                    columnDefs.emplace_back(column_type::DATETIME, 8);
                    break;
                case MYSQL_TYPE_TIMESTAMP:
                    columnDefs.emplace_back(column_type::DATETIME, 4);
                    break;
                case MYSQL_TYPE_TIME2: {
                    if (!requireMetadata(1)) {
                        return nullptr;
                    }
                    fieldMetadata = metadata[metadataPos];
                    metadataPos += 1;
                    uint8_t fsp = static_cast<uint8_t>(fieldMetadata);
                    columnDefs.emplace_back(column_type::DATETIME, 3 + ((fsp + 1) / 2));
                }
                    break;
                case MYSQL_TYPE_DATETIME2: {
                    if (!requireMetadata(1)) {
                        return nullptr;
                    }
                    fieldMetadata = metadata[metadataPos];
                    metadataPos += 1;
                    uint8_t fsp = static_cast<uint8_t>(fieldMetadata);
                    columnDefs.emplace_back(column_type::DATETIME, 5 + ((fsp + 1) / 2));
                }
                    break;
                case MYSQL_TYPE_TIMESTAMP2: {
                    if (!requireMetadata(1)) {
                        return nullptr;
                    }
                    fieldMetadata = metadata[metadataPos];
                    metadataPos += 1;
                    uint8_t fsp = static_cast<uint8_t>(fieldMetadata);
                    columnDefs.emplace_back(column_type::DATETIME, 4 + ((fsp + 1) / 2));
                }
                    break;
                case MYSQL_TYPE_ENUM:
                case MYSQL_TYPE_SET: {
                    if (!requireMetadata(2)) {
                        return nullptr;
                    }
                    fieldMetadata = readMetadataBE2();
                    uint8_t packLen = static_cast<uint8_t>(fieldMetadata & 0xFF);
                    columnDefs.emplace_back(column_type::INTEGER, packLen == 0 ? 1 : packLen);
                }
                    break;
#ifdef MYSQL_TYPE_VECTOR
                case MYSQL_TYPE_VECTOR:
#endif
                case MYSQL_TYPE_TYPED_ARRAY:
                case MYSQL_TYPE_NULL:
                case MYSQL_TYPE_INVALID:
                default:
                    _logger->warn("unsupported field type {} in table map event", static_cast<int>(binlogType));
                    return nullptr;
            }

            mysqlMetadata.push_back(fieldMetadata);
        }

        if (!signednessBits.empty()) {
            size_t expectedBits = ((numericIndex + 7) / 8) * 8;
            if (signednessBits.size() < numericIndex) {
                _logger->warn("signedness metadata shorter than numeric columns (numeric {}, bits {})",
                              numericIndex, signednessBits.size());
            } else if (signednessBits.size() != expectedBits) {
                _logger->warn("signedness metadata length mismatch (numeric {}, bits {}, expected {})",
                              numericIndex, signednessBits.size(), expectedBits);
            }
        }

        auto timestamp = event.header()->when.tv_sec;
        return std::make_shared<TableMapEvent>(
            event.get_table_id(),
            event.get_db_name(),
            event.get_table_name(),
            columnDefs,
            columnNames,
            unsignedFlags,
            mysqlTypes,
            mysqlMetadata,
            timestamp
        );
    }

    std::shared_ptr<base::DBEvent> MySQLBinaryLogReaderV2::decodeRowsQueryEvent(
        const std::vector<unsigned char> &buffer,
        bool fromPayload
    ) {
        if (_fde == nullptr) {
            return nullptr;
        }

        size_t eventSize = buffer.size();
        size_t checksumLen = (!fromPayload && _checksumAlg == mysql::binlog::event::BINLOG_CHECKSUM_ALG_CRC32)
            ? kBinlogChecksumLen
            : 0;
        if (eventSize < checksumLen) {
            return nullptr;
        }

        auto headerLen = _fde->common_header_len;
        auto postHeaderLen = _fde->post_header_len[mysql::binlog::event::ROWS_QUERY_LOG_EVENT - 1];

        size_t offset = headerLen + postHeaderLen + 1;
        if (offset > eventSize - checksumLen) {
            return nullptr;
        }

        size_t queryLen = eventSize - checksumLen - offset;
        std::string query(reinterpret_cast<const char *>(buffer.data() + offset), queryLen);
        return std::make_shared<RowQueryEvent>(query, eventTimestamp(buffer));
    }

    std::shared_ptr<base::DBEvent> MySQLBinaryLogReaderV2::decodeRowsEvent(
        const std::vector<unsigned char> &buffer,
        mysql::binlog::event::Log_event_type eventType,
        bool fromPayload
    ) {
        if (_fde == nullptr) {
            return nullptr;
        }

        size_t eventSize = buffer.size();
        size_t checksumLen = (!fromPayload && _checksumAlg == mysql::binlog::event::BINLOG_CHECKSUM_ALG_CRC32)
            ? kBinlogChecksumLen
            : 0;
        if (eventSize < checksumLen + kLogEventMinimalHeaderLen) {
            return nullptr;
        }

        const unsigned char *begin = buffer.data();
        const unsigned char *end = buffer.data() + eventSize - checksumLen;

        size_t headerLen = _fde->common_header_len;
        size_t postHeaderLen = _fde->post_header_len[eventType - 1];
        if (headerLen + postHeaderLen > static_cast<size_t>(end - begin)) {
            _logger->warn("rows event has invalid header length");
            return nullptr;
        }

        const unsigned char *ptr = begin + headerLen;
        uint64_t tableId = readUint48LE(ptr);
        ptr += 6;
        uint16_t flags = readUint16LE(ptr);
        ptr += 2;

        if (postHeaderLen == mysql::binlog::event::Binary_log_event::ROWS_HEADER_LEN_V2) {
            if (ptr + 2 > end) {
                _logger->warn("rows event extra header truncated");
                return nullptr;
            }
            uint16_t extraLen = readUint16LE(ptr);
            ptr += 2;
            if (extraLen < 2) {
                _logger->warn("rows event extra header length invalid");
                return nullptr;
            }
            size_t extraDataLen = static_cast<size_t>(extraLen - 2);
            if (ptr + extraDataLen > end) {
                _logger->warn("rows event extra header exceeds event size");
                return nullptr;
            }
            ptr += extraDataLen;
        } else if (postHeaderLen > mysql::binlog::event::Binary_log_event::ROWS_HEADER_LEN_V1) {
            size_t extra = postHeaderLen - mysql::binlog::event::Binary_log_event::ROWS_HEADER_LEN_V1;
            if (ptr + extra > end) {
                _logger->warn("rows event post header exceeds event size");
                return nullptr;
            }
            ptr += extra;
        }

        uint64_t width = 0;
        if (!readNetFieldLength(ptr, end, width)) {
            _logger->warn("failed to read rows event width");
            return nullptr;
        }

        auto bitmapSize = static_cast<size_t>((width + 7) / 8);
        if (ptr + bitmapSize > end) {
            _logger->warn("rows event columns bitmap truncated");
            return nullptr;
        }
        std::vector<uint8_t> columnsBefore(ptr, ptr + bitmapSize);
        ptr += bitmapSize;

        std::vector<uint8_t> columnsAfter;
        if (eventType == mysql::binlog::event::OBSOLETE_UPDATE_ROWS_EVENT_V1 ||
            eventType == mysql::binlog::event::UPDATE_ROWS_EVENT) {
            if (ptr + bitmapSize > end) {
                _logger->warn("rows event after-image bitmap truncated");
                return nullptr;
            }
            columnsAfter.assign(ptr, ptr + bitmapSize);
            ptr += bitmapSize;
        } else {
            columnsAfter = columnsBefore;
        }

        size_t rowDataSize = static_cast<size_t>(end - ptr);
        if (rowDataSize == 0) {
            _logger->warn("rows event has no row data");
            return nullptr;
        }

        auto rowData = std::shared_ptr<uint8_t>(new uint8_t[rowDataSize], std::default_delete<uint8_t[]>());
        std::memcpy(rowData.get(), ptr, rowDataSize);

        RowEvent::Type type;
        switch (eventType) {
            case mysql::binlog::event::OBSOLETE_WRITE_ROWS_EVENT_V1:
            case mysql::binlog::event::WRITE_ROWS_EVENT:
                type = RowEvent::INSERT;
                break;
            case mysql::binlog::event::OBSOLETE_DELETE_ROWS_EVENT_V1:
            case mysql::binlog::event::DELETE_ROWS_EVENT:
                type = RowEvent::DELETE;
                break;
            case mysql::binlog::event::OBSOLETE_UPDATE_ROWS_EVENT_V1:
            case mysql::binlog::event::UPDATE_ROWS_EVENT:
                type = RowEvent::UPDATE;
                break;
            default:
                return nullptr;
        }

        return std::make_shared<RowEvent>(
            type,
            tableId,
            static_cast<int>(width),
            std::move(columnsBefore),
            std::move(columnsAfter),
            rowData,
            static_cast<int>(rowDataSize),
            eventTimestamp(buffer),
            flags
        );
    }

    bool MySQLBinaryLogReaderV2::handleTransactionPayloadEvent(const std::vector<unsigned char> &buffer) {
        ensureDefaultFde();

        if (_checksumAlg == mysql::binlog::event::BINLOG_CHECKSUM_ALG_CRC32 &&
            mysql::binlog::event::Log_event_footer::event_checksum_test(
                const_cast<unsigned char *>(buffer.data()),
                buffer.size(),
                _checksumAlg)) {
            _logger->warn("transaction payload event checksum mismatch");
            return false;
        }

        mysql::binlog::event::Transaction_payload_event event(
            reinterpret_cast<const char *>(buffer.data()),
            _fde.get()
        );
        if (!event.header()->get_is_valid()) {
            _logger->warn("invalid transaction payload event");
            return false;
        }

        using BufferIStream = mysql::binlog::event::compression::Payload_event_buffer_istream;
        BufferIStream istream(event);
        BufferIStream::Buffer_ptr_t eventBuffer;

        while (istream >> eventBuffer) {
            if (!eventBuffer) {
                continue;
            }
            std::vector<unsigned char> payload(eventBuffer->data(), eventBuffer->data() + eventBuffer->size());
            _payloadEventQueue.emplace_back(std::move(payload));
        }

        auto status = istream.get_status();
        if (istream.has_error() &&
            status != mysql::binlog::event::compression::Decompress_status::end) {
            _logger->warn("payload decompression error: {}", istream.get_error_str());
            return false;
        }

        return true;
    }
}
