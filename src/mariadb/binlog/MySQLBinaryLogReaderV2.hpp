//
// Created by ultraverse on 1/21/26.
//

#ifndef ULTRAVERSE_MYSQL_BINARYLOGREADER_V2_HPP
#define ULTRAVERSE_MYSQL_BINARYLOGREADER_V2_HPP

#include <cstdint>

#include <deque>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "base/DBEvent.hpp"
#include "mariadb/DBEvent.hpp"
#include "utils/log.hpp"

#include "BinaryLogReader.hpp"

#include "mysql/binlog/event/binlog_event.h"
#include "mysql/binlog/event/control_events.h"
#include "mysql/binlog/event/rows_event.h"
#include "mysql/binlog/event/statement_events.h"
#include "mysql/binlog/event/compression/payload_event_buffer_istream.h"

namespace ultraverse::mariadb {
    class MySQLBinaryLogReaderV2: public BinaryLogReaderBase {
    public:
        explicit MySQLBinaryLogReaderV2(const std::string &filename);

        void open() override;
        void close() override;

        bool seek(int64_t position) override;
        bool next() override;

        int pos() override;

        std::shared_ptr<base::DBEvent> currentEvent() override;

    private:
        bool readNextEventBuffer(std::vector<unsigned char> &buffer);
        std::shared_ptr<base::DBEvent> decodeEventBuffer(const std::vector<unsigned char> &buffer, bool fromPayload);
        std::shared_ptr<TableMapEvent> decodeTableMapEvent(mysql::binlog::event::Table_map_event &event);
        std::shared_ptr<base::DBEvent> decodeRowsQueryEvent(const std::vector<unsigned char> &buffer, bool fromPayload);
        std::shared_ptr<base::DBEvent> decodeRowsEvent(const std::vector<unsigned char> &buffer,
                                                       mysql::binlog::event::Log_event_type eventType,
                                                       bool fromPayload);
        bool handleTransactionPayloadEvent(const std::vector<unsigned char> &buffer);

        void ensureDefaultFde();

        LoggerPtr _logger;
        std::string _filename;

        std::ifstream _stream;
        int _pos;

        std::shared_ptr<base::DBEvent> _currentEvent;

        std::unique_ptr<mysql::binlog::event::Format_description_event> _fde;
        mysql::binlog::event::enum_binlog_checksum_alg _checksumAlg;

        std::deque<std::vector<unsigned char>> _payloadEventQueue;
    };
}

#endif // ULTRAVERSE_MYSQL_BINARYLOGREADER_V2_HPP
