//
// Created by cheesekun on 8/12/22.
//

#include <stdexcept>

#include <fmt/core.h>

#include "../DBEvent.hpp"

#include "BinaryLogReader.hpp"

namespace ultraverse::mariadb {
    BinaryLogReader::BinaryLogReader(const std::string &filename):
        _logger(createLogger("BinaryLogReader")),
        _filename(filename),
        
        _pos(0),
        _hasChecksum(false)
    {
    
    }
    
    void BinaryLogReader::open() {
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
    
    void BinaryLogReader::close() {
        _logger->info("closing binary log: {}", _filename);
        _stream.close();
    }
    
    void BinaryLogReader::seek(int64_t position) {
        _logger->trace("seeking offset: {}", position);
        _stream.seekg(position);
        _pos = position;
    }
    
    bool BinaryLogReader::next() {
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
        } else {
            _logger->trace("unsupported event type: {}", (int) eventType);
        }
        
        seek(header->log_pos);
        
        return true;
    }
    
    std::shared_ptr<base::DBEvent> BinaryLogReader::currentEvent() {
        return _currentEvent;
    }
    
    std::shared_ptr<internal::EventHeader> BinaryLogReader::readHeader() {
        auto header = std::make_shared<internal::EventHeader>();
        _stream.read((char *) header.get(), sizeof(internal::EventHeader));
    
        _logger->trace("header read: {}", header->event_type);
        
        return header;
    }
    
    void
    BinaryLogReader::readFormatDescriptionEvent(std::shared_ptr<internal::EventHeader> header) {
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
            _hasChecksum = true;
            _logger->warn("the log file includes CRC32 checksum, but this will be ignored");
        }
    }
    
    std::shared_ptr<base::QueryEventBase>
    BinaryLogReader::readQueryEvent(std::shared_ptr<internal::EventHeader> header) {
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
                (statusVarsLength + schemaLength + 1)
            );
        
        auto queryCStr = std::shared_ptr<uint8_t>(new uint8_t[queryLength]);
        _stream.read((char *) queryCStr.get(), queryLength);
    
        std::string schema((char *) schemaCStr.get(), (int) schemaLength);
        std::string query((char *) queryCStr.get(), (int) queryLength);
        
        auto timestamp = header->timestamp;
        
        return std::make_shared<QueryEvent>(schema, query, timestamp);
    }
    
    std::shared_ptr<base::TransactionIDEventBase>
    BinaryLogReader::readXIDEvent(std::shared_ptr<internal::EventHeader> header) {
        auto xidEvent = std::make_shared<internal::XIDEvent>();
        _stream.read((char *) xidEvent.get(), sizeof(internal::XIDEvent));
    
        auto timestamp = header->timestamp;
        auto xid = xidEvent->xid;
        
        return std::make_shared<TransactionIDEvent>(xid, timestamp);
    }
    
}