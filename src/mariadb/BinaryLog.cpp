//
// Created by cheesekun on 8/8/22.
//

#include <cstring>

#include <stdexcept>
#include <sstream>
#include <memory>

#include <fmt/core.h>

#include "utils/log.hpp"

#include "BinaryLog.hpp"

namespace ultraverse::mariadb {
    BinaryLog::BinaryLog(DBHandle &handle):
        _handle(handle),
        _rpl(mariadb_rpl_init(_handle.handle().get()))
    {
        _rpl->use_checksum = 1;
    }
    
    void BinaryLog::setFileName(std::string fileName) {
        _rpl->filename = new char[255];
        std::strncpy(_rpl->filename, fileName.c_str(), fileName.length());
        _rpl->filename_length = fileName.length();
    }
    
    void BinaryLog::setStartPosition(int startPosition) {
        if (startPosition < 4) {
            // TODO: LOG_WARN("see https://dev.mysql.com/doc/c-api/8.0/en/c-api-binary-log-data-structures.html");
        }
        
        _rpl->start_position = (unsigned long) startPosition;
    }
    
    void BinaryLog::open() {
        auto retval = mariadb_rpl_open(_rpl);
        if (retval != 0) {
            throw std::runtime_error(
                fmt::format("could not open binary log: mariadb_rpl_open returned {}.", retval)
            );
        }
    }
    
    void BinaryLog::close() {
        mariadb_rpl_close(_rpl);
    }
    
    bool BinaryLog::next() {
        _rpl->use_checksum = 1;
        if (_event != nullptr) {
            mariadb_free_rpl_event(_event);
        }
        _event = mariadb_rpl_fetch(_rpl, nullptr);
        return _event != nullptr && _event->event_type != UNKNOWN_EVENT;
    }
    
    std::shared_ptr<base::DBEvent> BinaryLog::currentEvent() const {
        switch (_event->event_type) {
            case QUERY_EVENT:
                return std::make_shared<QueryEvent>(_event);
            case XID_EVENT:
                return std::make_shared<TransactionIDEvent>(_event);
            default:
                warning(fmt::format("unsupported event type {}", _event->event_type).c_str());
                return nullptr;
        }
    }
    
    MARIADB_RPL_EVENT *BinaryLog::currentRawEvent() const {
        return _event;
    }
}