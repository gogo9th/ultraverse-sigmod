//
// Created by cheesekun on 8/12/22.
//

#ifndef ULTRAVERSE_MARIADB_BINARYLOGPARSER_HPP
#define ULTRAVERSE_MARIADB_BINARYLOGPARSER_HPP

#include <cstdint>

#include <iostream>
#include <fstream>
#include <string>
#include <memory>

#include "base/DBEvent.hpp"
#include "utils/log.hpp"

#include "BinaryLogEvents.hpp"

namespace ultraverse::mariadb {
    static const uint8_t BINLOG_MAGIC[4] = {0xfe, 0x62, 0x69, 0x6e};
    
    class BinaryLogReader {
    public:
        BinaryLogReader(const std::string &filename);
        
        void open();
        void close();
        
        void seek(int64_t position);
        bool next();
        
        std::shared_ptr<base::DBEvent> currentEvent();
        
    private:
        bool isMagicValid();
        
        std::shared_ptr<internal::EventHeader> readHeader();
        
        void
        readFormatDescriptionEvent(std::shared_ptr<internal::EventHeader> header);
        
        std::shared_ptr<base::QueryEventBase>
        readQueryEvent(std::shared_ptr<internal::EventHeader> header);
        
        std::shared_ptr<base::TransactionIDEventBase>
        readXIDEvent(std::shared_ptr<internal::EventHeader> header);
        
        LoggerPtr _logger;
        std::string _filename;
        
        std::ifstream _stream;
        int _pos;
        
        bool _hasChecksum;
        
        std::shared_ptr<base::DBEvent> _currentEvent;
    };
}

#endif //ULTRAVERSE_MARIADB_BINARYLOGPARSER_HPP
