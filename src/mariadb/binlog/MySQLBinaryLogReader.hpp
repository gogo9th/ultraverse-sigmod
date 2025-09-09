//
// Created by cheesekun on 8/12/22.
//

#ifndef ULTRAVERSE_MYSQL_BINARYLOGPARSER_HPP
#define ULTRAVERSE_MYSQL_BINARYLOGPARSER_HPP

#include <cstdint>

#include <iostream>
#include <fstream>
#include <string>
#include <memory>

#include "base/DBEvent.hpp"
#include "utils/log.hpp"

#include "BinaryLogEvents.hpp"

#include "BinaryLogReader.hpp"

namespace ultraverse::mariadb {
    class MySQLBinaryLogReader: public BinaryLogReaderBase {
    public:
        MySQLBinaryLogReader(const std::string &filename);
    
        void open() override;
        void close() override;
    
        bool seek(int64_t position) override;
        bool next() override;
    
        int pos() override;
    
        std::shared_ptr<base::DBEvent> currentEvent() override;

    private:
        bool isMagicValid();
        
        std::shared_ptr<internal::EventHeader> readHeader();
        
        uint64_t readLenEncInt();
        
        void
        readFormatDescriptionEvent(std::shared_ptr<internal::EventHeader> header);
        
        std::shared_ptr<base::QueryEventBase>
        readQueryEvent(std::shared_ptr<internal::EventHeader> header);
        
        std::shared_ptr<base::TransactionIDEventBase>
        readXIDEvent(std::shared_ptr<internal::EventHeader> header);
        
        std::shared_ptr<TableMapEvent>
        readTableMapEvent(std::shared_ptr<internal::EventHeader> header);
        
        std::shared_ptr<RowQueryEvent>
        readRowAnnotationEvent(std::shared_ptr<internal::EventHeader> header);
        
        std::shared_ptr<RowEvent>
        readRowEvent(std::shared_ptr<internal::EventHeader> header, RowEvent::Type eventType, bool isV2);
        
        LoggerPtr _logger;
        std::string _filename;
        
        std::ifstream _stream;
        int _pos;
        
        bool _hasChecksum;
        
        std::shared_ptr<base::DBEvent> _currentEvent;
    };
    

}

#endif //ULTRAVERSE_MARIADB_BINARYLOGPARSER_HPP