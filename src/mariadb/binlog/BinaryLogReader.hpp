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
        
        bool seek(int64_t position);
        bool next();
        
        int pos();
        
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
    
    class BinaryLogSequentialReader {
    public:
        explicit BinaryLogSequentialReader(const std::string &basePath, const std::string &indexFile);
    
        bool seek(int index, int64_t position);
        bool next();
        int pos();
        int logFileListSize();
    
        std::shared_ptr<base::DBEvent> currentEvent();

        void terminate();
        
    private:
        void updateIndex();
        void openLog(const std::string &logFile);
        
        bool pollNext();
        
        LoggerPtr _logger;
        
        std::string _basePath;
        std::string _indexFile;
        std::vector<std::string> _logFileList;
        // TOOD: currentFile or currentIndex;
        int _currentIndex;

        bool terminateSignal = false;
        
        std::unique_ptr<BinaryLogReader> _binaryLogReader;
    };
}

#endif //ULTRAVERSE_MARIADB_BINARYLOGPARSER_HPP