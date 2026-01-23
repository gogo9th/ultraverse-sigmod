//
// Created by cheesekun on 2/1/23.
//

#ifndef ULTRAVERSE_BINARYLOGSEQUENTIALREADER_HPP
#define ULTRAVERSE_BINARYLOGSEQUENTIALREADER_HPP

#include <atomic>

#include "BinaryLogReader.hpp"

namespace ultraverse::mariadb {
    class BinaryLogSequentialReader {
    public:
        explicit BinaryLogSequentialReader(const std::string &basePath, const std::string &indexFile);
        
        bool seek(int index, int64_t position);
        bool next();
        int pos();
        int logFileListSize();
        
        std::shared_ptr<base::DBEvent> currentEvent();
        
        bool isPollDisabled() const;
        void setPollDisabled(bool isPollDisabled);
        
        void terminate();

    private:
        std::unique_ptr<BinaryLogReaderBase> openBinaryLog(const std::string &logFile);
        void updateIndex();
        void openLog(const std::string &logFile);
        bool pollNext();
    
        LoggerPtr _logger;
    
        std::string _basePath;
        std::string _indexFile;
        std::vector<std::string> _logFileList;
        // TOOD: currentFile or currentIndex;
        int _currentIndex;
    
        std::atomic<bool> terminateSignal{false};
        bool _isPollDisabled;
    
        std::unique_ptr<BinaryLogReaderBase> _binaryLogReader;
    };
}

#endif //ULTRAVERSE_BINARYLOGSEQUENTIALREADER_HPP
