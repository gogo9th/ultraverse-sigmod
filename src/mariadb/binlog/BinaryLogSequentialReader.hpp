//
// Created by cheesekun on 2/1/23.
//

#ifndef ULTRAVERSE_BINARYLOGSEQUENTIALREADER_HPP
#define ULTRAVERSE_BINARYLOGSEQUENTIALREADER_HPP

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
        
    protected:
        virtual std::unique_ptr<BinaryLogReaderBase> openBinaryLog(const std::string &logFile) = 0;
    
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
        bool _isPollDisabled;
    
    private:
        std::unique_ptr<BinaryLogReaderBase> _binaryLogReader;
    };
    
    class MariaDBBinaryLogSequentialReader: public BinaryLogSequentialReader {
    public:
        MariaDBBinaryLogSequentialReader(const std::string &basePath, const std::string &indexFile);

    protected:
        std::unique_ptr<BinaryLogReaderBase> openBinaryLog(const std::string &logFile) override;
    };
    
    class MySQLBinaryLogSequentialReader: public BinaryLogSequentialReader {
    public:
        MySQLBinaryLogSequentialReader(const std::string &basePath, const std::string &indexFile);

    protected:
        std::unique_ptr<BinaryLogReaderBase> openBinaryLog(const std::string &logFile) override;
    };
}

#endif //ULTRAVERSE_BINARYLOGSEQUENTIALREADER_HPP
