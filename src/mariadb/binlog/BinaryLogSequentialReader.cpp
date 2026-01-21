//
// Created by cheesekun on 2/1/23.
//

#include "BinaryLogSequentialReader.hpp"

#include "MariaDBBinaryLogReader.hpp"
#include "MySQLBinaryLogReaderV2.hpp"

namespace ultraverse::mariadb {
    BinaryLogSequentialReader::BinaryLogSequentialReader(const std::string &basePath, const std::string &indexFile):
        _logger(createLogger("BinaryLogSeqReader")),
    
        _basePath(basePath),
        _indexFile(indexFile),
        
        _currentIndex(0),
        _isPollDisabled(false)
    {
    }
    
    bool BinaryLogSequentialReader::seek(int index, int64_t position) {
        assert(index < _logFileList.size());
        
        openLog(_logFileList[index]);
        _currentIndex = index;
        
        return _binaryLogReader->seek(position);
    }
    
    bool BinaryLogSequentialReader::next() {
        while (!terminateSignal) {
            if (_binaryLogReader == nullptr) {
                return false;
            }
        
            auto result = _binaryLogReader->next();
            if (!result) {
                using namespace std::chrono_literals;
                
                if (pollNext()) {
                    continue;
                } else {
                    if (_isPollDisabled) {
                        return false;
                    }
                }
                std::this_thread::sleep_for(5s);
            } else {
                return true;
            }
        }
    
        return false;
    }
    
    bool BinaryLogSequentialReader::pollNext() {
        updateIndex();
        if (_currentIndex + 1 != _logFileList.size()) {
            seek(_currentIndex + 1, 4);
            return true;
        } else {
            seek(_currentIndex, _binaryLogReader->pos());
            return false;
        }
        
        // ?
        return false;
    }
    
    void BinaryLogSequentialReader::updateIndex() {
        _logFileList.clear();
        std::ifstream stream(_basePath + "/" + _indexFile, std::ios::in);
        
        if (!stream.good()) {
            throw std::runtime_error(
                fmt::format("could not open index file: {}", _indexFile)
            );
        }
        
        std::string line;
        while (std::getline(stream, line)) {
            _logFileList.push_back(line);
        }
    }
    
    void BinaryLogSequentialReader::openLog(const std::string &logFile) {
        if (_binaryLogReader != nullptr) {
            _binaryLogReader->close();
            _binaryLogReader = nullptr;
        }
        
        _binaryLogReader = openBinaryLog(logFile);
        _binaryLogReader->open();
    }
    
    std::shared_ptr<base::DBEvent> BinaryLogSequentialReader::currentEvent() {
        if (_binaryLogReader == nullptr) {
            return nullptr;
        }
        
        return _binaryLogReader->currentEvent();
    }
    
    int BinaryLogSequentialReader::pos() {
        if (_binaryLogReader == nullptr) {
            return -1;
        }
        return _binaryLogReader->pos();
    }
    
    bool BinaryLogSequentialReader::isPollDisabled() const {
        return _isPollDisabled;
    }
    
    void BinaryLogSequentialReader::setPollDisabled(bool isPollDisabled) {
        _isPollDisabled = isPollDisabled;
    }
    
    void BinaryLogSequentialReader::terminate() {
        terminateSignal = true;
    }

    int BinaryLogSequentialReader::logFileListSize() {
        return _logFileList.size();
    }
    
    MariaDBBinaryLogSequentialReader::MariaDBBinaryLogSequentialReader(const std::string &basePath,
                                                                       const std::string &indexFile)
        : BinaryLogSequentialReader(basePath, indexFile)
    {
        updateIndex();
        seek(_currentIndex, 4);
    }
    
    std::unique_ptr<BinaryLogReaderBase> MariaDBBinaryLogSequentialReader::openBinaryLog(const std::string &logFile) {
        return std::move(std::make_unique<MariaDBBinaryLogReader>(_basePath + "/" + logFile));
    }
    
    MySQLBinaryLogSequentialReader::MySQLBinaryLogSequentialReader(const std::string &basePath,
                                                                   const std::string &indexFile)
        : BinaryLogSequentialReader(basePath, indexFile)
    {
        updateIndex();
        seek(_currentIndex, 4);
    }
    
    std::unique_ptr<BinaryLogReaderBase> MySQLBinaryLogSequentialReader::openBinaryLog(const std::string &logFile) {
        return std::move(std::make_unique<MySQLBinaryLogReaderV2>(_basePath + "/" + logFile));
    }
    
    
}
