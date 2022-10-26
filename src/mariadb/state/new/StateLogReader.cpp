//
// Created by cheesekun on 8/21/22.
//

#include <cereal/archives/binary.hpp>

#include "StateLogReader.hpp"

namespace ultraverse::state::v2 {
    StateLogReader::StateLogReader(const std::string &logPath, const std::string &logName):
        _logPath(logPath),
        _logName(logName)
    {
    
    }
    
    StateLogReader::~StateLogReader() {
    
    }
    
    void StateLogReader::open() {
        std::string path = _logPath + "/" + _logName + ".ultstatelog";
        _stream = std::ifstream(path, std::ios::in | std::ios::binary);
    }
    
    void StateLogReader::close() {
        _stream.close();
    }
    
    bool StateLogReader::next() {
        auto header = std::make_shared<TransactionHeader>();
        _stream.read((char *) header.get(), sizeof(TransactionHeader));
    
        if (!_stream.good()) {
            return false;
        }
        
        _currentHeader = header;
        
        auto transaction = std::make_shared<Transaction>();
        cereal::BinaryInputArchive archive(_stream);
        archive(*transaction);
        _currentBody = transaction;
        
        return true;
    }
    
    std::shared_ptr<TransactionHeader> StateLogReader::txnHeader() {
        return _currentHeader;
    }
    
    std::shared_ptr<Transaction> StateLogReader::txnBody() {
        return _currentBody;
    }
}