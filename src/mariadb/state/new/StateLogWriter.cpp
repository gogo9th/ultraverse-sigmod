//
// Created by cheesekun on 8/21/22.
//

#include <cereal/archives/binary.hpp>

#include "StateLogWriter.hpp"

namespace ultraverse::state::v2 {
    StateLogWriter::StateLogWriter(const std::string &logPath):
        _logPath(logPath)
    {
    }
    
    StateLogWriter::~StateLogWriter() {
    
    }
    
    void StateLogWriter::open() {
        _stream = std::ofstream(_logPath, std::ios::out | std::ios::binary);
    }
    
    void StateLogWriter::close() {
        _stream.close();
    }

    bool StateLogWriter::seek(int64_t position) {
        _stream.seekp(position);

        return _stream.good();
    }

    int64_t StateLogWriter::pos() {
        return _stream.tellp();
    }
    
    StateLogWriter &StateLogWriter::operator<<(Transaction &transaction) {
        auto header = transaction.header();
        
        _stream.write((char *) &header, sizeof(TransactionHeader));
        
        cereal::BinaryOutputArchive archive(_stream);
        archive(transaction);
        
        _stream.flush();
    }
}