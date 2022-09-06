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
    
    void StateLogWriter::open(std::ios_base::openmode openMode) {
        _stream = std::ofstream(_logPath, openMode);
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
        std::stringstream tmpStream;
        cereal::BinaryOutputArchive archive(tmpStream);
        archive(transaction);
        std::string transactionString = tmpStream.str();

        std::cout << "pos: " << pos() << '\n';

        auto nextPos = sizeof(TransactionHeader) + transactionString.size() + _stream.tellp();
        header.nextPos = nextPos;

        std::cout << "nextPos: " << nextPos << '\n';
        _stream.write((char *)&header, sizeof(TransactionHeader));
        _stream.write(transactionString.c_str(), transactionString.size());
        _stream.flush();
    }
}