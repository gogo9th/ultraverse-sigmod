//
// Created by cheesekun on 8/21/22.
//

#include <cereal/archives/binary.hpp>

#include "StateLogWriter.hpp"

namespace ultraverse::state::v2 {
    StateLogWriter::StateLogWriter(const std::string &logPath, const std::string &logName):
        _logPath(logPath),
        _logName(logName)
    {
    }
    
    StateLogWriter::~StateLogWriter() {
    
    }
    
    void StateLogWriter::open(std::ios_base::openmode openMode) {
        std::string fileName = _logPath + "/" + _logName + ".ultstatelog";
        _stream = std::ofstream(fileName, openMode);
    }
    
    void StateLogWriter::close() {
        _stream.flush();
        _stream.close();
    }

    bool StateLogWriter::seek(int64_t position) {
        _stream.seekp(position);

        return _stream.good();
    }

    int64_t StateLogWriter::pos() {
        return _stream.tellp();
    }
    
    void StateLogWriter::operator<<(Transaction &transaction) {
        std::scoped_lock<std::mutex> _scopedLock(_mutex);
        auto header = transaction.header();
        std::stringstream tmpStream;
        cereal::BinaryOutputArchive archive(tmpStream);
        archive(transaction);
        std::string transactionString = tmpStream.str();

        auto nextPos = sizeof(TransactionHeader) + transactionString.size() + _stream.tellp();
        header.nextPos = nextPos;

        _stream.write((char *)&header, sizeof(TransactionHeader));
        _stream.write(transactionString.c_str(), transactionString.size());
        _stream.flush();
    }
    
    void StateLogWriter::operator<<(RowCluster &rowCluster) {
        writeRowCluster(rowCluster);
    }
    
    void StateLogWriter::operator<<(ColumnDependencyGraph &graph) {
        writeColumnDependencyGraph(graph);
    }
    
    void StateLogWriter::operator<<(TableDependencyGraph &graph) {
        writeTableDependencyGraph(graph);
    }
    
    void StateLogWriter::writeRowCluster(RowCluster &rowCluster) {
        std::string fileName = _logPath + "/" + _logName + ".ultcluster";
        std::ofstream stream(fileName, std::ios::binary);
        
        cereal::BinaryOutputArchive archive(stream);
        archive(rowCluster);
        
        stream.flush();
        stream.close();
    }
    
    void StateLogWriter::writeColumnDependencyGraph(ColumnDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ultcolumns";
        std::ofstream stream(fileName, std::ios::binary);
        
        cereal::BinaryOutputArchive archive(stream);
        archive(graph);
        
        stream.flush();
        stream.close();
    }
    
    void StateLogWriter::writeTableDependencyGraph(TableDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ulttables";
        std::ofstream stream(fileName, std::ios::binary);
        
        cereal::BinaryOutputArchive archive(stream);
        archive(graph);
        
        stream.flush();
        stream.close();
    }
}