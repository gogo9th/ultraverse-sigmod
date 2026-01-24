//
// Created by cheesekun on 8/21/22.
//

#include "StateLogWriter.hpp"

#include <stdexcept>

#include "ultraverse_state.pb.h"

namespace ultraverse::state::v2 {
    StateLogWriter::StateLogWriter(const std::string &logPath, const std::string &logName):
        _logPath(logPath),
        _logName(logName)
    {
    }
    
    StateLogWriter::~StateLogWriter() {
    
    }
    
    void StateLogWriter::open(std::ios_base::openmode openMode) {
        std::scoped_lock<std::mutex> _scopedLock(_mutex);
        std::string fileName = _logPath + "/" + _logName + ".ultstatelog";
        _stream = std::ofstream(fileName, openMode);
    }
    
    void StateLogWriter::close() {
        std::scoped_lock<std::mutex> _scopedLock(_mutex);
        _stream.flush();
        _stream.close();
    }

    bool StateLogWriter::seek(int64_t position) {
        std::scoped_lock<std::mutex> _scopedLock(_mutex);
        _stream.seekp(position);

        return _stream.good();
    }

    int64_t StateLogWriter::pos() {
        std::scoped_lock<std::mutex> _scopedLock(_mutex);
        return _stream.tellp();
    }
    
    void StateLogWriter::operator<<(Transaction &transaction) {
        std::scoped_lock<std::mutex> _scopedLock(_mutex);
        auto header = transaction.header();
        ultraverse::state::v2::proto::Transaction protoTxn;
        transaction.toProtobuf(&protoTxn);
        std::string transactionString;
        if (!protoTxn.SerializeToString(&transactionString)) {
            throw std::runtime_error("failed to serialize transaction protobuf");
        }

        const auto currentPos = static_cast<std::streamoff>(_stream.tellp());
        auto nextPos = static_cast<uint64_t>(currentPos) + sizeof(TransactionHeader) + transactionString.size();
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
        ultraverse::state::v2::proto::RowCluster protoCluster;
        rowCluster.toProtobuf(&protoCluster);
        if (!protoCluster.SerializeToOstream(&stream)) {
            throw std::runtime_error("failed to serialize row cluster protobuf");
        }

        stream.flush();
        stream.close();
    }
    
    void StateLogWriter::writeColumnDependencyGraph(ColumnDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ultcolumns";
        std::ofstream stream(fileName, std::ios::binary);
        ultraverse::state::v2::proto::ColumnDependencyGraph protoGraph;
        graph.toProtobuf(&protoGraph);
        if (!protoGraph.SerializeToOstream(&stream)) {
            throw std::runtime_error("failed to serialize column dependency graph protobuf");
        }

        stream.flush();
        stream.close();
    }
    
    void StateLogWriter::writeTableDependencyGraph(TableDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ulttables";
        std::ofstream stream(fileName, std::ios::binary);
        ultraverse::state::v2::proto::TableDependencyGraph protoGraph;
        graph.toProtobuf(&protoGraph);
        if (!protoGraph.SerializeToOstream(&stream)) {
            throw std::runtime_error("failed to serialize table dependency graph protobuf");
        }

        stream.flush();
        stream.close();
    }
}
