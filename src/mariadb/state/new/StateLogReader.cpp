//
// Created by cheesekun on 8/21/22.
//

#include "GIDIndexReader.hpp"
#include "StateLogReader.hpp"

#include <stdexcept>

#include "ultraverse_state.pb.h"

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
    
    void StateLogReader::reset() {
        _stream.close();
        std::string path = _logPath + "/" + _logName + ".ultstatelog";
        _stream = std::ifstream(path, std::ios::in | std::ios::binary);
        _currentHeader = nullptr;
        _currentBody = nullptr;
    }
    
    uint64_t StateLogReader::pos() {
        return _stream.tellg();
    }
    
    void StateLogReader::seek(uint64_t pos) {
        _stream.seekg(pos);
    
        _currentHeader = nullptr;
        _currentBody = nullptr;
    }
    
    bool StateLogReader::nextHeader() {
        auto header = std::make_shared<TransactionHeader>();
        _stream.read((char *) header.get(), sizeof(TransactionHeader));
    
        if (!_stream.good()) {
            _currentHeader = nullptr;
            return false;
        }
    
        _currentHeader = header;
        
        return true;
    }
    
    bool StateLogReader::nextTransaction() {
        if (_currentHeader == nullptr) {
            _currentBody = nullptr;
            return false;
        }

        const auto startPos = static_cast<std::streamoff>(_stream.tellg());
        if (!_stream.good()) {
            _currentBody = nullptr;
            return false;
        }

        const auto endPos = static_cast<std::streamoff>(_currentHeader->nextPos);
        if (endPos <= startPos) {
            _currentBody = nullptr;
            return false;
        }

        const auto size = static_cast<size_t>(endPos - startPos);
        std::string buffer(size, '\0');
        _stream.read(buffer.data(), static_cast<std::streamsize>(size));
        if (!_stream.good()) {
            _currentBody = nullptr;
            return false;
        }

        ultraverse::state::v2::proto::Transaction protoTxn;
        if (!protoTxn.ParseFromString(buffer)) {
            _currentBody = nullptr;
            return false;
        }

        auto transaction = std::make_shared<Transaction>();
        transaction->fromProtobuf(protoTxn);
        _currentBody = transaction;

        return true;
    }
    
    void StateLogReader::skipTransaction() {
        if (_currentHeader != nullptr) {
            _stream.seekg(_currentHeader->nextPos);
        }
    }
    
    bool StateLogReader::next() {
        return nextHeader() && nextTransaction();
    }
    
    std::shared_ptr<TransactionHeader> StateLogReader::txnHeader() {
        return _currentHeader;
    }
    
    std::shared_ptr<Transaction> StateLogReader::txnBody() {
        return _currentBody;
    }

    bool StateLogReader::seekGid(gid_t gid) {
        if (_gidIndexReader == nullptr) {
            _gidIndexReader = std::make_unique<GIDIndexReader>(_logPath, _logName);
        }

        auto offset = _gidIndexReader->offsetOf(gid);
        seek(offset);
        return true;
    }
    
    void StateLogReader::operator>>(RowCluster &rowCluster) {
        loadRowCluster(rowCluster);
    }
    
    void StateLogReader::operator>>(ColumnDependencyGraph &graph) {
        loadColumnDependencyGraph(graph);
    }
    
    void StateLogReader::operator>>(TableDependencyGraph &graph) {
        loadTableDependencyGraph(graph);
    }
    
    void StateLogReader::loadRowCluster(RowCluster &rowCluster) {
        std::string fileName = _logPath + "/" + _logName + ".ultcluster";
        std::ifstream stream(fileName, std::ios::binary);

        ultraverse::state::v2::proto::RowCluster protoCluster;
        if (!protoCluster.ParseFromIstream(&stream)) {
            throw std::runtime_error("failed to read row cluster protobuf");
        }
        rowCluster.fromProtobuf(protoCluster);

        stream.close();
    }
    
    void StateLogReader::loadColumnDependencyGraph(ColumnDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ultcolumns";
        std::ifstream stream(fileName, std::ios::binary);

        ultraverse::state::v2::proto::ColumnDependencyGraph protoGraph;
        if (!protoGraph.ParseFromIstream(&stream)) {
            throw std::runtime_error("failed to read column dependency graph protobuf");
        }
        graph.fromProtobuf(protoGraph);

        stream.close();
    }
    
    void StateLogReader::loadTableDependencyGraph(TableDependencyGraph &graph) {
        std::string fileName = _logPath + "/" + _logName + ".ulttables";
        std::ifstream stream(fileName, std::ios::binary);

        ultraverse::state::v2::proto::TableDependencyGraph protoGraph;
        if (!protoGraph.ParseFromIstream(&stream)) {
            throw std::runtime_error("failed to read table dependency graph protobuf");
        }
        graph.fromProtobuf(protoGraph);

        stream.close();
    }
}
