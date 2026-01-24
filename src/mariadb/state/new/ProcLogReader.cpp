//
// Created by cheesekun on 3/16/23.
//

#include "ProcLogReader.hpp"

#include "ultraverse_state.pb.h"

namespace ultraverse::state::v2 {
    ProcLogReader::ProcLogReader() {
    
    }
    
    bool ProcLogReader::open(const std::string &path, const std::string &logName) {
        const std::string fullPath = path + "/" + logName;
        _stream = std::ifstream(fullPath, std::ios::in | std::ios::binary);
        
        _currentHeader = nullptr;
        _current = nullptr;
        
        return _stream.good();
    }
    
    bool ProcLogReader::close() {
        _stream.close();
        _stream = std::ifstream();
        
        return true;
    }
    
    void ProcLogReader::seek(uint64_t pos) {
        _stream.seekg(pos);
        
        _currentHeader = nullptr;
        _current = nullptr;
    }
    
    bool ProcLogReader::nextHeader() {
        auto header = std::make_shared<ProcCallHeader>();
        _stream.read((char *) header.get(), sizeof(ProcCallHeader));
        
        if (!_stream.good()) {
            _currentHeader = nullptr;
            return false;
        }
        
        _currentHeader = header;
        
        return true;
    }
    
    bool ProcLogReader::nextProcCall() {
        if (_currentHeader == nullptr) {
            _current = nullptr;
            return false;
        }

        const auto startPos = static_cast<std::streamoff>(_stream.tellg());
        if (!_stream.good()) {
            _current = nullptr;
            return false;
        }

        const auto endPos = static_cast<std::streamoff>(_currentHeader->nextPos);
        if (endPos <= startPos) {
            _current = nullptr;
            return false;
        }

        const auto size = static_cast<size_t>(endPos - startPos);
        std::string buffer(size, '\0');
        _stream.read(buffer.data(), static_cast<std::streamsize>(size));
        if (!_stream.good()) {
            _current = nullptr;
            return false;
        }

        ultraverse::state::v2::proto::ProcCall protoCall;
        if (!protoCall.ParseFromString(buffer)) {
            _current = nullptr;
            return false;
        }

        auto procCall = std::make_shared<ProcCall>();
        procCall->fromProtobuf(protoCall);
        _current = procCall;

        return true;
    }
    
    bool ProcLogReader::matchForward(uint64_t callId) {
        while (true) {
            if (!nextHeader()) {
                break;
            }
            
            if (_currentHeader->callId == callId) {
                nextProcCall();
                return true;
            }
    
            seek(_currentHeader->nextPos);
        }
        
        return false;
    }

    std::shared_ptr<ProcCallHeader> ProcLogReader::currentHeader() {
        return _currentHeader;
    }
    
    std::shared_ptr<ProcCall> ProcLogReader::current() {
        return _current;
    }
}
