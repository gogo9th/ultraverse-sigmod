//
// Created by cheesekun on 8/19/22.
//

#include "Transaction.hpp"

namespace ultraverse::state::v2 {
    Transaction::Transaction():
        _timestamp(0),
        _gid(0),
        _xid(0),
        _isSuccessful(false),
        _flags(0),
        _nextPos(0)
    {
    
    }
    
    gid_t Transaction::gid() const {
        return _gid;
    }
    
    void Transaction::setGid(gid_t gid) {
        _gid = gid;
    }
    
    uint64_t Transaction::xid() const {
        return _xid;
    }
    
    void Transaction::setXid(uint64_t xid) {
        _xid = xid;
    }
    
    uint64_t Transaction::timestamp() const {
        return _timestamp;
    }
    
    void Transaction::setTimestamp(uint64_t timestamp) {
        _timestamp = timestamp;
    }
    
    uint8_t Transaction::flags() {
        return _flags;
    }
    
    void Transaction::setFlags(uint8_t flags) {
        _flags = flags;
    }
    
    Transaction &Transaction::operator<<(std::shared_ptr<Query> &query) {
        _queries.push_back(query);
        return *this;
    }
    
    Transaction &Transaction::operator+=(TransactionHeader &header) {
        _timestamp = header.timestamp;
        _gid = header.gid;
        _xid = header.xid;
        _flags = header.flags;
        _isSuccessful = header.isSuccessful;
        _nextPos = header.nextPos;
        
        return *this;
    }
}