//
// Created by cheesekun on 8/19/22.
//

#include "utils/StringUtil.hpp"
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
    
    void Transaction::updateRWSet() {
        // not implemented
    }
    
    TransactionHeader Transaction::header() {
        TransactionHeader header;
        
        header.timestamp = _timestamp;
        header.gid = _gid;
        header.xid = _xid;
        header.isSuccessful = _isSuccessful;
        header.flags = _flags;
        header.nextPos = _nextPos;
        
        return std::move(header);
    }
    
    std::vector<std::shared_ptr<Query>> &Transaction::queries() {
        return _queries;
    }
    
    CombinedIterator<StateItem> Transaction::readSet_begin() {
        std::vector<std::reference_wrapper<std::vector<StateItem>>> containers;
        
        std::transform(
            std::begin(_queries), std::end(_queries),
            std::back_inserter(containers),
            [](std::shared_ptr<Query> &query) { return std::reference_wrapper<std::vector<StateItem>>(query->readSet()); }
        );
        
        return CombinedIterator<StateItem>(containers);
    }
    
    CombinedIterator<StateItem> Transaction::readSet_end() {
        return readSet_begin().end();
    }
    
    CombinedIterator<StateItem> Transaction::writeSet_begin() {
        std::vector<std::reference_wrapper<std::vector<StateItem>>> containers;
        
        std::transform(
            std::begin(_queries), std::end(_queries),
            std::back_inserter(containers),
            [](std::shared_ptr<Query> &query) { return std::reference_wrapper<std::vector<StateItem>>(query->writeSet()); }
        );
        
        return CombinedIterator<StateItem>(containers);
    }
    
    CombinedIterator<StateItem> Transaction::writeSet_end() {
        return writeSet_begin().end();
    }
    
    bool Transaction::isRelatedToDatabase(const std::string database) {
        return std::any_of(_queries.begin(), _queries.end(), [&database](auto &query) {
            return query->database() == database;
        });
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