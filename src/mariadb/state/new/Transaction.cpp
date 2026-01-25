//
// Created by cheesekun on 8/19/22.
//

#include "utils/StringUtil.hpp"
#include "Transaction.hpp"

#include "ultraverse_state.pb.h"


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

    const std::vector<std::shared_ptr<Query>> &Transaction::queries() const {
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
    
    bool Transaction::isRelatedToDatabase(const std::string database) const {
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

    void Transaction::toProtobuf(ultraverse::state::v2::proto::Transaction *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        out->set_timestamp(_timestamp);
        out->set_gid(_gid);
        out->set_xid(_xid);
        out->set_is_successful(_isSuccessful);
        out->set_flags(_flags);
        out->set_next_pos(_nextPos);

        for (const auto &dep : _dependencies) {
            out->add_dependencies(dep);
        }

        for (const auto &query : _queries) {
            if (!query) {
                continue;
            }
            auto *queryMsg = out->add_queries();
            query->toProtobuf(queryMsg);
        }
    }

    void Transaction::fromProtobuf(const ultraverse::state::v2::proto::Transaction &msg) {
        _timestamp = msg.timestamp();
        _gid = msg.gid();
        _xid = msg.xid();
        _isSuccessful = msg.is_successful();
        _flags = static_cast<uint8_t>(msg.flags());
        _nextPos = msg.next_pos();

        _dependencies.clear();
        _dependencies.reserve(static_cast<size_t>(msg.dependencies_size()));
        for (const auto dep : msg.dependencies()) {
            _dependencies.push_back(dep);
        }

        _queries.clear();
        _queries.reserve(static_cast<size_t>(msg.queries_size()));
        for (const auto &queryMsg : msg.queries()) {
            auto query = std::make_shared<Query>();
            query->fromProtobuf(queryMsg);
            _queries.emplace_back(std::move(query));
        }
    }
}
