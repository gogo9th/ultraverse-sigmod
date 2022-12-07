//
// Created by cheesekun on 8/19/22.
//

#ifndef ULTRAVERSE_STATE_TRANSACTION_HPP
#define ULTRAVERSE_STATE_TRANSACTION_HPP

#include <memory>
#include <unordered_map>

#include "mariadb/state/StateHash.hpp"

#include "Query.hpp"

namespace ultraverse::state::v2 {
    
    /**
     * ultraverse 자체적으로 사용하는 global transaction id
     */
    using gid_t = uint64_t;
    
    class StateLogReader;
    
    struct TransactionHeader {
        uint64_t timestamp;
        
        gid_t gid;
        int xid;
        bool isSuccessful;
        
        uint8_t flags;
        
        uint64_t nextPos;
    } __attribute__ ((packed));
    
    class Transaction {
    public:
        static const uint8_t FLAG_HAS_DEPENDENCY  = 0b00000001;
        static const uint8_t FLAG_CONTAINS_DDL    = 0b00000010;
        /**
         * indicates the hash is unreliable since row data has omitted (or corrupted)
         */
        static const uint8_t FLAG_UNRELIABLE_HASH = 0b00000100;
        static const uint8_t FLAG_FORCE_EXECUTE   = 0b10000000;
        
        explicit Transaction();
        
        gid_t gid() const;
        void setGid(gid_t gid);
        
        uint64_t xid() const;
        void setXid(uint64_t xid);
        
        uint64_t timestamp() const;
        void setTimestamp(uint64_t timestamp);
        
        uint8_t flags();
        void setFlags(uint8_t flags);
    
        std::unordered_set<std::string> &readSet();
        std::unordered_set<std::string> &writeSet();
        
        void updateRWSet();
        
        TransactionHeader header();
        
        std::vector<std::shared_ptr<Query>> &queries();
    
        
        /**
         * appends query object to transaction.
         * @param query
         */
        Transaction &operator<<(std::shared_ptr<Query> &query);
        
        Transaction &operator+=(TransactionHeader &header);
        
        template <typename Archive>
        void serialize(Archive &archive);
    private:
        friend class StateLogReader;
        
        uint64_t _timestamp;
        
        gid_t _gid;
        uint64_t _xid;
        bool _isSuccessful;
    
        uint8_t _flags;
        
        uint64_t _nextPos;
        
        std::vector<gid_t> _dependencies;
        
        // Pair<TABLE_NAME, HASH>
        std::unordered_map<std::string, StateHash> _beforeHash;
        std::unordered_map<std::string, StateHash> _afterHash;
    
        // binlog reference
        std::string _referenceFile;
        uint64_t _referencePos;
        
        std::vector<std::shared_ptr<Query>> _queries;
    
        std::unordered_set<std::string> _readSet;
        std::unordered_set<std::string> _writeSet;
    };
}

#include "Transaction.cereal.cpp"

#endif //ULTRAVERSE_STATE_TRANSACTION_HPP
