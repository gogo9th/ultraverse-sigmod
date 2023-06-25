//
// Created by cheesekun on 8/19/22.
//

#ifndef ULTRAVERSE_STATE_TRANSACTION_HPP
#define ULTRAVERSE_STATE_TRANSACTION_HPP

#include <memory>
#include <unordered_map>

#include "CombinedIterator.hpp"

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
        static const uint8_t FLAG_UNRELIABLE_HASH   = 0b00000100;
        static const uint8_t FLAG_IS_PROCEDURE_CALL = 0b01000000;
        static const uint8_t FLAG_FORCE_EXECUTE     = 0b10000000;

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
        
        CombinedIterator<StateItem> whereSet_begin();
        
        /**
         * @note 성능 이슈가 발생할 수 있으므로 너무 자주 호출하지 마십시오. (아래 예시 참조)
         * @example
         *      // 나쁜 사용예
         *      auto it = transaction.whereSet_begin();
         *      while (it != transaction.whereSet_end()) {
         *          //       ^^^^^^^^^^^^^^^^^^^^^^^^^^ 매번 호출되면서 transaction.query를 순회하며 새 vector를 내부적으로 만듬
         *      }
         *
         *      // 좋은 사용예
         *      auto it = transaction.whereSet_begin();
         *      auto itEnd = transaction.whereSet_end();
         *
         *      while (it != itEnd) {
         *          // ...
         *      }
         */
        CombinedIterator<StateItem> whereSet_end();
        
        CombinedIterator<StateItem> itemSet_begin();
        /**
         * @note 성능 이슈가 발생할 수 있으므로 너무 자주 호출하지 마십시오. (아래 예시 참조)
         * @example
         *      // 나쁜 사용예
         *      auto it = transaction.itemSet_begin();
         *      while (it != transaction.itemSet_end()) {
         *          //       ^^^^^^^^^^^^^^^^^^^^^^^^^ 매번 호출되면서 transaction.query를 순회하며 새 vector를 내부적으로 만듬
         *      }
         *
         *      // 좋은 사용예
         *      auto it = transaction.itemSet_begin();
         *      auto itEnd = transaction.itemSet_end();
         *
         *      while (it != itEnd) {
         *          // ...
         *      }
         */
        CombinedIterator<StateItem> itemSet_end();
        
        
        /**
         * 주어진 DB와 관련된 쿼리가 하나라도 있는지 확인합니다.
         */
        bool isRelatedToDatabase(const std::string database);
        
        
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
    
        std::vector<std::shared_ptr<Query>> _queries;
    
        std::unordered_set<std::string> _readSet;
        std::unordered_set<std::string> _writeSet;
    };
}

#include "Transaction.cereal.cpp"

#endif //ULTRAVERSE_STATE_TRANSACTION_HPP
