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
    
    class Transaction {
    public:
        static const uint8_t FLAG_IS_IGNORABLE = 0b00000001;
        static const uint8_t FLAG_CONTAINS_DDL = 0b00000010;
        
        Transaction();
        
        void setGid(int gid);
        void setXid(int xid);
        void setTimestamp(uint64_t timestamp);
        
        /**
         * loads skipped fields. (eg. _queries)
         */
        void loadMore();
    
        /**
         * appends query object to transaction.
         * @param query
         */
        Transaction &operator<<(std::shared_ptr<Query> query);
        
    private:
        uint64_t timestamp;
        
        gid_t _gid;
        int _xid;
        bool _isSuccessful;
        
        uint64_t _nextPos;
        
        // binlog reference
        std::string _referenceFile;
        uint64_t _referencePos;
        
        // Pair<TABLE_NAME, HASH>
        std::unordered_map<std::string, StateHash> _beforeHash;
        std::unordered_map<std::string, StateHash> _afterHash;
        
        
        bool _isFullyLoaded;
        
        // 당장은 skip하나 추가적으로 로드할 수 있는 필드들
        std::vector<std::shared_ptr<Query>> _queries;
    };
}

#endif //ULTRAVERSE_STATE_TRANSACTION_HPP
