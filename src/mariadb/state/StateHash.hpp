//
// Created by cheesekun on 8/15/22.
//

#ifndef ULTRAVERSE_STATE_STATEHASH_HPP
#define ULTRAVERSE_STATE_STATEHASH_HPP

#include <memory>
#include <array>
#include <vector>
#include <string>

#include <openssl/bn.h>
#include <openssl/sha.h>
#include <openssl/md5.h>


namespace ultraverse::state {
    class StateHash {
    public:
        enum EventType {
            INSERT,
            DELETE,
            TRUNCATE,
            RENAME
        };
        
        static constexpr int DEFAULT_MODULO_COUNT = 2;
    
        /** Decreasing this value speeds up the algorithm */
        static constexpr int STATE_HASH_LENGTH = 16;
        /** Decreasing this value speeds up the algorithm */
        static constexpr int STATE_HASH_PRIME_CHECK_COUNT = 100;
    
        static constexpr int STATE_HASH_PRIME_BITS = STATE_HASH_LENGTH * 8;
    
        using BigNum = BIGNUM;
        using BigNumPtr = std::shared_ptr<BigNum>;
        
        using HashValue = std::array<unsigned char, STATE_HASH_LENGTH>;
        using HashValueList = std::vector<HashValue>;
        using HashStringList = std::vector<std::string>;
        
        /** 변경된 row의 전체 필드값 */
        using Record = std::string;
        
   
        /**
         * generates list of prime number (calls modulo?)
         */
        static std::vector<BigNumPtr> generateModulo(int count);
        
        explicit StateHash();
        explicit StateHash(std::vector<BigNumPtr> moduloList, std::vector<BigNumPtr> hashList);
        
        StateHash(StateHash &other);
        
        void compute(Record &record, EventType type);
        void hexdump();
        
        StateHash& operator+=(Record record);
        StateHash& operator-=(Record record);
        
        bool operator==(StateHash &other);
        
        template <typename Archive>
        void save(Archive &archive) const;
    
        template <typename Archive>
        void load(Archive &archive);
        
    private:
        static std::vector<BigNumPtr> allocateHashList(int count);
        static inline BigNumPtr copyBigNumPtr(BigNumPtr source);
        static std::vector<BigNumPtr> copyHashList(std::vector<BigNumPtr> &source);
        static bool compareHashList(const std::vector<BigNumPtr> &a, const std::vector<BigNumPtr> &b);
        
        static HashValue calculateHash(Record &record);
        
        BigNumPtr prime(HashValue digest, const BigNumPtr modulo);
        
        std::vector<BigNumPtr> _moduloList;
        std::vector<BigNumPtr> _hashList;
    };
}


#include "StateHash.cereal.cpp"

#endif //ULTRAVERSE_STATEHASH_HPP
