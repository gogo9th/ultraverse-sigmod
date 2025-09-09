//
// Created by cheesekun on 8/21/22.
//

#include <cassert>

#include <openssl/bn.h>

#include <cereal/types/vector.hpp>
#include <cereal/types/memory.hpp>
#include <cereal/types/unordered_map.hpp>

#include "StateHash.hpp"

namespace ultraverse::state {
    
    template <typename Archive>
    void StateHash::save(Archive &archive) const {
        if (_hashList.empty() || _moduloList.empty()) {
            int32_t size = 0;
            archive(size);
            return;
        }
        
        assert(_hashList.size() == _moduloList.size());
    
        int32_t size = BN_num_bytes(_moduloList[0].get());
        int32_t listSize = _hashList.size();
        auto dstPtr = std::make_unique<std::vector<uint8_t>>(
            size * (_hashList.size() * 2)
        );
        dstPtr->reserve(
            size * (_hashList.size() * 2)
        );
    
        auto i = 0;

        for (auto &hash: _moduloList) {
            BN_bn2binpad(hash.get(), dstPtr->data() + (size * i++), size);
        }
        for (auto &hash: _hashList) {
            BN_bn2binpad(hash.get(), dstPtr->data() + (size * i++), size);
        }
    
        archive(size);
        archive(listSize);
        archive(dstPtr);
    }
    
    template <typename Archive>
    void StateHash::load(Archive &archive) {
        int32_t bnSize = 0;
        int32_t listSize = 0;
        std::unique_ptr<std::vector<uint8_t>> srcPtr;
        
        archive(bnSize);
        
        if (bnSize == 0) {
            return;
        }
        
        archive(listSize);
        archive(srcPtr);

        assert(bnSize != 0);
        assert(listSize != 0);

        /*
        _moduloList.reserve(listSize);
        _hashList.reserve(listSize);

        for (auto i = 0; i < listSize * 2; i++) {
            StateHash::BigNumPtr bignum(BN_new(), BN_free);
            BN_bin2bn(srcPtr->data() + (bnSize * i), bnSize, bignum.get());

            if (i < listSize) {
                _moduloList.push_back(bignum);
            } else {
                _hashList.push_back(bignum);
            }
        }
        */
    }
}
