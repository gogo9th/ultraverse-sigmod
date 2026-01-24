//
// Created by cheesekun on 8/15/22.
//

#include <cassert>
#include <algorithm>
#include <sstream>

#include <openssl/evp.h>

#include "ultraverse_state.pb.h"

#include "StateHash.hpp"


namespace ultraverse::state {
    std::vector<StateHash::BigNumPtr> StateHash::generateModulo(int count) {
        std::vector<BigNumPtr> moduloList;
        moduloList.reserve(count);
        
        for (int i = 0; i < count; i++) {
            while (true) {
                BigNumPtr modulo(BN_new(), BN_free);
                BN_generate_prime_ex(modulo.get(), STATE_HASH_PRIME_BITS, 1, nullptr, nullptr, nullptr);
                
                auto iter = std::find_if(moduloList.begin(), moduloList.end(), [&modulo](auto &value) {
                    return BN_cmp(modulo.get(), value.get()) == 0;
                });
                
                if (iter == moduloList.end()) {
                    moduloList.push_back(modulo);
                    break;
                }
            }
        }
        
        return std::move(moduloList);
    }
    
    std::vector<StateHash::BigNumPtr> StateHash::allocateHashList(int count) {
        std::vector<BigNumPtr> hashList;
        hashList.reserve(count);
        
        for (int i = 0; i < count; i++) {
            BigNumPtr hash(BN_new(), BN_free);
            
            auto *rawPtr = hash.get();
            BN_dec2bn(&rawPtr, "1");
            
            hashList.push_back(hash);
        }
        
        return hashList;
    }
    
    inline StateHash::BigNumPtr StateHash::copyBigNumPtr(const StateHash::BigNumPtr &source) {
        BigNumPtr destination(BN_new(), BN_free);
        BN_copy(destination.get(), source.get());
        return destination;
    }
    
    std::vector<StateHash::BigNumPtr> StateHash::copyHashList(const std::vector<BigNumPtr> &source) {
        std::vector<BigNumPtr> destination;
        /*
        destination.reserve(source.size());
        std::transform(source.begin(), source.end(), std::back_inserter(destination), [](auto &sourceVal) {
            return StateHash::copyBigNumPtr(sourceVal);
        });
         */

        return destination;
    }
    
    bool StateHash::compareHashList(const std::vector<BigNumPtr> &a, const std::vector<BigNumPtr> &b) {
        if (a.size() != b.size()) {
            return false;
        }
        
        for (int i = 0; i < a.size(); i++) {
            if (BN_cmp(a[i].get(), b[i].get()) != 0) {
                return false;
            }
        }
        
        return true;
    }
    
    void StateHash::hexdump() {
        int index = 0;
        for (auto &hash: _hashList) {
            auto hexstr = BN_bn2hex(hash.get());
            std::printf("StateHash::hexdump(%d): %s\n", index++, hexstr);
            OPENSSL_free(hexstr);
        }
    }
    
    std::string StateHash::stringify() const {
        std::stringstream sstream;
        
        for (const auto &hash: _hashList) {
            auto hexstr = BN_bn2hex(hash.get());
            
            sstream << hexstr;
            sstream << " ";
            
            OPENSSL_free(hexstr);
        }
        
        return sstream.str();
    }
    
    StateHash::HashValue StateHash::calculateHash(StateHash::Record &record) {
        HashValue hashValue;
        
        const EVP_MD *md5 = EVP_get_digestbyname("md5");
        EVP_MD_CTX *md5Ctx = EVP_MD_CTX_new();
        EVP_DigestInit_ex2(md5Ctx, md5, nullptr);
        EVP_DigestUpdate(md5Ctx, record.c_str(), record.size());
        EVP_DigestFinal_ex(md5Ctx, hashValue.data(), nullptr);
        EVP_MD_CTX_free(md5Ctx);
       
        return hashValue;
    }
    
    StateHash::BigNumPtr StateHash::prime(StateHash::HashValue digest, const StateHash::BigNumPtr &modulo) {
        const EVP_MD *md5 = EVP_get_digestbyname("md5");
        std::shared_ptr<EVP_MD_CTX> md5Ctx;
        BigNumPtr bn;
        
        while (true) {
            bn = BigNumPtr(BN_bin2bn(digest.data(), digest.size(), nullptr), BN_free);
            
            if (BN_cmp(bn.get(), modulo.get()) != 0) {
                break;
            }
            
            md5Ctx = std::shared_ptr<EVP_MD_CTX>(EVP_MD_CTX_new(), EVP_MD_CTX_free);
            EVP_DigestInit_ex2(md5Ctx.get(), md5, nullptr);
            EVP_DigestUpdate(md5Ctx.get(), digest.data(), digest.size());
            EVP_DigestFinal_ex(md5Ctx.get(), digest.data(), nullptr);
        }
        
        return bn;
    }
    
    StateHash::StateHash()
    {
    }
    
    StateHash::StateHash(std::vector<BigNumPtr> moduloList, std::vector<BigNumPtr> hashList):
        _moduloList(std::move(moduloList)),
        _hashList(std::move(hashList))
    {
        assert(_moduloList.size() == _hashList.size());
    }
    
    StateHash::StateHash(const StateHash &other):
        _moduloList(copyHashList(other._moduloList)),
        _hashList(copyHashList(other._hashList))
    {
        // assert(*this == other);
        assert(_moduloList.size() == _hashList.size());
    }
    
    void StateHash::init() {
        _moduloList = generateModulo(DEFAULT_MODULO_COUNT);
        _hashList = allocateHashList(DEFAULT_MODULO_COUNT);
    }
    
    bool StateHash::isInitialized() const {
        return _moduloList.size() != 0;
    }
    
    void StateHash::compute(StateHash::Record &record, StateHash::EventType type) {
        assert(_moduloList.size() == _hashList.size());
        
        std::shared_ptr<BN_CTX> bnCtx(BN_CTX_new(), BN_CTX_free);
        
        auto digest = calculateHash(record);
        
        size_t idx = 0;
        for (auto &modulo: _moduloList) {
            BigNumPtr bnPrime = prime(digest, modulo);
            BigNumPtr r(BN_new(), BN_free);
            
            if (type == DELETE) {
                BigNumPtr temp(BN_new(), BN_free);
                BN_mod_inverse(temp.get(), bnPrime.get(), modulo.get(), bnCtx.get());
                bnPrime = temp;
            }
            
            BN_mod_mul(r.get(), _hashList[idx].get(), bnPrime.get(), modulo.get(), bnCtx.get());
            BN_copy(_hashList[idx].get(), r.get());
            
            idx++;
        }
    }
    
    StateHash &StateHash::operator+=(StateHash::Record record) {
        compute(record, INSERT);
        
        return *this;
    }
    
    StateHash &StateHash::operator-=(StateHash::Record record) {
        compute(record, DELETE);
    
        return *this;
    }
    
    bool StateHash::operator==(const StateHash &other) const {
        return compareHashList(this->_hashList, other._hashList) &&
               compareHashList(this->_moduloList, other._moduloList);
    }

    void StateHash::toProtobuf(ultraverse::state::v2::proto::StateHash *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        for (const auto &modulo : _moduloList) {
            if (!modulo) {
                out->add_modulo("");
                continue;
            }
            const int size = BN_num_bytes(modulo.get());
            std::string buffer(static_cast<size_t>(size), '\0');
            if (size > 0) {
                BN_bn2bin(modulo.get(), reinterpret_cast<unsigned char *>(buffer.data()));
            }
            out->add_modulo(buffer);
        }

        for (const auto &hash : _hashList) {
            if (!hash) {
                out->add_hash("");
                continue;
            }
            const int size = BN_num_bytes(hash.get());
            std::string buffer(static_cast<size_t>(size), '\0');
            if (size > 0) {
                BN_bn2bin(hash.get(), reinterpret_cast<unsigned char *>(buffer.data()));
            }
            out->add_hash(buffer);
        }
    }

    void StateHash::fromProtobuf(const ultraverse::state::v2::proto::StateHash &msg) {
        _moduloList.clear();
        _hashList.clear();

        _moduloList.reserve(static_cast<size_t>(msg.modulo_size()));
        for (const auto &payload : msg.modulo()) {
            BigNumPtr bn(BN_new(), BN_free);
            if (!payload.empty()) {
                BN_bin2bn(reinterpret_cast<const unsigned char *>(payload.data()),
                          static_cast<int>(payload.size()),
                          bn.get());
            }
            _moduloList.push_back(std::move(bn));
        }

        _hashList.reserve(static_cast<size_t>(msg.hash_size()));
        for (const auto &payload : msg.hash()) {
            BigNumPtr bn(BN_new(), BN_free);
            if (!payload.empty()) {
                BN_bin2bn(reinterpret_cast<const unsigned char *>(payload.data()),
                          static_cast<int>(payload.size()),
                          bn.get());
            }
            _hashList.push_back(std::move(bn));
        }
    }
}
