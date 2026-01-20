//
// StateHash unit tests
//

#include <catch2/catch_test_macros.hpp>
#include <openssl/bn.h>

#include "mariadb/state/StateHash.hpp"

using ultraverse::state::StateHash;

namespace {
const std::vector<StateHash::BigNumPtr> &sharedModulo() {
    static const std::vector<StateHash::BigNumPtr> modulos =
        StateHash::generateModulo(StateHash::DEFAULT_MODULO_COUNT);
    return modulos;
}

std::vector<StateHash::BigNumPtr> cloneList(const std::vector<StateHash::BigNumPtr> &src) {
    std::vector<StateHash::BigNumPtr> dst;
    dst.reserve(src.size());
    for (const auto &bn : src) {
        StateHash::BigNumPtr copy(BN_new(), BN_free);
        BN_copy(copy.get(), bn.get());
        dst.push_back(copy);
    }
    return dst;
}

std::vector<StateHash::BigNumPtr> makeHashList(size_t count, unsigned long value = 1UL) {
    std::vector<StateHash::BigNumPtr> list;
    list.reserve(count);
    for (size_t i = 0; i < count; i++) {
        StateHash::BigNumPtr bn(BN_new(), BN_free);
        BN_set_word(bn.get(), value);
        list.push_back(bn);
    }
    return list;
}

StateHash makeStateHashWithModulo(const std::vector<StateHash::BigNumPtr> &modulo, unsigned long value = 1UL) {
    return StateHash(cloneList(modulo), makeHashList(modulo.size(), value));
}

StateHash makeDefaultStateHash(unsigned long value = 1UL) {
    return makeStateHashWithModulo(sharedModulo(), value);
}
} // namespace

TEST_CASE("StateHash initializes and stringifies", "[statehash]") {
    StateHash hash;
    REQUIRE_FALSE(hash.isInitialized());
    REQUIRE(hash.stringify().empty());

    hash.init();
    REQUIRE(hash.isInitialized());
    REQUIRE_FALSE(hash.stringify().empty());
}

TEST_CASE("StateHash generateModulo returns unique primes", "[statehash]") {
    auto modulos = StateHash::generateModulo(2);
    REQUIRE(modulos.size() == 2);
    REQUIRE(BN_cmp(modulos[0].get(), modulos[1].get()) != 0);
}

TEST_CASE("StateHash compute(INSERT) matches operator+=", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    StateHash::Record record = "user:1|name:alice";
    hashA.compute(record, StateHash::INSERT);
    hashB += record;

    REQUIRE(hashA == hashB);
}

TEST_CASE("StateHash compute(DELETE) matches operator-=", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    StateHash::Record record = "user:1|name:alice";
    hashA.compute(record, StateHash::DELETE);
    hashB -= record;

    REQUIRE(hashA == hashB);
}

TEST_CASE("StateHash insert then delete restores original hash", "[statehash]") {
    auto hash = makeDefaultStateHash();

    StateHash::Record record = "user:1|name:alice";
    auto before = hash.stringify();

    hash += record;
    REQUIRE(hash.stringify() != before);

    hash -= record;
    REQUIRE(hash.stringify() == before);
}

TEST_CASE("StateHash insert commutes across records", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    StateHash::Record recordA = "user:1|name:alice";
    StateHash::Record recordB = "user:2|name:bob";

    hashA += recordA;
    hashA += recordB;

    hashB += recordB;
    hashB += recordA;

    REQUIRE(hashA == hashB);
}

TEST_CASE("StateHash equality checks both modulo and hash", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    REQUIRE(hashA == hashB);

    StateHash::Record record = "user:1|name:alice";
    hashB += record;
    REQUIRE_FALSE(hashA == hashB);

    auto modulos2 = StateHash::generateModulo(2);
    auto hashC = makeStateHashWithModulo(modulos2);
    REQUIRE_FALSE(hashA == hashC);
}

TEST_CASE("StateHash primes use expected bit length", "[statehash]") {
    for (const auto &modulo : sharedModulo()) {
        REQUIRE(BN_num_bits(modulo.get()) == StateHash::STATE_HASH_PRIME_BITS);
    }
}

TEST_CASE("StateHash empty record is deterministic", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    StateHash::Record record;
    hashA += record;
    hashB += record;

    REQUIRE(hashA == hashB);
}

TEST_CASE("StateHash long record is deterministic", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    StateHash::Record record(1 << 16, 'x');
    hashA += record;
    hashB += record;

    REQUIRE(hashA == hashB);
}

TEST_CASE("StateHash insert twice then delete once equals insert once", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    StateHash::Record record = "user:1|name:alice";

    hashA += record;
    hashA += record;
    hashA -= record;

    hashB += record;

    REQUIRE(hashA == hashB);
}

TEST_CASE("StateHash insert A,B then delete A equals insert B", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    StateHash::Record recordA = "user:1|name:alice";
    StateHash::Record recordB = "user:2|name:bob";

    hashA += recordA;
    hashA += recordB;
    hashA -= recordA;

    hashB += recordB;

    REQUIRE(hashA == hashB);
}

TEST_CASE("StateHash insert/delete commute across different records", "[statehash]") {
    auto hashA = makeDefaultStateHash();
    auto hashB = makeDefaultStateHash();

    StateHash::Record recordA = "user:1|name:alice";
    StateHash::Record recordB = "user:2|name:bob";

    hashA += recordA;
    hashA -= recordB;

    hashB -= recordB;
    hashB += recordA;

    REQUIRE(hashA == hashB);
}

TEST_CASE("StateHash hash list affects equality", "[statehash]") {
    auto hashA = makeDefaultStateHash(1UL);
    auto hashB = makeDefaultStateHash(2UL);

    REQUIRE_FALSE(hashA == hashB);
}
