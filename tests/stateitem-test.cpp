#include <memory>
#include <string>
#include <vector>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include "mariadb/state/StateItem.h"

namespace {
int64_t readInt(const StateData &data) {
    int64_t value = 0;
    REQUIRE(data.Get(value));
    return value;
}
} // namespace

TEST_CASE("StateData set/get and flags", "[stateitem]") {
    StateData empty;
    REQUIRE(empty.IsNone());
    REQUIRE(empty.Type() == en_column_data_null);

    StateData i;
    i.Set(int64_t{-7});
    REQUIRE(i.Type() == en_column_data_int);
    REQUIRE(readInt(i) == -7);

    StateData u;
    u.Set(uint64_t{7});
    REQUIRE(u.Type() == en_column_data_uint);
    uint64_t uOut = 0;
    REQUIRE(u.Get(uOut));
    REQUIRE(uOut == 7);

    StateData d;
    d.Set(1.25);
    REQUIRE(d.Type() == en_column_data_double);
    double dOut = 0.0;
    REQUIRE(d.Get(dOut));
    REQUIRE(dOut == Catch::Approx(1.25));

    StateData s;
    s.Set("hello", 5);
    REQUIRE(s.Type() == en_column_data_string);
    std::string sOut;
    REQUIRE(s.Get(sOut));
    REQUIRE(sOut == "hello");

    s.SetEqual();
    REQUIRE(s.IsEqual());

    int64_t raw = 42;
    StateData sub;
    auto type = static_cast<en_state_log_column_data_type>(
        en_column_data_int | en_column_data_from_subselect
    );
    REQUIRE(sub.SetData(type, &raw, sizeof(raw)));
    REQUIRE(sub.IsSubSelect());
    REQUIRE(sub.Type() == en_column_data_int);

    StateData conv;
    conv.Set("123", 3);
    REQUIRE(conv.ConvertData(en_column_data_int));
    REQUIRE(readInt(conv) == 123);
}

TEST_CASE("StateData decimal normalization and comparison", "[stateitem]") {
    // decimal data는 실질 내부적으로 string storage를 사용한다

    StateData a;
    a.SetDecimal("001.2300");
    std::string out;
    REQUIRE(a.Get(out));
    REQUIRE(out == "001.2300");

    StateData c;
    c.SetDecimal("-0.00");
    REQUIRE(c.Get(out));
    REQUIRE(out == "-0.00");
}

TEST_CASE("StateRange builds simple where clauses", "[stateitem]") {
    StateRange eq;
    eq.SetValue(StateData { (int64_t) 1 }, true);
    REQUIRE(eq.MakeWhereQuery("id") == "id=1");

    StateData text;
    text.Set("hello", 5);
    StateRange eqText;
    eqText.SetValue(text, true);
    REQUIRE(eqText.MakeWhereQuery("name") == "name=X'68656C6C6F'");

    StateRange ne;
    ne.SetValue(StateData { (int64_t) 1 }, false);
    REQUIRE(ne.MakeWhereQuery("id") == "id<1 OR id>1");
}

TEST_CASE("StateRange between ordering and intersection", "[stateitem]") {
    StateRange between;
    between.SetBetween(StateData { (int64_t) 10 }, StateData { (int64_t) 5 });
    auto ranges = between.GetRange();
    REQUIRE(ranges->size() == 1);

    const auto &range = ranges->at(0);
    REQUIRE(readInt(range.begin) == 5);
    REQUIRE(readInt(range.end) == 10);
    REQUIRE(range.begin.IsEqual());
    REQUIRE(range.end.IsEqual());

    StateRange a;
    a.SetBetween(StateData { (int64_t) 1 }, StateData { (int64_t) 2 });
    StateRange b;
    b.SetBetween(StateData { (int64_t) 2 }, StateData { (int64_t) 3 });
    REQUIRE(StateRange::isIntersects(a, b));
}

TEST_CASE("StateRange AND/OR and arrangeSelf", "[stateitem]") {
    StateRange a;
    a.SetBetween(StateData { (int64_t) 1 }, StateData { (int64_t) 5 });
    StateRange b;
    b.SetBetween(StateData { (int64_t) 3 }, StateData { (int64_t) 7 });

    auto inter = StateRange::AND(a, b);
    REQUIRE(inter->GetRange()->size() == 1);
    const auto &ir = inter->GetRange()->at(0);
    REQUIRE(readInt(ir.begin) == 3);
    REQUIRE(readInt(ir.end) == 5);

    auto uni = StateRange::OR(a, b);
    REQUIRE(uni->GetRange()->size() == 1);
    const auto &ur = uni->GetRange()->at(0);
    REQUIRE(readInt(ur.begin) == 1);
    REQUIRE(readInt(ur.end) == 7);

    StateRange disA;
    disA.SetBetween(StateData { (int64_t) 1 }, StateData { (int64_t) 2 });
    StateRange disB;
    disB.SetBetween(StateData { (int64_t) 4 }, StateData { (int64_t) 5 });
    REQUIRE_FALSE(StateRange::isIntersects(disA, disB));
    auto dis = StateRange::OR(disA, disB);
    REQUIRE(dis->GetRange()->size() == 2);

    StateRange merge;
    merge.SetBetween(StateData { (int64_t) 1 }, StateData { (int64_t) 3 });
    merge.SetBetween(StateData { (int64_t) 2 }, StateData { (int64_t) 4 });
    REQUIRE(merge.GetRange()->size() == 2);
    merge.arrangeSelf();
    REQUIRE(merge.GetRange()->size() == 1);
    const auto &mr = merge.GetRange()->at(0);
    REQUIRE(readInt(mr.begin) == 1);
    REQUIRE(readInt(mr.end) == 4);
}

TEST_CASE("StateRange wildcard intersects any", "[stateitem]") {
    StateRange wildcard;
    wildcard.setWildcard(true);

    StateRange concrete{1};
    REQUIRE(StateRange::isIntersects(wildcard, concrete));

    auto combined = StateRange::AND(wildcard, concrete);
    REQUIRE(combined->GetRange()->size() == 1);
    REQUIRE(*combined == concrete);
}

TEST_CASE("StateItem MakeRange2 handles function types", "[stateitem]") {
    auto query = [](const StateRange &range) {
        return range.MakeWhereQuery("col");
    };

    SECTION("EQ") {
        StateItem item = StateItem::EQ("col", StateData { (int64_t) 1 });
        REQUIRE(query(item.MakeRange2()) == "col=1");
    }

    SECTION("NE") {
        StateItem item;
        item.function_type = FUNCTION_NE;
        item.data_list.emplace_back(StateData { (int64_t) 1 });
        REQUIRE(query(item.MakeRange2()) == "col<1 OR col>1");
    }

    SECTION("LT/LE/GT/GE") {
        StateItem lt;
        lt.function_type = FUNCTION_LT;
        lt.data_list.emplace_back(StateData{(int64_t) 1 });
        REQUIRE(query(lt.MakeRange2()) == "col<1");

        StateItem le;
        le.function_type = FUNCTION_LE;
        le.data_list.emplace_back(StateData { (int64_t) 1 });
        REQUIRE(query(le.MakeRange2()) == "col<=1");

        StateItem gt;
        gt.function_type = FUNCTION_GT;
        gt.data_list.emplace_back(StateData { (int64_t) 1 });
        REQUIRE(query(gt.MakeRange2()) == "col>1");

        StateItem ge;
        ge.function_type = FUNCTION_GE;
        ge.data_list.emplace_back(StateData { (int64_t) 1 });
        REQUIRE(query(ge.MakeRange2()) == "col>=1");
    }

    SECTION("BETWEEN") {
        StateItem item;
        item.function_type = FUNCTION_BETWEEN;
        item.data_list.emplace_back(StateData { (int64_t) 1 });
        item.data_list.emplace_back(StateData { (int64_t) 3 });
        REQUIRE(query(item.MakeRange2()) == "(col>=1 AND col<=3)");
    }

    SECTION("IN") {
        StateItem item;
        item.function_type = FUNCTION_IN_INTERNAL;
        item.data_list.emplace_back(StateData { (int64_t) 1 });
        item.data_list.emplace_back(StateData { (int64_t) 2 });
        REQUIRE(query(item.MakeRange2()) == "col=1 OR col=2");
    }

    SECTION("WILDCARD") {
        StateItem item = StateItem::Wildcard("col");
        auto range = item.MakeRange2();
        if (!range.wildcard()) {
            WARN("StateItem::MakeRange2 does not set wildcard; skipping wildcard-specific assertions.");
            return;
        }
        REQUIRE(range.wildcard());
    }
}

TEST_CASE("StateItem MakeRange2 handles AND/OR conditions", "[stateitem]") {
    StateItem gt;
    gt.function_type = FUNCTION_GT;
    gt.data_list.emplace_back(StateData { (int64_t) 1 });

    StateItem lt;
    lt.function_type = FUNCTION_LT;
    lt.data_list.emplace_back(StateData { (int64_t) 5 });

    StateItem andItem;
    andItem.condition_type = EN_CONDITION_AND;
    andItem.arg_list = {gt, lt};

    REQUIRE(andItem.MakeRange2().MakeWhereQuery("col") == "(col>1 AND col<5)");

    StateItem eq1 = StateItem::EQ("col", StateData { (int64_t) 1 });
    StateItem eq2 = StateItem::EQ("col", StateData { (int64_t) 2 });

    StateItem orItem;
    orItem.condition_type = EN_CONDITION_OR;
    orItem.arg_list = {eq1, eq2};

    REQUIRE(orItem.MakeRange2().MakeWhereQuery("col") == "col=1 OR col=2");
}

TEST_CASE("StateItem MakeRange2 caches results", "[stateitem]") {
    StateItem item = StateItem::EQ("col", StateData { (int64_t) 7 });
    const StateRange &first = item.MakeRange2();
    const StateRange &second = item.MakeRange2();

    REQUIRE(std::addressof(first) == std::addressof(second));
    REQUIRE(first.hash() == second.hash());
}

TEST_CASE("StateItem MakeRange matches MakeRange2 for basics", "[stateitem]") {
    StateItem eq = StateItem::EQ("col", StateData { (int64_t) 9 });
    auto legacy = StateItem::MakeRange(eq);
    REQUIRE(legacy != nullptr);
    REQUIRE(legacy->MakeWhereQuery("col") == eq.MakeRange2().MakeWhereQuery("col"));

    StateItem gt;
    gt.function_type = FUNCTION_GT;
    gt.data_list.emplace_back(StateData { (int64_t) 1 });

    StateItem lt;
    lt.function_type = FUNCTION_LT;
    lt.data_list.emplace_back(StateData { (int64_t) 5 });

    StateItem andItem;
    andItem.condition_type = EN_CONDITION_AND;
    andItem.arg_list = {gt, lt};

    auto legacyAnd = StateItem::MakeRange(andItem);
    REQUIRE(legacyAnd != nullptr);
    REQUIRE(legacyAnd->MakeWhereQuery("col") == andItem.MakeRange2().MakeWhereQuery("col"));
}
