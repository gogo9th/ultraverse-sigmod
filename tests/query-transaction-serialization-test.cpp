#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include "ultraverse_state.pb.h"

#include "mariadb/state/new/Query.hpp"
#include "mariadb/state/new/Transaction.hpp"

namespace {
using ultraverse::state::v2::Query;
using ultraverse::state::v2::Transaction;
using ultraverse::state::v2::TransactionHeader;

StateItem makeItem(const std::string &name, const StateData &data, EN_FUNCTION_TYPE fnType) {
    StateItem item;
    item.condition_type = EN_CONDITION_AND;
    item.function_type = fnType;
    item.name = name;
    if (fnType != FUNCTION_WILDCARD) {
        item.data_list.emplace_back(data);
    }
    return item;
}

void requireStateItemEqual(const StateItem &lhs, const StateItem &rhs) {
    REQUIRE(lhs.condition_type == rhs.condition_type);
    REQUIRE(lhs.function_type == rhs.function_type);
    REQUIRE(lhs.name == rhs.name);
    REQUIRE(lhs.arg_list.size() == rhs.arg_list.size());
    for (size_t i = 0; i < lhs.arg_list.size(); ++i) {
        requireStateItemEqual(lhs.arg_list[i], rhs.arg_list[i]);
    }
    REQUIRE(lhs.data_list.size() == rhs.data_list.size());
    for (size_t i = 0; i < lhs.data_list.size(); ++i) {
        REQUIRE(lhs.data_list[i] == rhs.data_list[i]);
    }
    REQUIRE(lhs.sub_query_list.size() == rhs.sub_query_list.size());
    for (size_t i = 0; i < lhs.sub_query_list.size(); ++i) {
        requireStateItemEqual(lhs.sub_query_list[i], rhs.sub_query_list[i]);
    }
    REQUIRE(lhs._isRangeCacheBuilt == rhs._isRangeCacheBuilt);
    REQUIRE(lhs._rangeCache == rhs._rangeCache);
}

void requireQueryEqual(Query &lhs, Query &rhs) {
    REQUIRE(lhs.type() == rhs.type());
    REQUIRE(lhs.timestamp() == rhs.timestamp());
    REQUIRE(lhs.database() == rhs.database());
    REQUIRE(lhs.statement() == rhs.statement());
    REQUIRE(lhs.flags() == rhs.flags());
    REQUIRE(lhs.affectedRows() == rhs.affectedRows());

    auto &lhsRead = lhs.readSet();
    auto &rhsRead = rhs.readSet();
    REQUIRE(lhsRead.size() == rhsRead.size());
    for (size_t i = 0; i < lhsRead.size(); ++i) {
        requireStateItemEqual(lhsRead[i], rhsRead[i]);
    }

    auto &lhsWrite = lhs.writeSet();
    auto &rhsWrite = rhs.writeSet();
    REQUIRE(lhsWrite.size() == rhsWrite.size());
    for (size_t i = 0; i < lhsWrite.size(); ++i) {
        requireStateItemEqual(lhsWrite[i], rhsWrite[i]);
    }

    auto &lhsVars = lhs.varMap();
    auto &rhsVars = rhs.varMap();
    REQUIRE(lhsVars.size() == rhsVars.size());
    for (size_t i = 0; i < lhsVars.size(); ++i) {
        requireStateItemEqual(lhsVars[i], rhsVars[i]);
    }

    REQUIRE(lhs.readColumns() == rhs.readColumns());
    REQUIRE(lhs.writeColumns() == rhs.writeColumns());

    const auto &lhsCtx = lhs.statementContext();
    const auto &rhsCtx = rhs.statementContext();
    REQUIRE(lhsCtx.hasLastInsertId == rhsCtx.hasLastInsertId);
    REQUIRE(lhsCtx.lastInsertId == rhsCtx.lastInsertId);
    REQUIRE(lhsCtx.hasInsertId == rhsCtx.hasInsertId);
    REQUIRE(lhsCtx.insertId == rhsCtx.insertId);
    REQUIRE(lhsCtx.hasRandSeed == rhsCtx.hasRandSeed);
    REQUIRE(lhsCtx.randSeed1 == rhsCtx.randSeed1);
    REQUIRE(lhsCtx.randSeed2 == rhsCtx.randSeed2);
    REQUIRE(lhsCtx.userVars.size() == rhsCtx.userVars.size());
    for (size_t i = 0; i < lhsCtx.userVars.size(); ++i) {
        const auto &l = lhsCtx.userVars[i];
        const auto &r = rhsCtx.userVars[i];
        REQUIRE(l.name == r.name);
        REQUIRE(l.type == r.type);
        REQUIRE(l.isNull == r.isNull);
        REQUIRE(l.isUnsigned == r.isUnsigned);
        REQUIRE(l.charset == r.charset);
        REQUIRE(l.value == r.value);
    }
}

Query buildQuery(const std::string &db, const std::string &stmt, uint64_t ts, uint32_t rows) {
    Query query;
    query.setType(Query::UPDATE);
    query.setTimestamp(ts);
    query.setDatabase(db);
    query.setStatement(stmt);
    query.setFlags(Query::FLAG_IS_CONTINUOUS);
    query.setAffectedRows(rows);

    query.readSet().push_back(makeItem("users.id", StateData(static_cast<int64_t>(42)), FUNCTION_EQ));
    query.writeSet().push_back(makeItem("users.name", StateData(std::string("alice")), FUNCTION_EQ));
    query.varMap().push_back(makeItem("@v1", StateData(static_cast<int64_t>(7)), FUNCTION_EQ));

    query.readColumns().insert("users.id");
    query.readColumns().insert("users.name");
    query.writeColumns().insert("users.name");

    auto &ctx = query.statementContext();
    ctx.hasLastInsertId = true;
    ctx.lastInsertId = 999;
    ctx.hasInsertId = true;
    ctx.insertId = 111;
    ctx.hasRandSeed = true;
    ctx.randSeed1 = 1234;
    ctx.randSeed2 = 5678;

    Query::UserVar var;
    var.name = "uv1";
    var.type = Query::UserVar::DECIMAL;
    var.isNull = false;
    var.isUnsigned = true;
    var.charset = 33;
    var.value = "1.23";
    ctx.userVars.push_back(var);

    return query;
}

StateItem buildStateItem() {
    StateItem root;
    root.condition_type = EN_CONDITION_OR;
    root.function_type = FUNCTION_IN_INTERNAL;
    root.name = "users.id";
    root.data_list.emplace_back(StateData(static_cast<int64_t>(1)));
    root.data_list.emplace_back(StateData(static_cast<int64_t>(2)));

    StateItem arg;
    arg.condition_type = EN_CONDITION_AND;
    arg.function_type = FUNCTION_EQ;
    arg.name = "users.name";
    arg.data_list.emplace_back(StateData(std::string("alice")));
    root.arg_list.push_back(arg);

    root.sub_query_list.push_back(StateItem::Wildcard("orders.*"));

    return root;
}
} // namespace

TEST_CASE("Query protobuf round-trip preserves fields", "[query][protobuf]") {
    Query original = buildQuery("testdb", "UPDATE users SET name='alice' WHERE id=42", 123456, 3);

    ultraverse::state::v2::proto::Query protoQuery;
    original.toProtobuf(&protoQuery);
    std::string payload;
    REQUIRE(protoQuery.SerializeToString(&payload));

    ultraverse::state::v2::proto::Query restoredProto;
    REQUIRE(restoredProto.ParseFromString(payload));

    Query restored;
    restored.fromProtobuf(restoredProto);

    requireQueryEqual(original, restored);
}

TEST_CASE("Transaction protobuf round-trip preserves header and queries", "[transaction][protobuf]") {
    TransactionHeader header{};
    header.timestamp = 987654;
    header.gid = 42;
    header.xid = 777;
    header.isSuccessful = true;
    header.flags = Transaction::FLAG_HAS_DEPENDENCY | Transaction::FLAG_FORCE_EXECUTE;
    header.nextPos = 12345;

    Transaction txn;
    txn += header;

    auto q1 = std::make_shared<Query>(buildQuery("db1", "UPDATE t SET a=1", 111, 1));
    auto q2 = std::make_shared<Query>(buildQuery("db2", "UPDATE t SET a=2", 222, 2));
    txn << q1;
    txn << q2;

    ultraverse::state::v2::proto::Transaction protoTxn;
    txn.toProtobuf(&protoTxn);
    std::string payload;
    REQUIRE(protoTxn.SerializeToString(&payload));

    ultraverse::state::v2::proto::Transaction restoredProto;
    REQUIRE(restoredProto.ParseFromString(payload));

    Transaction restored;
    restored.fromProtobuf(restoredProto);

    auto restoredHeader = restored.header();
    REQUIRE(restoredHeader.timestamp == header.timestamp);
    REQUIRE(restoredHeader.gid == header.gid);
    REQUIRE(restoredHeader.xid == header.xid);
    REQUIRE(restoredHeader.isSuccessful == header.isSuccessful);
    REQUIRE(restoredHeader.flags == header.flags);
    REQUIRE(restoredHeader.nextPos == header.nextPos);

    auto &restoredQueries = restored.queries();
    REQUIRE(restoredQueries.size() == 2);

    requireQueryEqual(*q1, *restoredQueries[0]);
    requireQueryEqual(*q2, *restoredQueries[1]);
}

TEST_CASE("StateItem protobuf round-trip preserves fields", "[stateitem][protobuf]") {
    StateItem original = buildStateItem();

    ultraverse::state::v2::proto::StateItem protoItem;
    original.toProtobuf(&protoItem);
    std::string payload;
    REQUIRE(protoItem.SerializeToString(&payload));

    ultraverse::state::v2::proto::StateItem restoredProto;
    REQUIRE(restoredProto.ParseFromString(payload));

    StateItem restored;
    restored.fromProtobuf(restoredProto);

    requireStateItemEqual(original, restored);
}
