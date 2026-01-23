//
// Created by cheesekun on 2/22/23.
//

#include <algorithm>
#include <array>
#include <cmath>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <string>

#include <libultparser/libultparser.h>
#include <ultparser_query.pb.h>

#define OK(expr, message)                                            \
    std::cerr << message << "... ";                                  \
    if (!(expr)) {                                                   \
        std::cerr << "FAIL" << std::endl;                            \
        return false;                                                \
    } else {                                                         \
        std::cerr << "OK" << std::endl;                              \
    }

#define SQL_OK(sql)                  \
    if (parseSQL(sql) == false) {    \
        return false;                \
    }

#define NOT_OK(sql)                  \
    if (parseSQL(sql) == true) {     \
        return false;                \
    }

static std::string toLower(const std::string &value) {
    std::string out = value;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return out;
}

static uintptr_t g_parser = 0;

bool parseSQL(const std::string &sqlString, ultparser::ParseResult *out = nullptr) {
    std::cerr << "testing " << sqlString << " ... ";

    if (g_parser == 0) {
        g_parser = ult_sql_parser_create();
    }
    ultparser::ParseResult parseResult;
    char *parseResultCStr = nullptr;
    int64_t parseResultCStrSize = ult_sql_parse_new(
        g_parser,
        (char *) sqlString.c_str(),
        static_cast<int64_t>(sqlString.size()),
        &parseResultCStr
    );

    bool isSuccessful = false;
    if (parseResultCStrSize <= 0 || parseResultCStr == nullptr) {
        isSuccessful = false;
    } else if (!parseResult.ParseFromArray(parseResultCStr, parseResultCStrSize)) {
        isSuccessful = false;
    } else if (parseResult.result() != ultparser::ParseResult::SUCCESS) {
        isSuccessful = false;
    } else {
        isSuccessful = true;
    }

    if (parseResultCStr != nullptr) {
        free(parseResultCStr);
    }

    std::cerr << (isSuccessful ? "OK" : "FAIL") << "\n";

    if (!isSuccessful) {
        if (parseResult.result() == ultparser::ParseResult::ERROR) {
            std::cerr << "parser error: " << parseResult.error() << std::endl;
        } else {
            std::cerr << "failed to parse SQL: " << sqlString << std::endl;
        }
    } else if (parseResult.warnings_size() > 0) {
        for (const auto &warning : parseResult.warnings()) {
            std::cerr << "parser warning: " << warning << std::endl;
        }
    }

    if (out != nullptr) {
        *out = parseResult;
    }

    return isSuccessful;
}

bool hashSQL(const std::string &sqlString, std::array<unsigned char, 20> *out) {
    if (g_parser == 0) {
        g_parser = ult_sql_parser_create();
    }

    std::array<unsigned char, 20> hash{};
    int64_t hashSize = ult_query_hash_new(
        g_parser,
        (char *) sqlString.c_str(),
        static_cast<int64_t>(sqlString.size()),
        reinterpret_cast<char *>(hash.data())
    );

    if (hashSize != 20) {
        std::cerr << "hash FAIL: " << sqlString << "\n";
        return false;
    }

    if (out != nullptr) {
        *out = hash;
    }

    return true;
}

bool runTests() {
    SQL_OK("SELECT 1;")
    
    // function call
    SQL_OK("SELECT NOW();")
    SQL_OK("SELECT now();")
    SQL_OK("UPDATE users SET joined_at = NOW() WHERE id = 32;")
    
    {
        std::string sqlString = "UPDATE users SET joined_at = NOW() WHERE id = 32;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "statements_size must be 1");

        const auto &statement = parseResult.statements(0);
        OK(statement.has_dml(), "statement must be DML");

        const auto &dml = statement.dml();
        OK(dml.type() == ultparser::DMLQuery::UPDATE, "statement type must be UPDATE");
        OK(dml.update_or_write_size() == 1, "update_or_write size must be 1");

        const auto &assignment = dml.update_or_write(0);
        OK(assignment.operator_() == ultparser::DMLQueryExpr::EQ, "assignment operator must be EQ");
        OK(assignment.has_left(), "assignment must have left");
        OK(assignment.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "left must be identifier");
        OK(toLower(assignment.left().identifier()) == "joined_at", "left identifier must be joined_at");

        OK(assignment.has_right(), "assignment must have right");
        OK(assignment.right().value_type() == ultparser::DMLQueryExpr::FUNCTION, "right must be function");
        OK(toLower(assignment.right().function()) == "now", "function name must be now");
        OK(assignment.right().value_list_size() == 0, "NOW() must have no args");

        OK(dml.has_where(), "where clause must exist");
        const auto &where = dml.where();
        OK(where.operator_() == ultparser::DMLQueryExpr::EQ, "where operator must be EQ");
        OK(where.has_left(), "where must have left");
        OK(where.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "where left must be identifier");
        OK(toLower(where.left().identifier()) == "id", "where left must be id");
        OK(where.has_right(), "where must have right");
        OK(where.right().value_type() == ultparser::DMLQueryExpr::INTEGER, "where right must be integer");
        OK(where.right().integer() == 32, "where right must be 32");
    }
    
    // MySQL NAME_CONST
    SQL_OK("UPDATE warehouse SET W_YTD := W_YTD +  NAME_CONST('var_paymentAmount',3980.34) WHERE W_ID = NAME_CONST('var_w_id',10);")
    
    {
        std::string sqlString = "UPDATE warehouse SET W_YTD := W_YTD +  NAME_CONST('var_paymentAmount',3980.34) WHERE W_ID = NAME_CONST('var_w_id',10);";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "statements_size must be 1");

        const auto &statement = parseResult.statements(0);
        OK(statement.has_dml(), "statement must be DML");

        const auto &dml = statement.dml();
        OK(dml.type() == ultparser::DMLQuery::UPDATE, "statement type must be UPDATE");
        OK(dml.update_or_write_size() == 1, "update_or_write size must be 1");

        const auto &assignment = dml.update_or_write(0);
        OK(assignment.operator_() == ultparser::DMLQueryExpr::EQ, "assignment operator must be EQ");
        OK(assignment.has_right(), "assignment must have right");
        OK(assignment.right().operator_() == ultparser::DMLQueryExpr::PLUS, "update expr must be PLUS");

        const auto &plusLeft = assignment.right().left();
        const auto &plusRight = assignment.right().right();

        OK(plusLeft.value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "plus left must be identifier");
        OK(toLower(plusLeft.identifier()) == "w_ytd", "plus left identifier must be w_ytd");

        OK(plusRight.value_type() == ultparser::DMLQueryExpr::FUNCTION, "plus right must be function");
        OK(toLower(plusRight.function()) == "name_const", "function name must be NAME_CONST");
        OK(plusRight.value_list_size() == 2, "NAME_CONST must have 2 args");

        const auto &arg0 = plusRight.value_list(0);
        const auto &arg1 = plusRight.value_list(1);

        OK(arg0.value_type() == ultparser::DMLQueryExpr::STRING, "NAME_CONST arg0 must be string");
        OK(toLower(arg0.string()) == "var_paymentamount", "NAME_CONST arg0 must be var_paymentAmount");

        OK(arg1.value_type() == ultparser::DMLQueryExpr::DECIMAL, "NAME_CONST arg1 must be decimal");
        OK(arg1.decimal() == "3980.34", "NAME_CONST arg1 must be 3980.34");
    }
    
    SQL_OK("UPDATE customer SET C_BALANCE =  NAME_CONST('var_c_balance',-3990.34), C_YTD_PAYMENT =  NAME_CONST('var_c_ytd_payment',3990.34),      C_PAYMENT_CNT =  NAME_CONST('var_c_payment_cnt',2)     WHERE C_W_ID =  NAME_CONST('var_customerWarehouseID',10) AND C_D_ID =  NAME_CONST('var_customerDistrictID',7)      AND C_ID =  NAME_CONST('var_c_id',62)")
    
    // NAME_CONST with 'strval' COLLATE charset
    SQL_OK("INSERT users (name) VALUES (NAME_CONST('var_name', 'testuser' COLLATE 'utf8mb4_general_ci'));")
    SQL_OK("UPDATE users SET name = NAME_CONST('var_name', 'testuser' COLLATE 'utf8mb4_general_ci') WHERE id = 42;")
    
    // NAME_CONST with _utf8mb4'strval' COLLATE charset
    SQL_OK("INSERT users (name) VALUES (NAME_CONST('var_name', _utf8mb4'testuser' COLLATE 'utf8mb4_general_ci'));")
    
    {
        std::string sqlString = "INSERT users (name, point) VALUES (NAME_CONST('var_name', _utf8mb4'testuser' COLLATE 'utf8mb4_general_ci'), NAME_CONST('var_point', 32));";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "statements_size must be 1");

        const auto &statement = parseResult.statements(0);
        OK(statement.has_dml(), "statement must be DML");

        const auto &dml = statement.dml();
        OK(dml.type() == ultparser::DMLQuery::INSERT, "statement type must be INSERT");
        OK(dml.update_or_write_size() == 2, "update_or_write size must be 2");

        const auto &nameAssignment = dml.update_or_write(0);
        OK(nameAssignment.has_left(), "name assignment must have left");
        OK(nameAssignment.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "name left must be identifier");
        OK(toLower(nameAssignment.left().identifier()) == "name", "name column must be name");
        OK(nameAssignment.has_right(), "name assignment must have right");
        OK(nameAssignment.right().value_type() == ultparser::DMLQueryExpr::FUNCTION, "name right must be function");
        OK(toLower(nameAssignment.right().function()) == "name_const", "name function must be NAME_CONST");
        OK(nameAssignment.right().value_list_size() == 2, "name NAME_CONST must have 2 args");
        OK(nameAssignment.right().value_list(0).value_type() == ultparser::DMLQueryExpr::STRING,
           "name NAME_CONST arg0 must be string");
        OK(toLower(nameAssignment.right().value_list(0).string()) == "var_name",
           "name NAME_CONST arg0 must be var_name");

        const auto &pointAssignment = dml.update_or_write(1);
        OK(pointAssignment.has_left(), "point assignment must have left");
        OK(pointAssignment.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "point left must be identifier");
        OK(toLower(pointAssignment.left().identifier()) == "point", "point column must be point");
        OK(pointAssignment.has_right(), "point assignment must have right");
        OK(pointAssignment.right().value_type() == ultparser::DMLQueryExpr::FUNCTION, "point right must be function");
        OK(toLower(pointAssignment.right().function()) == "name_const", "point function must be NAME_CONST");
        OK(pointAssignment.right().value_list_size() == 2, "point NAME_CONST must have 2 args");
        OK(pointAssignment.right().value_list(1).value_type() == ultparser::DMLQueryExpr::INTEGER,
           "point NAME_CONST arg1 must be integer");
        OK(pointAssignment.right().value_list(1).integer() == 32,
           "point NAME_CONST arg1 must be 32");
    }
    
    // NAME_CONST() * NAME_CONST
    SQL_OK("UPDATE scores SET score = NAME_CONST('var_score', 32) * NAME_CONST('var_multiplier', 2) WHERE user_id = 42;")
    SQL_OK("UPDATE scores SET score = (NAME_CONST('var_score', 32) * NAME_CONST('var_multiplier', 2)) WHERE user_id = 42;")
    
    SQL_OK("INSERT scores (user_id, score) VALUES (42, NAME_CONST('var_score', 32) * NAME_CONST('var_multiplier', 2));")

    // complex boolean + arithmetic expressions
    SQL_OK("SELECT u.id, u.name, (u.score + 10) * 2 AS boosted FROM users u WHERE (u.status = 'active' AND u.score >= 100) OR (u.status = 'new' AND u.score < 20);")

    {
        std::string sqlString = "SELECT u.id, u.name, (u.score + 10) * 2 AS boosted FROM users u WHERE (u.status = 'active' AND u.score >= 100) OR (u.status = 'new' AND u.score < 20);";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "statements_size must be 1");

        const auto &statement = parseResult.statements(0);
        OK(statement.has_dml(), "statement must be DML");

        const auto &dml = statement.dml();
        OK(dml.type() == ultparser::DMLQuery::SELECT, "statement type must be SELECT");
        OK(dml.select_size() == 3, "select size must be 3");

        OK(dml.has_where(), "where clause must exist");
        const auto &where = dml.where();
        OK(where.operator_() == ultparser::DMLQueryExpr::OR, "where operator must be OR");
        OK(where.expressions_size() == 2, "OR expressions size must be 2");

        const auto &leftAnd = where.expressions(0);
        const auto &rightAnd = where.expressions(1);
        OK(leftAnd.operator_() == ultparser::DMLQueryExpr::AND, "left expression must be AND");
        OK(rightAnd.operator_() == ultparser::DMLQueryExpr::AND, "right expression must be AND");
        OK(leftAnd.expressions_size() == 2, "left AND expressions size must be 2");
        OK(rightAnd.expressions_size() == 2, "right AND expressions size must be 2");
    }

    SQL_OK("SELECT id FROM users WHERE name LIKE 'A%' AND email LIKE '%@example.com';")
    SQL_OK("UPDATE orders SET checksum = MD5(CONCAT(user_id, '-', order_id)), score = (score + 5) * 3 WHERE (status = 'paid' OR status = 'shipped') AND total >= 1000;")
    SQL_OK("DELETE FROM logs WHERE (level = 'debug' OR level = 'trace') AND (retry_count % 3 = 0);")
    SQL_OK("INSERT INTO pricing (sku, price, discount, note) VALUES ('SKU-1', 19.9900, -0.05, CONCAT('promo-', 2026));")

    // SELECT ... INTO (procedure-style variable assignment)
    SQL_OK("SELECT score INTO game_score FROM game_records WHERE user_id = 1;")

    {
        std::string sqlString = "SELECT score INTO game_score FROM game_records WHERE user_id = 1;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "statements_size must be 1");

        const auto &statement = parseResult.statements(0);
        OK(statement.has_dml(), "statement must be DML");

        const auto &dml = statement.dml();
        OK(dml.type() == ultparser::DMLQuery::SELECT, "statement type must be SELECT");
        OK(dml.select_size() == 1, "select size must be 1");

        OK(dml.table().real().value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "table must be identifier");
        OK(toLower(dml.table().real().identifier()) == "game_records", "table must be game_records");

        OK(dml.select(0).real().value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "select expr must be identifier");
        OK(toLower(dml.select(0).real().identifier()) == "score", "select expr must be score");

        OK(dml.has_where(), "where clause must exist");
        const auto &where = dml.where();
        OK(where.operator_() == ultparser::DMLQueryExpr::EQ, "where operator must be EQ");
        OK(where.has_left(), "where must have left");
        OK(where.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "where left must be identifier");
        OK(toLower(where.left().identifier()) == "user_id", "where left must be user_id");
        OK(where.has_right(), "where must have right");
        OK(where.right().value_type() == ultparser::DMLQueryExpr::INTEGER, "where right must be integer");
        OK(where.right().integer() == 1, "where right must be 1");
    }

    {
        std::string sqlString =
            "CREATE PROCEDURE test_select_into()\n"
            "BEGIN\n"
            "  SELECT score INTO game_score FROM game_records WHERE user_id = 1;\n"
            "END;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "statements_size must be 1");

        const auto &statement = parseResult.statements(0);
        OK(statement.type() == ultparser::Query::PROCEDURE, "statement type must be PROCEDURE");
        OK(statement.has_procedure(), "statement must have procedure");

        const auto &procedure = statement.procedure();
        OK(toLower(procedure.name()) == "test_select_into", "procedure name must be test_select_into");
        OK(procedure.statements_size() == 1, "procedure statements_size must be 1");

        const auto &procStatement = procedure.statements(0);
        OK(procStatement.has_dml(), "procedure statement must be DML");

        const auto &dml = procStatement.dml();
        OK(dml.type() == ultparser::DMLQuery::SELECT, "procedure statement type must be SELECT");
        OK(dml.select_size() == 1, "procedure select size must be 1");
    }

    // SET variable tests
    SQL_OK("SET @x = 1;")
    SQL_OK("SET @user_id = 42, @name = 'test';")
    SQL_OK("SET @total = @price * @quantity;")

    {
        std::string sqlString = "SET @x = 1;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "SET statements_size must be 1");

        const auto &statement = parseResult.statements(0);
        OK(statement.type() == ultparser::Query::SET, "statement type must be SET");
        OK(statement.has_set(), "statement must have set");

        const auto &setQuery = statement.set();
        OK(setQuery.assignments_size() == 1, "SET assignments_size must be 1");

        const auto &assignment = setQuery.assignments(0);
        OK(assignment.name() == "x", "SET variable name must be x");
        OK(!assignment.is_global(), "SET is_global must be false");
        OK(!assignment.is_system(), "SET is_system must be false");
        OK(assignment.has_value(), "SET must have value");
        OK(assignment.value().value_type() == ultparser::DMLQueryExpr::INTEGER, "SET value type must be INTEGER");
        OK(assignment.value().integer() == 1, "SET value must be 1");
    }

    {
        std::string sqlString = "SET @user_id = 42, @name = 'test';";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "SET multiple statements_size must be 1");

        const auto &setQuery = parseResult.statements(0).set();
        OK(setQuery.assignments_size() == 2, "SET multiple assignments_size must be 2");

        OK(setQuery.assignments(0).name() == "user_id", "first variable name must be user_id");
        OK(setQuery.assignments(0).value().integer() == 42, "first value must be 42");

        OK(setQuery.assignments(1).name() == "name", "second variable name must be name");
        OK(setQuery.assignments(1).value().string() == "test", "second value must be test");
    }

    {
        std::string sqlString = "SET @total = @price * @quantity;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        const auto &setQuery = parseResult.statements(0).set();
        OK(setQuery.assignments_size() == 1, "SET expr assignments_size must be 1");
        OK(setQuery.assignments(0).name() == "total", "SET expr variable name must be total");

        const auto &value = setQuery.assignments(0).value();
        OK(value.operator_() == ultparser::DMLQueryExpr::MUL, "SET expr operator must be MUL");
        OK(value.has_left(), "SET expr must have left");
        OK(value.has_right(), "SET expr must have right");
        OK(value.left().identifier() == "@price", "SET expr left must be @price");
        OK(value.right().identifier() == "@quantity", "SET expr right must be @quantity");
    }

    {
        std::string sqlString = "SELECT * FROM users; INSERT INTO users (id) VALUES (1);";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 2, "multi statement size must be 2");
        OK(parseResult.statements(0).has_dml(), "first statement must be DML");
        OK(parseResult.statements(0).dml().type() == ultparser::DMLQuery::SELECT,
           "first statement must be SELECT");
        OK(parseResult.statements(1).has_dml(), "second statement must be DML");
        OK(parseResult.statements(1).dml().type() == ultparser::DMLQuery::INSERT,
           "second statement must be INSERT");
    }

    {
        std::string sqlString = "SELECT * FROM /* comment */ users;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "commented SELECT statements_size must be 1");
        const auto &statement = parseResult.statements(0);
        OK(statement.has_dml(), "commented SELECT must be DML");
        const auto &dml = statement.dml();
        OK(dml.type() == ultparser::DMLQuery::SELECT, "commented SELECT type must be SELECT");
        OK(dml.table().real().value_type() == ultparser::DMLQueryExpr::IDENTIFIER,
           "commented SELECT table must be identifier");
        OK(toLower(dml.table().real().identifier()) == "users", "commented SELECT table must be users");
    }

    {
        std::string sqlA = "SELECT * FROM users;";
        std::string sqlB =
            "SELECT\n"
            "    *\n"
            "FROM\n"
            "    users\n"
            ";\n";
        std::array<unsigned char, 20> hashA{};
        std::array<unsigned char, 20> hashB{};

        OK(hashSQL(sqlA, &hashA), "hash for single-line SELECT");
        OK(hashSQL(sqlB, &hashB), "hash for multi-line SELECT");
        OK(std::equal(hashA.begin(), hashA.end(), hashB.begin()),
           "hash must match for equivalent queries");
    }

    // Subquery tests
    SQL_OK("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);")
    SQL_OK("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id);")
    SQL_OK("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM banned WHERE banned.user_id = users.id);")
    SQL_OK("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users);")
    SQL_OK("SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users;")
    SQL_OK("SELECT * FROM (SELECT id, name FROM users) AS t WHERE t.id = 1;")

    // Subquery IN test
    {
        std::string sqlString = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "subquery IN statements_size must be 1");

        const auto &dml = parseResult.statements(0).dml();
        OK(dml.type() == ultparser::DMLQuery::SELECT, "subquery IN type must be SELECT");
        OK(dml.has_where(), "subquery IN must have WHERE");

        const auto &where = dml.where();
        OK(where.operator_() == ultparser::DMLQueryExpr::IN, "subquery IN where operator must be IN");
        OK(where.has_left(), "subquery IN where must have left");
        OK(where.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "subquery IN left must be identifier");
        OK(toLower(where.left().identifier()) == "id", "subquery IN left must be id");

        OK(where.has_right(), "subquery IN where must have right");
        OK(where.right().value_type() == ultparser::DMLQueryExpr::SUBQUERY, "subquery IN right must be SUBQUERY");
        OK(where.right().has_subquery(), "subquery IN right must have subquery");

        const auto &subquery = where.right().subquery();
        OK(subquery.type() == ultparser::DMLQuery::SELECT, "inner subquery type must be SELECT");
        OK(toLower(subquery.table().real().identifier()) == "orders", "inner subquery table must be orders");
    }

    // EXISTS subquery test
    {
        std::string sqlString = "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id);";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        OK(parseResult.statements_size() == 1, "EXISTS statements_size must be 1");

        const auto &dml = parseResult.statements(0).dml();
        OK(dml.has_where(), "EXISTS must have WHERE");

        const auto &where = dml.where();
        OK(where.value_type() == ultparser::DMLQueryExpr::SUBQUERY, "EXISTS where must be SUBQUERY");
        OK(where.subquery_exists(), "EXISTS subquery_exists must be true");
        OK(!where.subquery_not(), "EXISTS subquery_not must be false");
        OK(where.has_subquery(), "EXISTS must have subquery");
    }

    // NOT EXISTS subquery test
    {
        std::string sqlString = "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM banned WHERE banned.user_id = users.id);";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        const auto &dml = parseResult.statements(0).dml();
        const auto &where = dml.where();
        OK(where.value_type() == ultparser::DMLQueryExpr::SUBQUERY, "NOT EXISTS where must be SUBQUERY");
        OK(where.subquery_exists(), "NOT EXISTS subquery_exists must be true");
        OK(where.subquery_not(), "NOT EXISTS subquery_not must be true");
    }

    // Scalar subquery test
    {
        std::string sqlString = "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        const auto &dml = parseResult.statements(0).dml();
        OK(dml.select_size() == 2, "scalar subquery select size must be 2");

        const auto &scalarSubquery = dml.select(1);
        OK(toLower(scalarSubquery.alias()) == "order_count", "scalar subquery alias must be order_count");
        OK(scalarSubquery.real().value_type() == ultparser::DMLQueryExpr::SUBQUERY, "scalar subquery must be SUBQUERY");
        OK(scalarSubquery.real().has_subquery(), "scalar subquery must have subquery");
    }

    // Derived table test
    {
        std::string sqlString = "SELECT * FROM (SELECT id, name FROM users) AS t WHERE t.id = 1;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        const auto &dml = parseResult.statements(0).dml();
        OK(dml.subqueries_size() >= 1, "derived table must have subqueries");

        const auto &derivedTable = dml.subqueries(0);
        OK(derivedTable.type() == ultparser::DMLQuery::SELECT, "derived table type must be SELECT");
        OK(toLower(derivedTable.table().real().identifier()) == "users", "derived table inner table must be users");
    }

    // Aggregate function tests
    SQL_OK("SELECT COUNT(*) FROM users;")
    SQL_OK("SELECT SUM(amount), AVG(price) FROM orders;")
    SQL_OK("SELECT COUNT(DISTINCT user_id) FROM orders;")
    SQL_OK("SELECT MIN(created_at), MAX(updated_at) FROM posts;")

    // Aggregate function detailed test
    {
        std::string sqlString = "SELECT COUNT(*), SUM(amount) FROM orders;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        const auto &dml = parseResult.statements(0).dml();
        OK(dml.select_size() == 2, "aggregate select size must be 2");

        const auto &countExpr = dml.select(0).real();
        OK(countExpr.value_type() == ultparser::DMLQueryExpr::FUNCTION, "COUNT must be FUNCTION");
        OK(toLower(countExpr.function()) == "count", "function name must be count");
        OK(countExpr.is_aggregate(), "COUNT is_aggregate must be true");

        const auto &sumExpr = dml.select(1).real();
        OK(sumExpr.value_type() == ultparser::DMLQueryExpr::FUNCTION, "SUM must be FUNCTION");
        OK(toLower(sumExpr.function()) == "sum", "function name must be sum");
        OK(sumExpr.is_aggregate(), "SUM is_aggregate must be true");
        OK(sumExpr.value_list_size() == 1, "SUM must have 1 arg");
        OK(sumExpr.value_list(0).value_type() == ultparser::DMLQueryExpr::IDENTIFIER, "SUM arg must be identifier");
        OK(toLower(sumExpr.value_list(0).identifier()) == "amount", "SUM arg must be amount");
    }

    // COUNT DISTINCT test
    {
        std::string sqlString = "SELECT COUNT(DISTINCT user_id) FROM orders;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        const auto &dml = parseResult.statements(0).dml();
        const auto &countExpr = dml.select(0).real();
        OK(countExpr.value_type() == ultparser::DMLQueryExpr::FUNCTION, "COUNT DISTINCT must be FUNCTION");
        OK(countExpr.is_aggregate(), "COUNT DISTINCT is_aggregate must be true");
        OK(countExpr.is_distinct(), "COUNT DISTINCT is_distinct must be true");
    }

    // Regular function should NOT be aggregate
    {
        std::string sqlString = "SELECT NOW(), UPPER(name) FROM users;";
        ultparser::ParseResult parseResult;

        if (!parseSQL(sqlString, &parseResult)) {
            return false;
        }

        const auto &dml = parseResult.statements(0).dml();
        const auto &nowExpr = dml.select(0).real();
        OK(nowExpr.value_type() == ultparser::DMLQueryExpr::FUNCTION, "NOW must be FUNCTION");
        OK(!nowExpr.is_aggregate(), "NOW is_aggregate must be false");

        const auto &upperExpr = dml.select(1).real();
        OK(upperExpr.value_type() == ultparser::DMLQueryExpr::FUNCTION, "UPPER must be FUNCTION");
        OK(!upperExpr.is_aggregate(), "UPPER is_aggregate must be false");
    }

    return true;
}

int main() {
    g_parser = ult_sql_parser_create();
    bool ok = runTests();
    ult_sql_parser_destroy(g_parser);
    g_parser = 0;
    return ok ? 0 : 1;
}
