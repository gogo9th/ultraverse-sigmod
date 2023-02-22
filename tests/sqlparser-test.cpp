//
// Created by cheesekun on 2/22/23.
//

#include <cstring>
#include <iostream>

#include <SQLParser.h>

#define OK(expr, message) \
    std::cerr << message << "... "; \
    if (!(expr)) { \
        std::cerr << "FAIL" << std::endl; \
        return 1; \
    } else { \
        std::cerr << "OK" << std::endl; \
    } \
    
#define SQL_OK(sql) \
    if (parseSQL(sql) == false) return 1;

#define NOT_OK(sql) \
    if (parseSQL(sql) == true) return 1;

bool parseSQL(const std::string &sqlString) {
    std::cerr << "testing " << sqlString << " ... ";
    
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(sqlString, &result);
    bool isSuccessful = result.isValid();
    
    std::cerr << (isSuccessful ? "OK" : "FAIL") << "\n";
    
    if (!isSuccessful) {
        std::cerr << "failed to parse SQL: " << sqlString << "\n"
                  << "    (" << result.errorMsg() << " at line " << result.errorLine() << ", column "
                  << result.errorColumn() << std::endl;
    }
    
    return isSuccessful;
}

int main() {
    SQL_OK("SELECT 1;")
    
    // function call
    SQL_OK("SELECT NOW();")
    SQL_OK("SELECT now();")
    SQL_OK("UPDATE users SET joined_at = NOW() WHERE id = 32;")
    
    {
        std::string sqlString = "UPDATE users SET joined_at = NOW() WHERE id = 32;";
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(sqlString, &result);
    
        if (!result.isValid()) {
            return 1;
        }
    
        auto *updateStatement = static_cast<hsql::UpdateStatement *>(result.getStatements()[0]);
        auto *updates = updateStatement->updates;
        auto *where = updateStatement->where;
    
        OK(updates->size() == 1, "updates->size() must be 1");
    
        OK(updates->at(0)->value->type == hsql::ExprType::kExprFunctionRef, "type of updates[0]->value must be function");
        OK(strcmp(updates->at(0)->value->name, "NOW") == 0, "value of updates[0]->value must be \"NOW\"");
    }
    
    // MySQL NAME_CONST
    SQL_OK("UPDATE warehouse SET W_YTD := W_YTD +  NAME_CONST('var_paymentAmount',3980.34) WHERE W_ID = NAME_CONST('var_w_id',10);")
    
    {
        std::string sqlString = "UPDATE warehouse SET W_YTD := W_YTD +  NAME_CONST('var_paymentAmount',3980.34) WHERE W_ID = NAME_CONST('var_w_id',10);";
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(sqlString, &result);
        
        if (!result.isValid()) {
            return 1;
        }
        
        auto *updateStatement = static_cast<hsql::UpdateStatement *>(result.getStatements()[0]);
        auto *updates = updateStatement->updates;
        auto *where = updateStatement->where;
        
        OK(updates->size() == 1, "updates->size() must be 1");
        
        auto *update = updates->at(0);
        
        OK(update->value->opType == hsql::kOpPlus, "type of updates[0]->opType must be PLUS");
        OK(strcmp(update->value->expr->name, "W_YTD") == 0, "value of updates[0]->expr must be \"W_YTD\"");
        OK(((int) update->value->expr2->fval) == 3980, "value of (int) updates[0]->expr2 must be 3980");
    }
    
    SQL_OK("UPDATE customer SET C_BALANCE =  NAME_CONST('var_c_balance',-3990.34), C_YTD_PAYMENT =  NAME_CONST('var_c_ytd_payment',3990.34),      C_PAYMENT_CNT =  NAME_CONST('var_c_payment_cnt',2)     WHERE C_W_ID =  NAME_CONST('var_customerWarehouseID',10) AND C_D_ID =  NAME_CONST('var_customerDistrictID',7)      AND C_ID =  NAME_CONST('var_c_id',62)")
    
    // NAME_CONST with 'strval' COLLATE charset
    SQL_OK("INSERT users (name) VALUES (NAME_CONST('var_name', 'testuser' COLLATE 'utf8mb4_general_ci'));")
    SQL_OK("UPDATE users SET name = NAME_CONST('var_name', 'testuser' COLLATE 'utf8mb4_general_ci') WHERE id = 42;")
    
    // NAME_CONST with _utf8mb4'strval' COLLATE charset
    SQL_OK("INSERT users (name) VALUES (NAME_CONST('var_name', _utf8mb4'testuser' COLLATE 'utf8mb4_general_ci'));")
    
    {
        std::string sqlString = "INSERT users (name, point) VALUES (NAME_CONST('var_name', _utf8mb4'testuser' COLLATE 'utf8mb4_general_ci'), NAME_CONST('var_point', 32));";
        hsql::SQLParserResult result;
        hsql::SQLParser::parse(sqlString, &result);
        
        if (!result.isValid()) {
            return 1;
        }
        
        auto *insertStatement = static_cast<hsql::InsertStatement *>(result.getStatements()[0]);
        auto *values = insertStatement->values;
        
        OK(values->size() == 2, "values->size() must be 2");
        
        OK(values->at(0)->type == hsql::ExprType::kExprLiteralString, "type of values[0] must be string");
        OK(strcmp(values->at(0)->name, "testuser") == 0, "value of values[0] must be \"testuser\"");
        
        OK(values->at(1)->type == hsql::ExprType::kExprLiteralInt, "type of values[1] must be Int");
        OK(values->at(1)->ival == 32, "value of values[1] must be 32");
    }
    
    // NAME_CONST() * NAME_CONST
    SQL_OK("UPDATE scores SET score = NAME_CONST('var_score', 32) * NAME_CONST('var_multiplier', 2) WHERE user_id = 42;")
    SQL_OK("UPDATE scores SET score = (NAME_CONST('var_score', 32) * NAME_CONST('var_multiplier', 2)) WHERE user_id = 42;")
    
    SQL_OK("INSERT scores (user_id, score) VALUES (42, NAME_CONST('var_score', 32) * NAME_CONST('var_multiplier', 2));")
    
    return 0;
}