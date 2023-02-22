//
// Created by cheesekun on 2/22/23.
//

#include <iostream>
#include <SQLParser.h>

#define OK(sql) \
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
    OK("SELECT 1;")
    
    // function call
    OK("SELECT NOW();")
    OK("SELECT now();")
    OK("UPDATE users SET joined_at = NOW() WHERE id = 32;")
    
    // MySQL NAME_CONST
    OK("UPDATE warehouse SET W_YTD := W_YTD +  NAME_CONST('var_paymentAmount',3980.34) WHERE W_ID = NAME_CONST('var_w_id',10);")
    OK("UPDATE customer SET C_BALANCE =  NAME_CONST('var_c_balance',-3990.34), C_YTD_PAYMENT =  NAME_CONST('var_c_ytd_payment',3990.34),      C_PAYMENT_CNT =  NAME_CONST('var_c_payment_cnt',2)     WHERE C_W_ID =  NAME_CONST('var_customerWarehouseID',10) AND C_D_ID =  NAME_CONST('var_customerDistrictID',7)      AND C_ID =  NAME_CONST('var_c_id',62)")
    
    // NAME_CONST with 'strval' COLLATE charset
    OK("INSERT users (name) VALUES (NAME_CONST('var_name', 'testuser' COLLATE 'utf8mb4_general_ci');")
    OK("UPDATE users SET name = NAME_CONST('var_name', 'testuser' COLLATE 'utf8mb4_general_ci') WHERE id = 42;")
    
    // NAME_CONST with _utf8mb4'strval' COLLATE charset
    OK("INSERT users (name) VALUES (NAME_CONST('var_name', _utf8mb4'testuser' COLLATE 'utf8mb4_general_ci');")
    
    return 0;
}