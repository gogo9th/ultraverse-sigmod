//
// Created by cheesekun on 3/6/23.
//

#include <iostream>

#include "../src/sql/PySQLParser.hpp"

using namespace ultraverse::sql;

int main() {
    PySQLParser::initialize();
    
    {
        PySQLParser parser;
        
        std::string hint = parser.getProcedureHint("INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (procname) VALUES ('HELOWRLD');");
        std::cout << hint << std::endl;
    }
    
    PySQLParser::finalize();
    return 0;
}