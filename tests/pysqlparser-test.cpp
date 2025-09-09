//
// Created by cheesekun on 3/6/23.
//

#include <iostream>

#include "../src/sql/PySQLParser.hpp"

using namespace ultraverse::sql;

int main() {
    PySQLParser::initialize();
    
    PySQLParser::finalize();
    return 0;
}