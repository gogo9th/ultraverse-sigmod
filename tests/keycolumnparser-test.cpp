//
// Key column parser unit tests
//

#include <catch2/catch_test_macros.hpp>

#include <vector>

#include "utils/StringUtil.hpp"

using ultraverse::utility::flattenKeyColumnGroups;
using ultraverse::utility::parseKeyColumnGroups;

TEST_CASE("parseKeyColumnGroups parses vector entries with composite keys", "[keyColumns]") {
    std::vector<std::string> input{
        "table1.column1",
        "table2.column2+table3.column3"
    };

    auto groups = parseKeyColumnGroups(input);

    REQUIRE(groups.size() == 2);
    CHECK(groups[0] == std::vector<std::string>{"table1.column1"});
    CHECK(groups[1] == std::vector<std::string>{"table2.column2", "table3.column3"});
}

TEST_CASE("parseKeyColumnGroups trims whitespace and skips empty entries", "[keyColumns]") {
    std::string expression = " table1.column1 , table2.column2 + table3.column3 , , ";

    auto groups = parseKeyColumnGroups(expression);

    REQUIRE(groups.size() == 2);
    CHECK(groups[0] == std::vector<std::string>{"table1.column1"});
    CHECK(groups[1] == std::vector<std::string>{"table2.column2", "table3.column3"});

    auto flat = flattenKeyColumnGroups(groups);
    CHECK(flat == std::vector<std::string>{
        "table1.column1",
        "table2.column2",
        "table3.column3"
    });
}
