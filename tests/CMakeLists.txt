include(CTest)

include_directories(../dependencies/sql-parser/src)
include_directories(../dependencies/sql-parser/src/parser)

add_executable(sqlparser-test sqlparser-test.cpp)
target_link_libraries(sqlparser-test sqlparser)

add_test(NAME sqlparser-test COMMAND sqlparser-test)