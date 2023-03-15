include(CTest)

add_compile_definitions(SPDLOG_FMT_EXTERNAL)

include_directories(../dependencies/sql-parser/src)
include_directories(../dependencies/sql-parser/src/parser)
include_directories(../dependencies/nlohmann-json/include)
include_directories(../dependencies/cereal/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../dependencies/sql-parser)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${PYTHON3_INCLUDE_DIRS})

include_directories(../src)

add_executable(sqlparser-test sqlparser-test.cpp)
target_link_libraries(sqlparser-test sqlparser)

add_test(NAME sqlparser-test COMMAND sqlparser-test)

add_executable(pysqlparser-test pysqlparser-test.cpp )
target_link_libraries(pysqlparser-test ultraverse)

add_test(NAME pysqlparser-test COMMAND pysqlparser-test)
