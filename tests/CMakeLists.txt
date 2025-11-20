include(CTest)
set(CMAKE_CXX_STANDARD 17)

add_compile_options(-msse4.2 -O2)
add_compile_definitions(SPDLOG_FMT_EXTERNAL)

include_directories(../dependencies/sql-parser/src)
include_directories(../dependencies/sql-parser/src/parser)
include_directories(../dependencies/nlohmann-json/include)
include_directories(../dependencies/cereal/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../dependencies/sql-parser)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${PYTHON3_INCLUDE_DIRS})

include_directories(../src)

add_executable(cluster-test cluster-test.cpp)
target_compile_definitions(cluster-test PRIVATE CATCH_CONFIG_MAIN)

target_link_libraries(cluster-test ultraverse)

add_test(NAME cluster-test COMMAND cluster-test)
