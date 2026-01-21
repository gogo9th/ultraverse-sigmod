include(CTest)
include(FetchContent)
set(CMAKE_CXX_STANDARD 20)

add_compile_options(-O0 -g)
add_compile_definitions(SPDLOG_FMT_EXTERNAL)

if (APPLE)
    add_compile_options(-fexperimental-library "-I/opt/homebrew/opt/llvm/include")
endif()

find_package(Boost REQUIRED)

include_directories(../dependencies/sql-parser/src)
include_directories(../dependencies/sql-parser/src/parser)
include_directories(../dependencies/nlohmann-json/include)
include_directories(../dependencies/cereal/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../dependencies/sql-parser)
include_directories(${Boost_INCLUDE_DIRS})

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../parserlib/cpp_autogen)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../parserlib/cpp)

include_directories(../src)

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.12.0
)
FetchContent_MakeAvailable(Catch2)

# add_executable(cluster-test cluster-test.cpp)
# target_compile_definitions(cluster-test PRIVATE CATCH_CONFIG_MAIN)

# target_link_libraries(cluster-test ultraverse Catch2::Catch2WithMain)

# if (APPLE)
#     target_link_options(cluster-test PRIVATE "-L/opt/homebrew/lib")
# endif()


# add_test(NAME cluster-test COMMAND cluster-test)

add_executable(statecluster-test statecluster-test.cpp)
target_link_libraries(statecluster-test ultraverse Catch2::Catch2WithMain)

add_executable(rowgraph-test rowgraph-test.cpp)
target_link_libraries(rowgraph-test ultraverse Catch2::Catch2WithMain)
# target_compile_definitions(rowgraph-test PRIVATE ULTRAVERSE_TESTING)

add_executable(relationship-resolver-test relationship-resolver-test.cpp)
target_link_libraries(relationship-resolver-test ultraverse Catch2::Catch2WithMain)

add_executable(statehash-test statehash-test.cpp)
target_link_libraries(statehash-test ultraverse Catch2::Catch2WithMain)

add_executable(stateitem-test stateitem-test.cpp)
target_link_libraries(stateitem-test ultraverse Catch2::Catch2WithMain)

add_executable(sqlparser-test sqlparser-test.cpp)
target_link_libraries(sqlparser-test ultraverse Catch2::Catch2WithMain)

add_executable(tabledependencygraph-test tabledependencygraph-test.cpp)
target_link_libraries(tabledependencygraph-test ultraverse Catch2::Catch2WithMain)

add_executable(naminghistory-test naminghistory-test.cpp)
target_link_libraries(naminghistory-test ultraverse Catch2::Catch2WithMain)

add_executable(rowcluster-test rowcluster-test.cpp)
target_link_libraries(rowcluster-test ultraverse Catch2::Catch2WithMain)

add_executable(taintanalyzer-test taintanalyzer-test.cpp)
target_link_libraries(taintanalyzer-test ultraverse Catch2::Catch2WithMain)

add_executable(queryeventbase-rwset-test queryeventbase-rwset-test.cpp)
target_link_libraries(queryeventbase-rwset-test ultraverse Catch2::Catch2WithMain)

add_executable(statechanger-test
        statechanger-test.cpp

        ../src/mariadb/state/new/StateChanger.cpp
        ../src/mariadb/state/new/StateChanger.hpp
        ../src/mariadb/state/new/StateChanger.prepare.cpp
        ../src/mariadb/state/new/StateChanger.replay.cpp
        ../src/mariadb/state/new/StateChangeContext.hpp
)
target_link_libraries(statechanger-test ultraverse Catch2::Catch2WithMain tbb)

add_dependencies(sqlparser-test ultparserlib)

if (APPLE)
    target_link_options(statecluster-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(rowgraph-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(relationship-resolver-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(statehash-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(stateitem-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(sqlparser-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(tabledependencygraph-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(naminghistory-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(rowcluster-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(taintanalyzer-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(queryeventbase-rwset-test PRIVATE "-L/opt/homebrew/lib")
    target_link_options(statechanger-test PRIVATE "-L/opt/homebrew/lib" -fexperimental-library)
endif()

add_test(NAME statecluster-test COMMAND statecluster-test)
add_test(NAME rowgraph-test COMMAND rowgraph-test)
add_test(NAME relationship-resolver-test COMMAND relationship-resolver-test)
add_test(NAME statehash-test COMMAND statehash-test)
add_test(NAME stateitem-test COMMAND stateitem-test)
add_test(NAME sqlparser-test COMMAND sqlparser-test)
add_test(NAME tabledependencygraph-test COMMAND tabledependencygraph-test)
add_test(NAME naminghistory-test COMMAND naminghistory-test)
add_test(NAME rowcluster-test COMMAND rowcluster-test)
add_test(NAME taintanalyzer-test COMMAND taintanalyzer-test)
add_test(NAME queryeventbase-rwset-test COMMAND queryeventbase-rwset-test)
add_test(NAME statechanger-test COMMAND statechanger-test)
