include(CTest)
include(FetchContent)

find_package(Boost REQUIRED)

include_directories(../dependencies/nlohmann-json/include)
include_directories(${Boost_INCLUDE_DIRS})

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../parserlib/cpp_autogen)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../parserlib/cpp)
include_directories(${CMAKE_BINARY_DIR}/src/proto_autogen)

include_directories(../src)

if (APPLE)
    set(ULTRAVERSE_RPATH_ORIGIN "@loader_path")
else()
    set(ULTRAVERSE_RPATH_ORIGIN "$ORIGIN")
endif()

set(ULTPARSERLIB_SO_PATH
    ${CMAKE_CURRENT_BINARY_DIR}/../parserlib/cpp/libultparser/libultparser.so
)

function(ultraverse_set_rpath target_name)
    if (NOT WIN32)
        set_property(TARGET ${target_name} APPEND PROPERTY
            BUILD_RPATH "${ULTRAVERSE_RPATH_ORIGIN}"
        )
        set_property(TARGET ${target_name} APPEND PROPERTY
            INSTALL_RPATH "${ULTRAVERSE_RPATH_ORIGIN}"
        )
    endif()
endfunction()

function(ultraverse_copy_ultparser target_name)
    add_custom_command(
        TARGET ${target_name} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${ULTPARSERLIB_SO_PATH}"
            "$<TARGET_FILE_DIR:${target_name}>/libultparser.so"
        COMMENT "Copying libultparser.so next to ${target_name}"
    )
endfunction()

function(ultraverse_add_homebrew_link_dir target_name)
    if (APPLE)
        target_link_options(${target_name} PRIVATE "-L/opt/homebrew/lib")
    endif()
endfunction()

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

add_executable(ultraverseconfig-test ultraverseconfig-test.cpp)
target_link_libraries(ultraverseconfig-test ultraverse Catch2::Catch2WithMain)

add_executable(keycolumnparser-test keycolumnparser-test.cpp)
target_link_libraries(keycolumnparser-test ultraverse Catch2::Catch2WithMain)

add_executable(stateitem-test stateitem-test.cpp)
target_link_libraries(stateitem-test ultraverse Catch2::Catch2WithMain)

add_executable(query-transaction-serialization-test query-transaction-serialization-test.cpp)
target_link_libraries(query-transaction-serialization-test ultraverse Catch2::Catch2WithMain)

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

add_executable(procmatcher-trace-test procmatcher-trace-test.cpp)
target_link_libraries(procmatcher-trace-test ultraverse Catch2::Catch2WithMain)

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

set(ULTRAVERSE_TEST_TARGETS
    statecluster-test
    rowgraph-test
    relationship-resolver-test
    statehash-test
    ultraverseconfig-test
    keycolumnparser-test
    stateitem-test
    query-transaction-serialization-test
    sqlparser-test
    tabledependencygraph-test
    naminghistory-test
    rowcluster-test
    taintanalyzer-test
    queryeventbase-rwset-test
    procmatcher-trace-test
    statechanger-test
)

foreach(ultrav_target IN LISTS ULTRAVERSE_TEST_TARGETS)
    ultraverse_set_rpath(${ultrav_target})
    ultraverse_copy_ultparser(${ultrav_target})
    ultraverse_add_homebrew_link_dir(${ultrav_target})
endforeach()

add_test(NAME statecluster-test COMMAND statecluster-test)
add_test(NAME rowgraph-test COMMAND rowgraph-test)
add_test(NAME relationship-resolver-test COMMAND relationship-resolver-test)
add_test(NAME statehash-test COMMAND statehash-test)
add_test(NAME ultraverseconfig-test COMMAND ultraverseconfig-test)
add_test(NAME keycolumnparser-test COMMAND keycolumnparser-test)
add_test(NAME stateitem-test COMMAND stateitem-test)
add_test(NAME query-transaction-serialization-test COMMAND query-transaction-serialization-test)
add_test(NAME sqlparser-test COMMAND sqlparser-test)
add_test(NAME tabledependencygraph-test COMMAND tabledependencygraph-test)
add_test(NAME naminghistory-test COMMAND naminghistory-test)
add_test(NAME rowcluster-test COMMAND rowcluster-test)
add_test(NAME taintanalyzer-test COMMAND taintanalyzer-test)
add_test(NAME queryeventbase-rwset-test COMMAND queryeventbase-rwset-test)
add_test(NAME procmatcher-trace-test COMMAND procmatcher-trace-test)
add_test(NAME statechanger-test COMMAND statechanger-test)

add_custom_target(allTests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -C $<CONFIG>
    DEPENDS ${ULTRAVERSE_TEST_TARGETS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
