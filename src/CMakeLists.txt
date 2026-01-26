set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "Use external fmt library" FORCE)
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

Include(FetchContent)

FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG        12.1.0
)

FetchContent_Declare(
        spdlog
        GIT_REPOSITORY https://github.com/gabime/spdlog.git
        GIT_TAG        v1.16.0
)

FetchContent_MakeAvailable(fmt)
FetchContent_MakeAvailable(spdlog)

find_package(PkgConfig REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED)

# pkg_check_modules(JEMALLOC REQUIRED jemalloc)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
pkg_check_modules(MYSQL REQUIRED mysqlclient)

include(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/mysql_binlogevents.cmake)

find_program(PROTOC_EXECUTABLE protoc)
if (NOT PROTOC_EXECUTABLE)
    message(FATAL_ERROR "protoc not found (required for ultraverse_state.proto)")
endif()

set(ULTRAVERSE_STATE_PROTO_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/mariadb/state/new/proto
)
set(ULTRAVERSE_STATE_PROTO
    ${ULTRAVERSE_STATE_PROTO_DIR}/ultraverse_state.proto
)
set(ULTRAVERSE_STATE_PROTO_OUT
    ${CMAKE_CURRENT_BINARY_DIR}/proto_autogen
)

file(MAKE_DIRECTORY ${ULTRAVERSE_STATE_PROTO_OUT})

set(ULTRAVERSE_STATE_PROTO_SRCS
    ${ULTRAVERSE_STATE_PROTO_OUT}/ultraverse_state.pb.cc
)
set(ULTRAVERSE_STATE_PROTO_HDRS
    ${ULTRAVERSE_STATE_PROTO_OUT}/ultraverse_state.pb.h
)

add_custom_command(
    OUTPUT ${ULTRAVERSE_STATE_PROTO_SRCS} ${ULTRAVERSE_STATE_PROTO_HDRS}
    COMMAND ${PROTOC_EXECUTABLE}
        --cpp_out=${ULTRAVERSE_STATE_PROTO_OUT}
        -I ${ULTRAVERSE_STATE_PROTO_DIR}
        ${ULTRAVERSE_STATE_PROTO}
    DEPENDS ${ULTRAVERSE_STATE_PROTO}
    COMMENT "Generating protobuf for ultraverse state serialization"
)

add_custom_target(ultraverse_state_proto
    DEPENDS ${ULTRAVERSE_STATE_PROTO_SRCS} ${ULTRAVERSE_STATE_PROTO_HDRS}
)


# add_compile_options(-march=native -O2)

# add_link_options(-fsanitize=leak)

set(ULTRAVERSE_DEPENDENCIES
    ${BOOST_LIBRARIES}
    ${MYSQL_LIBRARIES}
    fmt::fmt spdlog::spdlog
    ${Boost_LIBRARIES}
    
    ${PROTOBUF_LIBRARIES}
    
    OpenSSL::SSL
    
    tbb
    mysql_binlog_event_standalone
)

set(ULTPARSERLIB_SO_PATH
    ${CMAKE_CURRENT_BINARY_DIR}/../parserlib/cpp/libultparser/libultparser.so
)

list(APPEND ULTRAVERSE_DEPENDENCIES ${ULTPARSERLIB_SO_PATH})

set(LIBULTRAVERSE_BASE_SRCS
    
    base/DBEvent.cpp
    base/DBHandlePool.cpp
    base/DBHandlePool.hpp
    
    base/TaskExecutor.cpp
    base/TaskExecutor.hpp
    
    utils/log.cpp
    utils/log.hpp
    utils/StringUtil.cpp
    utils/StringUtil.hpp
    
    config/UltraverseConfig.cpp
    config/UltraverseConfig.hpp

    mariadb/state/new/CombinedIterator.cpp 
    mariadb/state/new/CombinedIterator.hpp
    
    ${CMAKE_CURRENT_SOURCE_DIR}/../parserlib/cpp_autogen/ultparser_query.pb.cc
    ${ULTRAVERSE_STATE_PROTO_SRCS}
)

set(LIBULTRAVERSE_STATE_SRCS
    
    mariadb/state/StateItem.cc
    mariadb/state/StateItem.h
    mariadb/state/WhereClauseBuilder.cpp
    mariadb/state/WhereClauseBuilder.hpp
    
    mariadb/state/StateHash.cpp
    mariadb/state/StateHash.hpp
    
    mariadb/state/new/Query.cpp
    mariadb/state/new/Query.hpp
    mariadb/state/new/Transaction.cpp
    mariadb/state/new/Transaction.hpp
    mariadb/state/new/analysis/TaintAnalyzer.cpp
    mariadb/state/new/analysis/TaintAnalyzer.hpp
    
    mariadb/state/new/StateLogWriter.cpp
    mariadb/state/new/StateLogWriter.hpp
    
    mariadb/state/new/StateLogReader.cpp
    mariadb/state/new/StateLogReader.hpp

    mariadb/state/new/StateIO.cpp
    mariadb/state/new/StateIO.hpp
    
    mariadb/state/new/cluster/CandidateColumn.hpp
    
    mariadb/state/new/cluster/RowCluster.hpp
    mariadb/state/new/cluster/RowCluster.cpp
    mariadb/state/new/cluster/NamingHistory.cpp
    mariadb/state/new/cluster/NamingHistory.hpp
    mariadb/state/new/ColumnDependencyGraph.cpp
    mariadb/state/new/ColumnDependencyGraph.hpp
    mariadb/state/new/StateChangePlan.cpp
    mariadb/state/new/StateChangePlan.hpp
    mariadb/state/new/HashWatcher.cpp
    mariadb/state/new/HashWatcher.hpp
    mariadb/state/new/TableDependencyGraph.cpp
    mariadb/state/new/TableDependencyGraph.hpp
    mariadb/state/new/StateChanger.sqlload.cpp
    
    mariadb/state/new/GIDIndexWriter.cpp
    mariadb/state/new/GIDIndexWriter.hpp
    mariadb/state/new/GIDIndexReader.cpp
    mariadb/state/new/GIDIndexReader.hpp
    
    mariadb/state/new/ProcCall.cpp
    mariadb/state/new/ProcLogReader.cpp
    mariadb/state/new/ProcMatcher.cpp
    mariadb/state/new/cluster/StateRelationshipResolver.cpp mariadb/state/new/cluster/StateRelationshipResolver.hpp
    mariadb/state/new/graph/RowGraph.cpp
    mariadb/state/new/graph/RowGraph.hpp
    mariadb/state/new/StateClusterWriter.cpp mariadb/state/new/StateClusterWriter.hpp mariadb/state/new/StateChangeReport.cpp mariadb/state/new/StateChangeReport.hpp)

set(LIBULTRAVERSE_MARIADB_SRCS
    mariadb/DBHandle.cpp
    mariadb/DBEvent.cpp
    
    mariadb/binlog/MySQLBinaryLogReaderV2.cpp
    mariadb/binlog/MySQLBinaryLogReaderV2.hpp
    mariadb/binlog/BinaryLogEvents.hpp
    mariadb/binlog/BinaryLogReader.hpp
    mariadb/binlog/BinaryLogReader.cpp
    mariadb/binlog/BinaryLogSequentialReader.hpp
    mariadb/binlog/BinaryLogSequentialReader.cpp
    mariadb/state/new/cluster/StateCluster.cpp mariadb/state/new/cluster/StateCluster.hpp)


set(LIBULTRAVERSE_SRCS
    ${LIBULTRAVERSE_BASE_SRCS}
    ${LIBULTRAVERSE_STATE_SRCS}
    ${LIBULTRAVERSE_MARIADB_SRCS}
)

set(ULTRAVERSE_APP_BASE_SRCS
    Application.cpp
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../parserlib/cpp_autogen)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../parserlib/cpp)
include_directories(../dependencies/nlohmann-json/include)
include_directories(${ULTRAVERSE_STATE_PROTO_OUT})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${MYSQL_INCLUDE_DIRS})
include_directories(${PYTHON3_INCLUDE_DIRS})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if (APPLE)
    set(ULTRAVERSE_RPATH_ORIGIN "@loader_path")
else()
    set(ULTRAVERSE_RPATH_ORIGIN "$ORIGIN")
endif()

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

function(ultraverse_fix_ultparser_load target_name)
    if (APPLE)
        add_custom_command(
            TARGET ${target_name} POST_BUILD
            COMMAND install_name_tool -change "libultparser.so" "@rpath/libultparser.so" "$<TARGET_FILE:${target_name}>"
            COMMENT "Fixing libultparser load path for ${target_name}"
        )
    endif()
endfunction()

function(ultraverse_add_homebrew_link_dir target_name)
    if (APPLE)
        target_link_options(${target_name} PRIVATE "-L/opt/homebrew/lib")
    endif()
endfunction()

add_library(ultraverse SHARED ${LIBULTRAVERSE_SRCS})
add_dependencies(ultraverse ultparserlib ultraverse_state_proto)

target_link_libraries(ultraverse ${ULTRAVERSE_DEPENDENCIES})

set(STATE_LOG_VIEWER_SRCS
    ${ULTRAVERSE_APP_BASE_SRCS}
    state_log_viewer.cpp
)

set(STATELOGD_SRCS
    ${ULTRAVERSE_APP_BASE_SRCS}
    statelogd.cpp
)

set(DB_STATE_CHANGE_SRCS
    ${ULTRAVERSE_APP_BASE_SRCS}
    mariadb/state/new/StateChanger.cpp
    mariadb/state/new/StateChanger.hpp
    mariadb/state/new/StateChanger.prepare.cpp
    mariadb/state/new/StateChanger.replay.cpp
    mariadb/state/new/StateChangeContext.hpp
    db_state_change.cpp
)


add_executable(state_log_viewer ${STATE_LOG_VIEWER_SRCS})
add_executable(statelogd ${STATELOGD_SRCS})
add_executable(db_state_change ${DB_STATE_CHANGE_SRCS})

set(ULTRAVERSE_RUNTIME_TARGETS
    ultraverse
    state_log_viewer
    statelogd
    db_state_change
)

foreach(ultrav_target IN LISTS ULTRAVERSE_RUNTIME_TARGETS)
    ultraverse_set_rpath(${ultrav_target})
    ultraverse_copy_ultparser(${ultrav_target})
    ultraverse_fix_ultparser_load(${ultrav_target})
    ultraverse_add_homebrew_link_dir(${ultrav_target})
endforeach()

target_link_libraries(state_log_viewer ultraverse ${ULTRAVERSE_DEPENDENCIES})
target_link_libraries(statelogd ultraverse ${ULTRAVERSE_DEPENDENCIES})
target_link_libraries(db_state_change ultraverse ${ULTRAVERSE_DEPENDENCIES})