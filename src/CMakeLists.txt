set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig REQUIRED)
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

pkg_check_modules(MARIADB REQUIRED mariadb)
pkg_check_modules(FMT REQUIRED fmt)
pkg_check_modules(SPDLOG REQUIRED spdlog)

add_compile_definitions(SPDLOG_FMT_EXTERNAL)

set(ULTRAVERSE_DEPENDENCIES
    ${MARIADB_LIBRARIES}
    ${FMT_LIBRARIES}
    ${SPDLOG_LIBRARIES}
    
    sqlparser
)

set(ULTRAVERSE_SRCS
    Application.cpp
    
    base/DBEvent.cpp
    base/DBHandlePool.cpp
    base/DBHandlePool.hpp
    
    mariadb/BinaryLog.cpp
    mariadb/DBHandle.cpp
    mariadb/DBEvent.cpp
    
    mariadb/state/StateThreadPool.cpp
    mariadb/state/StateThreadPool.h

    mariadb/state/StateTable.cc
    mariadb/state/StateTable.h
    
    mariadb/state/StateQuery.hpp
    
    mariadb/state/StateUserQuery.cc
    mariadb/state/StateUserQuery.h
    
    mariadb/state/StateItem.cc
    mariadb/state/StateItem.h
    
    mariadb/state/StateForeign.cc
    mariadb/state/StateForeign.h
    
    mariadb/state/StateTaskContext.cpp
    mariadb/state/StateTaskContext.hpp
    
    utils/log.cpp
    utils/log.hpp
    mariadb/binlog/BinaryLogReader.cpp
    mariadb/binlog/BinaryLogReader.hpp
    mariadb/binlog/BinaryLogReader.cpp
    mariadb/binlog/BinaryLogReader.hpp
    mariadb/binlog/BinaryLogEvents.hpp
    mariadb/state/StateHash.cpp
    mariadb/state/StateHash.hpp
    
    mariadb/state/new/Query.cpp
    mariadb/state/new/Query.hpp
    mariadb/state/new/Transaction.cpp
    mariadb/state/new/Transaction.hpp
    )

include_directories(../dependencies/sql-parser/src)
include_directories(../dependencies/sql-parser/src/parser)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../dependencies/sql-parser)


include_directories(${CMAKE_CURRENT_SOURCE_DIR})


add_executable(state_log_viewer ${ULTRAVERSE_SRCS} state_log_viewer.cpp)
add_executable(state_log_writer ${ULTRAVERSE_SRCS} state_log_writer.cpp)
add_executable(db_state_change ${ULTRAVERSE_SRCS} db_state_change.cpp)

target_link_libraries(state_log_viewer ${ULTRAVERSE_DEPENDENCIES})
target_link_libraries(state_log_writer ${ULTRAVERSE_DEPENDENCIES})
target_link_libraries(db_state_change ${ULTRAVERSE_DEPENDENCIES})
