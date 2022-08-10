set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig REQUIRED)
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

pkg_check_modules(MARIADB REQUIRED mariadb)
pkg_check_modules(FMT REQUIRED fmt)

set(ULTRAVERSE_DEPENDENCIES
    ${MARIADB_LIBRARIES}
    ${FMT_LIBRARIES}
    
    sqlparser
)

set(ULTRAVERSE_SRCS
    Application.cpp
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
)

include_directories(../dependencies/sql-parser/src)
include_directories(../dependencies/sql-parser/src/parser)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../dependencies/sql-parser)


include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_executable(state_log_viewer ${ULTRAVERSE_SRCS} state_log_viewer.cpp)

target_link_libraries(state_log_viewer ${ULTRAVERSE_DEPENDENCIES})