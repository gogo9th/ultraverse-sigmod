set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig REQUIRED)
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED)

# pkg_check_modules(JEMALLOC REQUIRED jemalloc)
pkg_check_modules(MARIADB REQUIRED mariadb)
pkg_check_modules(FMT REQUIRED fmt)
pkg_check_modules(SPDLOG REQUIRED spdlog)
pkg_check_modules(GRAPHVIZ REQUIRED libgvc)
pkg_check_modules(PYTHON3 REQUIRED python3)

add_compile_definitions(SPDLOG_FMT_EXTERNAL)
# add_compile_options(-msse4.2 -O2)
# add_link_options(-fsanitize=leak)

set(ULTRAVERSE_DEPENDENCIES
    ${BOOST_LIBRARIES}
    ${MARIADB_LIBRARIES}
    ${FMT_LIBRARIES}
    ${SPDLOG_LIBRARIES}
    ${Boost_LIBRARIES}
    ${GRAPHVIZ_LIBRARIES}
    python3
    
    sqlparser
    OpenSSL::SSL
)

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
    
    sql/PySQLParser.cpp
    sql/PySQLParser.hpp
)

set(LIBULTRAVERSE_STATE_SRCS
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
    mariadb/state/StateHash.cpp
    mariadb/state/StateHash.hpp
    
    mariadb/state/new/Query.cpp
    mariadb/state/new/Query.hpp
    mariadb/state/new/Transaction.cpp
    mariadb/state/new/Transaction.hpp
    
    mariadb/state/StateGraphBoost.cc
    
    mariadb/state/new/StateLogWriter.cpp
    mariadb/state/new/StateLogWriter.hpp
    
    mariadb/state/new/StateLogReader.cpp
    mariadb/state/new/StateLogReader.hpp
    
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
)

set(LIBULTRAVERSE_MARIADB_SRCS
    mariadb/BinaryLog.cpp
    mariadb/DBHandle.cpp
    mariadb/DBEvent.cpp
    
    mariadb/binlog/MariaDBBinaryLogReader.cpp
    mariadb/binlog/MariaDBBinaryLogReader.hpp
    mariadb/binlog/MySQLBinaryLogReader.cpp
    mariadb/binlog/MySQLBinaryLogReader.hpp
    mariadb/binlog/BinaryLogEvents.hpp
    mariadb/binlog/BinaryLogReader.hpp
    mariadb/binlog/BinaryLogReader.cpp
    mariadb/binlog/BinaryLogSequentialReader.hpp
    mariadb/binlog/BinaryLogSequentialReader.cpp
)


set(LIBULTRAVERSE_SRCS
    ${LIBULTRAVERSE_BASE_SRCS}
    ${LIBULTRAVERSE_STATE_SRCS}
    ${LIBULTRAVERSE_MARIADB_SRCS}
)

set(ULTRAVERSE_APP_BASE_SRCS
    Application.cpp
)

include_directories(../dependencies/sql-parser/src)
include_directories(../dependencies/sql-parser/src/parser)
include_directories(../dependencies/nlohmann-json/include)
include_directories(../dependencies/cereal/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../dependencies/sql-parser)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${PYTHON3_INCLUDE_DIRS})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(ultraverse SHARED ${LIBULTRAVERSE_SRCS})

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
    mariadb/state/new/StateChangeContext.hpp
    db_state_change.cpp
)

add_executable(state_log_viewer ${STATE_LOG_VIEWER_SRCS})
add_executable(statelogd ${STATELOGD_SRCS})
add_executable(db_state_change ${DB_STATE_CHANGE_SRCS})

target_link_libraries(state_log_viewer ultraverse ${ULTRAVERSE_DEPENDENCIES})
target_link_libraries(statelogd ultraverse ${ULTRAVERSE_DEPENDENCIES})
target_link_libraries(db_state_change ultraverse ${ULTRAVERSE_DEPENDENCIES})
