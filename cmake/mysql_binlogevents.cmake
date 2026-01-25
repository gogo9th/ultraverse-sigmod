set(MYSQL_BINLOGEVENTS_ROOT "${ULTRAVERSE_MYSQLD_SRC_PATH}/libs/mysql")

file(GLOB MYSQL_BINLOGEVENTS_SOURCES CONFIGURE_DEPENDS
    "${MYSQL_BINLOGEVENTS_ROOT}/binlog/event/*.cpp"
    "${MYSQL_BINLOGEVENTS_ROOT}/binlog/event/codecs/*.cpp"
    "${MYSQL_BINLOGEVENTS_ROOT}/binlog/event/compression/*.cpp"
    "${MYSQL_BINLOGEVENTS_ROOT}/serialization/*.cpp"
    "${MYSQL_BINLOGEVENTS_ROOT}/gtid/*.cpp"
    "${ULTRAVERSE_MYSQLD_SRC_PATH}/mysys/pack.cc"
    "${ULTRAVERSE_MYSQLD_SRC_PATH}/mysys/decimal.cc"
)

file(GLOB_RECURSE MYSQL_BINLOGEVENTS_CONTAINER_SOURCES CONFIGURE_DEPENDS
    "${MYSQL_BINLOGEVENTS_ROOT}/containers/*.cpp"
)

list(APPEND MYSQL_BINLOGEVENTS_SOURCES ${MYSQL_BINLOGEVENTS_CONTAINER_SOURCES})

find_package(ZLIB REQUIRED)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd)

add_library(mysql_binlog_event_standalone STATIC ${MYSQL_BINLOGEVENTS_SOURCES})
target_include_directories(mysql_binlog_event_standalone PUBLIC
    "${ULTRAVERSE_MYSQLD_SRC_PATH}/libs"
    "${ULTRAVERSE_MYSQLD_SRC_PATH}/include"
    "${ULTRAVERSE_MYSQLD_SRC_PATH}"
)
target_compile_definitions(mysql_binlog_event_standalone PUBLIC
    STANDALONE_BINLOG
    BINLOG_EVENT_COMPRESSION_USE_ZSTD_system
)
target_link_libraries(mysql_binlog_event_standalone PUBLIC
    ZLIB::ZLIB
    PkgConfig::ZSTD
)
