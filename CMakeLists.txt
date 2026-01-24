cmake_minimum_required(VERSION 3.28)
project(ultraverse)

enable_testing()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the build type." FORCE)
endif()

add_compile_definitions(SPDLOG_FMT_EXTERNAL)
add_compile_options(
    "$<$<CONFIG:Debug>:-O0>"
    "$<$<CONFIG:Debug>:-g>"
    "$<$<CONFIG:Release>:-O2>"
)

if (APPLE)
    add_compile_options(
        "$<$<CONFIG:Debug>:-fstandalone-debug>"
        -fexperimental-library
    )
    add_link_options(
        "$<$<CONFIG:Debug>:-fstandalone-debug>"
        -fexperimental-library
    )
endif()

# -fPIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(ULTRAVERSE_MYSQLD_SRC_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/mysql-server"
    CACHE PATH "Path to MySQL source tree (for libbinlogevents)"
)
if (NOT EXISTS "${ULTRAVERSE_MYSQLD_SRC_PATH}/libs/mysql/binlog/event")
    message(FATAL_ERROR "ULTRAVERSE_MYSQLD_SRC_PATH does not contain libs/mysql/binlog/event: ${ULTRAVERSE_MYSQLD_SRC_PATH}")
endif()

add_subdirectory(parserlib)

add_subdirectory(src)
add_subdirectory(dependencies/cereal)

add_subdirectory(tests)
#add_subdirectory(external/Catch2)

# add_subdirectory(tests)
