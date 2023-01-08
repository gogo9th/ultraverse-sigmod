cmake_minimum_required(VERSION 3.0)
project(ultraverse)

# -fPIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(src)
add_subdirectory(dependencies/cereal)
add_subdirectory(dependencies/sql-parser)
# add_subdirectory(tests)