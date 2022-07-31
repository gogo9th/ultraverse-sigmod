cmake_minimum_required (VERSION 2.6.0)

project(procedure_test)
add_definitions("-DRTI_UNIX -DRTI_LINUX -DRTI_64BIT")

set(FLAGS_RELEASE "-O3 -Wall")
set(FLAGS_DEBUG "-O0 -g -Wall")
set(FLAGS_COMMON "-pthread -std=c++11 -m64 -static-libgcc -Wl,--no-as-needed -ldl")

set(SOURCE_DIR ${PROJECT_SOURCE_DIR}/sources)
set(LIB_DIR ${SOURCE_DIR}/lib)

string(REGEX MATCH "[dD]ebug" IS_DEBUG ${PROJECT_BINARY_DIR})
if(IS_DEBUG)
  set(CMAKE_BUILD_TYPE "Debug")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${FLAGS_DEBUG} ${FLAGS_COMMON}")
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${FLAGS_DEBUG} ${FLAGS_COMMON}")
else(IS_DEBUG)
  set(CMAKE_BUILD_TYPE "Release")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${FLAGS_RELEASE} ${FLAGS_COMMON}")
  set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${FLAGS_RELEASE} ${FLAGS_COMMON}")
endif(IS_DEBUG)

message(STATUS "Binary Build Type is ${CMAKE_BUILD_TYPE}")

# MySQL library
set(MYSQL_DIR ../../libmariadb)
get_filename_component(MYSQL_DIR ${MYSQL_DIR} ABSOLUTE)

set(MYSQL_INCLUDE_DIR ${MYSQL_DIR}/include)
set(MYSQL_LIBRARY_DIR ${MYSQL_DIR}/libmariadb)
set(MYSQL_LIBRARIES ${MYSQL_DIR}/libmariadb/libmariadb.so) 

MESSAGE ( MYSQL_LIBRARIES "MYSQL LIB : " ${MYSQL_LIBRARIES} )

add_executable(procedure_test
  procedure_test.cpp
  db_mysql.cpp
)

TARGET_INCLUDE_DIRECTORIES(procedure_test PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${MYSQL_DIR}
  ${MYSQL_INCLUDE_DIR}
  ${MYSQL_LIBRARY_DIR}
)

TARGET_LINK_LIBRARIES(procedure_test
  ${MYSQL_LIBRARIES}
)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/procedure_test.ini DESTINATION ${PROJECT_BINARY_DIR})
