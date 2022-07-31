cmake_minimum_required (VERSION 2.6.0)

project(db_state_test)
add_definitions("-DRTI_UNIX -DRTI_LINUX -DRTI_64BIT")

set(FLAGS_RELEASE "-O3 -std=c++11 -Wall")
set(FLAGS_DEBUG "-O0 -g -std=c++11 -Wall")
set(FLAGS_COMMON "-m64 -static-libgcc -Wl,--no-as-needed -ldl")

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
set(MYSQL_BUILD_DIR ../../build/libmariadb)
get_filename_component(MYSQL_DIR ${MYSQL_DIR} ABSOLUTE)
get_filename_component(MYSQL_BUILD_DIR ${MYSQL_BUILD_DIR} ABSOLUTE)

set(MYSQL_LIBRARIES ${MYSQL_BUILD_DIR}/libmariadb/libmariadb.so) 

MESSAGE ( MYSQL_LIBRARIES "MYSQL LIB : " ${MYSQL_LIBRARIES} )

add_executable(db_state_test
  db_state_test.cpp
  db_mysql.cpp
)

TARGET_INCLUDE_DIRECTORIES(db_state_test PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${MYSQL_DIR}
  ${MYSQL_BUILD_DIR}
  ${MYSQL_DIR}/include
  ${MYSQL_BUILD_DIR}/include
)

TARGET_LINK_LIBRARIES(db_state_test
  ${MYSQL_LIBRARIES}
)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/db_state_test.ini DESTINATION ${PROJECT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/init_mysql.sh DESTINATION ${PROJECT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/user.sql DESTINATION ${PROJECT_BINARY_DIR})

