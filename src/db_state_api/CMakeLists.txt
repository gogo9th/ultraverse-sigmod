cmake_minimum_required (VERSION 2.6.0)

project(db_state_api)
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
set(MYSQL_DIR ../)
set(MYSQL_LIBRARIES ${MYSQL_DIR}/libmariadb/libmariadb/libmariadb.so) 

MESSAGE ( MYSQL_LIBRARIES "MYSQL LIB : " ${MYSQL_LIBRARIES} )

#db_query_prepar
add_library(db_state_api
  db_state_api.cpp
)

TARGET_INCLUDE_DIRECTORIES(db_state_api PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${MYSQL_DIR}/include
  ${MYSQL_DIR}/libmariadb/include
)

TARGET_LINK_LIBRARIES(db_state_api
  ${MYSQL_LIBRARIES}
)

