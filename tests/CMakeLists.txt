include(CTest)
include(FetchContent)
set(CMAKE_CXX_STANDARD 20)

add_compile_options(-O2)
add_compile_definitions(SPDLOG_FMT_EXTERNAL)

find_package(Boost REQUIRED)

include_directories(../dependencies/sql-parser/src)
include_directories(../dependencies/sql-parser/src/parser)
include_directories(../dependencies/nlohmann-json/include)
include_directories(../dependencies/cereal/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../dependencies/sql-parser)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${PYTHON3_INCLUDE_DIRS})

include_directories(../src)

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.12.0
)
FetchContent_MakeAvailable(Catch2)

add_executable(cluster-test cluster-test.cpp)
target_compile_definitions(cluster-test PRIVATE CATCH_CONFIG_MAIN)

target_link_libraries(cluster-test ultraverse Catch2::Catch2WithMain)

if (APPLE)
    target_link_options(cluster-test PRIVATE "-L/opt/homebrew/lib")
endif()


add_test(NAME cluster-test COMMAND cluster-test)
