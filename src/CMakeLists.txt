set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig REQUIRED)

pkg_check_modules(MARIADB REQUIRED mariadb)
pkg_check_modules(FMT REQUIRED fmt)


set(ULTRAVERSE_DEPENDENCIES
    ${MARIADB_LIBRARIES}
    ${FMT_LIBRARIES}
)

set(ULTRAVERSE_SRCS
    mariadb/BinaryLog.cpp
    mariadb/DBHandle.cpp
)

add_executable(state_log_viewer ${ULTRAVERSE_SRCS} state_log_viewer.cpp)

target_link_libraries(state_log_viewer ${ULTRAVERSE_DEPENDENCIES})