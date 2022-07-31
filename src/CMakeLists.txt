set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig REQUIRED)

pkg_check_modules(MARIADB REQUIRED mariadb)

set(ULTRAVERSE_DEPENDENCIES
    ${MARIADB_LIBRARIES}
)

set(ULTRAVERSE_SRCS
    main.cpp
)

add_executable(ultraverse ${ULTRAVERSE_SRCS})

target_link_libraries(ultraverse ${ULTRAVERSE_DEPENDENCIES})