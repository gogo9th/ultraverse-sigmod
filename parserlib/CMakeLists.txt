project(ultparserlib)
include(cmake_scripts/FindProtocGenGo.cmake)

find_protoc_gen_go()

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/cpp_autogen)
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/cpp_autogen)
endif()


if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/pb)
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/pb)
endif()

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/ULTPARSERLIB_DEP_INSTALLED
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/go.mod ${CMAKE_CURRENT_SOURCE_DIR}/go.sum
    COMMAND go mod download
    COMMAND touch ${CMAKE_CURRENT_SOURCE_DIR}/ULTPARSERLIB_DEP_INSTALLED
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Installing dependencies"
)


set(ULTPARSERLIB_GO_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/capi.go
    ${CMAKE_CURRENT_SOURCE_DIR}/parser/parser.go
    ${CMAKE_CURRENT_SOURCE_DIR}/parser/expr.go
    ${CMAKE_CURRENT_SOURCE_DIR}/parser/stmt.go
    ${CMAKE_CURRENT_SOURCE_DIR}/parser/proc.go
)

set(ULTPARSERLIB_PROTOBUF_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/ultparser_query.proto
)

set(ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp_autogen/ultparser_query.pb.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp_autogen/ultparser_query.pb.h
)

set(ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/pb/ultparser_query.pb.go
)

set_source_files_properties(
    ${ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS}
    PROPERTIES GENERATED TRUE
)

set(ULTPARSERLIB_SONAME ${CMAKE_CURRENT_BINARY_DIR}/cpp/libultparser/libultparser.so)

set(ULTPARSERLIB_POST_BUILD_COMMANDS)
if (APPLE)
    list(APPEND ULTPARSERLIB_POST_BUILD_COMMANDS
        COMMAND install_name_tool -id "@rpath/libultparser.so" ${ULTPARSERLIB_SONAME}
    )
endif()

add_custom_command(
    OUTPUT ${ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS}
    DEPENDS ${ULTPARSERLIB_PROTOBUF_SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND protoc --experimental_allow_proto3_optional --cpp_out=cpp_autogen ultparser_query.proto
    COMMENT "Generating protobuf C++ autogen files"
)

add_custom_command(
    OUTPUT ${ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS}
    DEPENDS ${ULTPARSERLIB_PROTOBUF_SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND protoc --plugin=${PROTOC_GEN_GO_BIN} --experimental_allow_proto3_optional --go_out=pb ultparser_query.proto
    COMMENT "Generating protobuf Go autogen files"
)

add_custom_command(
    OUTPUT ${ULTPARSERLIB_SONAME}
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/ULTPARSERLIB_DEP_INSTALLED
        ${ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS} ${ULTPARSERLIB_GO_SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND env go build -o ${ULTPARSERLIB_SONAME} -buildmode=c-shared .
    ${ULTPARSERLIB_POST_BUILD_COMMANDS}
    COMMENT "Building ultparserlib.so"
)

add_custom_target(
    ultparserlib ALL
    DEPENDS
        ${ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS} ${ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS} ${ULTPARSERLIB_SONAME}
    SOURCES ${ULTPARSERLIB_GO_SRCS} ${ULTPARSERLIB_PROTOBUF_SRCS}
)
