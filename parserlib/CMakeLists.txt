project(ultparserlib)
include(cmake_scripts/FindProtocGenGo.cmake)

find_protoc_gen_go()

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/cpp_autogen)
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/cpp_autogen)
endif()


if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/pb)
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/pb)
endif()

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/cpp_autogen/ultparser_query.pb.cc)
    file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/cpp_autogen/ultparser_query.pb.cc "/* dummy file */")
endif()

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/ULTPARSERLIB_DEP_INSTALLED
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/go.mod ${CMAKE_CURRENT_SOURCE_DIR}/go.sum
    COMMAND go get
    COMMAND touch ${CMAKE_CURRENT_SOURCE_DIR}/ULTPARSERLIB_DEP_INSTALLED
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Installing dependencies"
)


set(ULTPARSERLIB_GO_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/ultparserlib.go
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

set(ULTPARSERLIB_SONAME ${CMAKE_CURRENT_BINARY_DIR}/cpp/libultparser/libultparser.so)

add_custom_command(
    OUTPUT ${ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS} ${ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS}
    DEPENDS ${ULTPARSERLIB_PROTOBUF_SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND protoc --experimental_allow_proto3_optional --cpp_out=cpp_autogen ultparser_query.proto
    COMMAND protoc --plugin=${PROTOC_GEN_GO_BIN} --experimental_allow_proto3_optional --go_out=pb ultparser_query.proto
    COMMENT "Generating protobuf autogen files for go/cpp"
)

add_custom_command(
    OUTPUT ${ULTPARSERLIB_SONAME}
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/ULTPARSERLIB_DEP_INSTALLED
        ${ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS} ${ULTPARSERLIB_GO_SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND env go build -o ${ULTPARSERLIB_SONAME} -buildmode=c-shared ${ULTPARSERLIB_GO_SRCS}
    COMMENT "Building ultparserlib.so"
)

add_custom_target(
    ultparserlib ALL
    DEPENDS
        ${ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS} ${ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS} ${ULTPARSERLIB_SONAME}
    SOURCES ${ULTPARSERLIB_GO_SRCS} ${ULTPARSERLIB_PROTOBUF_SRCS}
)
