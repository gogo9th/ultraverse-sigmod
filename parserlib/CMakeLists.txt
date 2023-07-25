project(ultparserlib)


set(ULTPARSERLIB_GO_SRCS
    ultparserlib.go
)

set(ULTPARSERLIB_PROTOBUF_SRCS
    ultparser_query.proto
)

set(ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS
    cpp_autogen/ultparser_query.pb.cc
    cpp_autogen/ultparser_query.pb.h
)

set(ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS
    pb/ultparser_query.pb.go
)

set(ULTPARSERLIB_SONAME cpp/libultparser/libultparser.so)

add_custom_command(
    OUTPUT ${ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS}
    DEPENDS ${ULTPARSERLIB_PROTOBUF_SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND mkdir -p cpp_autogen && protoc --experimental_allow_proto3_optional --cpp_out=cpp_autogen ${ULTPARSERLIB_PROTOBUF_SRCS}
    COMMENT "Generating protobuf cpp autogen files"
)

add_custom_command(
    OUTPUT ${ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS}
    DEPENDS ${ULTPARSERLIB_PROTOBUF_SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND mkdir -p pb && protoc --experimental_allow_proto3_optional --go_out=pb ${ULTPARSERLIB_PROTOBUF_SRCS}
    COMMENT "Generating protobuf go autogen files"
)

add_custom_command(
    OUTPUT ${ULTPARSERLIB_SONAME}
    DEPENDS ${ULTPARSERLIB_PROTOBUF_GO_AUTOGEN_SRCS} ${ULTPARSERLIB_GO_SRCS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND mkdir -p cpp/parserlib && env go build -o ${CMAKE_CURRENT_BINARY_DIR}/${ULTPARSERLIB_SONAME} -buildmode=c-shared ${ULTPARSERLIB_GO_SRCS}
    COMMENT "Building ultparserlib.so"
)

add_custom_target(
    ultparserlib ALL
    DEPENDS ${ULTPARSERLIB_PROTOBUF_CPP_AUTOGEN_SRCS} ${ULTPARSERLIB_SONAME}
    SOURCES ${ULTPARSERLIB_GO_SRCS} ${ULTPARSERLIB_PROTOBUF_SRCS}
)
