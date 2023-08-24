# check 'protoc-gen-go' is installed (use 'which protoc-gen-go' to check)
# if not installed, install it using 'go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28'

function(find_protoc_gen_go)
    message($ENV{HOME}/go/bin)
    find_program(
        PROTOC_GEN_GO
            NAMES protoc-gen-go
            HINTS $ENV{HOME}/go/bin ENV PATH ENV GOPATH
    )
    if(NOT PROTOC_GEN_GO)
        message(FATAL_ERROR "protoc-gen-go not found")
        return()
    endif()

    set(PROTOC_GEN_GO_BIN ${PROTOC_GEN_GO} PARENT_SCOPE)
endfunction()