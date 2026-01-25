package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"sync"
	"sync/atomic"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"parserlib/parser"
)

// Handle management for parser instances
var (
	parserHandles   sync.Map // map[uintptr]*parser.Parser
	nextParserHandle uintptr
)

// getNextHandle returns a unique handle for a new parser instance.
func getNextHandle() uintptr {
	return uintptr(atomic.AddUintptr(&nextParserHandle, 1))
}

// protobufToCStr marshals a protobuf message to a C string.
// Returns (C string pointer, length). Caller must free the C string.
func protobufToCStr(message proto.Message) (*C.char, int64) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, 0
	}
	return (*C.char)(C.CBytes(data)), int64(len(data))
}

// ==================== NEW INSTANCE-BASED API ====================

// ult_sql_parser_create creates a new parser instance and returns a handle.
//
//export ult_sql_parser_create
func ult_sql_parser_create() C.uintptr_t {
	p := parser.New()
	handle := getNextHandle()
	parserHandles.Store(handle, p)
	return C.uintptr_t(handle)
}

// ult_sql_parser_destroy destroys a parser instance.
//
//export ult_sql_parser_destroy
func ult_sql_parser_destroy(handle C.uintptr_t) {
	parserHandles.Delete(uintptr(handle))
}

// ult_sql_parse_new parses SQL using the new instance-based API.
// Returns the length of the serialized protobuf, or negative on error.
//
//export ult_sql_parse_new
func ult_sql_parse_new(
	handle C.uintptr_t,
	sqlCStr *C.char,
	sqlLen C.int64_t,
	outStmts **C.char,
) C.int64_t {
	p, ok := parserHandles.Load(uintptr(handle))
	if !ok {
		return -1
	}

	sql := C.GoStringN(sqlCStr, C.int(sqlLen))
	result := p.(*parser.Parser).Parse(sql)

	cstr, size := protobufToCStr(result)
	*outStmts = cstr
	return C.int64_t(size)
}

// ult_query_hash_new computes a SHA1 hash of the normalized SQL.
// out_hash must be a pre-allocated 20-byte buffer.
// Returns 20 on success, negative on error.
//
//export ult_query_hash_new
func ult_query_hash_new(
	handle C.uintptr_t,
	sqlCStr *C.char,
	sqlLen C.int64_t,
	outHash *C.char,
) C.int64_t {
	p, ok := parserHandles.Load(uintptr(handle))
	if !ok {
		return -1
	}

	sql := C.GoStringN(sqlCStr, C.int(sqlLen))
	hash, err := p.(*parser.Parser).Hash(sql)
	if err != nil || hash == nil {
		return -2
	}

	// Copy hash to output buffer
	hashSlice := (*[20]byte)(unsafe.Pointer(outHash))
	copy(hashSlice[:], hash)
	return 20
}

// ult_parse_jsonify_new converts parsed SQL to JSON for debugging.
//
//export ult_parse_jsonify_new
func ult_parse_jsonify_new(
	handle C.uintptr_t,
	sqlCStr *C.char,
	sqlLen C.int64_t,
	outJSON **C.char,
) C.int64_t {
	p, ok := parserHandles.Load(uintptr(handle))
	if !ok {
		return -1
	}

	sql := C.GoStringN(sqlCStr, C.int(sqlLen))
	jsonBytes, err := p.(*parser.Parser).Jsonify(sql)
	if err != nil || jsonBytes == nil {
		*outJSON = nil
		return 0
	}

	*outJSON = C.CString(string(jsonBytes))
	return C.int64_t(len(jsonBytes))
}

func main() {
	// Required for cgo shared library
}
