//
// Created by cheesekun on 8/12/22.
//

#ifndef ULTRAVERSE_BINARYLOGEVENTS_HPP
#define ULTRAVERSE_BINARYLOGEVENTS_HPP

#include <cstdint>

#define PACKED_STRUCT __attribute__ ((packed))

namespace ultraverse::mariadb::internal {
    
    using encint4 = uint32_t;
    using encint2 = uint16_t;
    using encint1 = uint8_t;
    
    using int8 = uint64_t;
    using int4 = uint32_t;
    using int2 = uint16_t;
    using int1 = uint8_t;
    
    using hack_eofchar = uint8_t;
    
    enum EventType: int1 {
        UNKNOWN_EVENT= 0,
        START_EVENT_V3= 1,
        QUERY_EVENT= 2,
        STOP_EVENT= 3,
        ROTATE_EVENT= 4,
        INTVAR_EVENT= 5,
        LOAD_EVENT= 6,
        SLAVE_EVENT= 7,
        CREATE_FILE_EVENT= 8,
        APPEND_BLOCK_EVENT= 9,
        EXEC_LOAD_EVENT= 10,
        DELETE_FILE_EVENT= 11,
        NEW_LOAD_EVENT= 12,
        RAND_EVENT= 13,
        USER_VAR_EVENT= 14,
        FORMAT_DESCRIPTION_EVENT= 15,
        XID_EVENT= 16,
        BEGIN_LOAD_QUERY_EVENT= 17,
        EXECUTE_LOAD_QUERY_EVENT= 18,
        TABLE_MAP_EVENT = 19,
    
        PRE_GA_WRITE_ROWS_EVENT = 20, /* deprecated */
        PRE_GA_UPDATE_ROWS_EVENT = 21, /* deprecated */
        PRE_GA_DELETE_ROWS_EVENT = 22, /* deprecated */
    
        WRITE_ROWS_EVENT_V1 = 23,
        UPDATE_ROWS_EVENT_V1 = 24,
        DELETE_ROWS_EVENT_V1 = 25,
        INCIDENT_EVENT= 26,
        HEARTBEAT_LOG_EVENT= 27,
        IGNORABLE_LOG_EVENT= 28,
        ROWS_QUERY_LOG_EVENT= 29,
        WRITE_ROWS_EVENT = 30,
        UPDATE_ROWS_EVENT = 31,
        DELETE_ROWS_EVENT = 32,
        GTID_LOG_EVENT= 33,
        ANONYMOUS_GTID_LOG_EVENT= 34,
        PREVIOUS_GTIDS_LOG_EVENT= 35,
        TRANSACTION_CONTEXT_EVENT= 36,
        VIEW_CHANGE_EVENT= 37,
        XA_PREPARE_LOG_EVENT= 38,
    };
    
    struct EventHeader {
        int4 timestamp;
        int1 event_type;
        int4 server_id;
        int4 event_size;
        
        // if binlog-version > 1
        /** position of next event */
        int4 log_pos;
        int2 flags;
    } PACKED_STRUCT;
    
    struct FormatDescriptionEvent {
        int2 binlog_version;
        char mysql_server_version[50];
        int4 create_timestamp;
        int1 event_header_length;
        
        // 이거 끝에 체크섬 여부 있음
        // int1 checksum_algorithm;
    } PACKED_STRUCT;
    
    struct RotateEvent {
        int8 position;
        hack_eofchar binlog[255];
    } PACKED_STRUCT;
    
    struct QueryEventPostHeader {
        int4 slave_proxy_id;
        int4 execution_time;
        int1 schema_length;
        int2 error_code;
        
        // if binlog_version >= 4
        int2 status_vars_length;
    } PACKED_STRUCT;
    
    struct XIDEvent {
        int8 xid;
    } PACKED_STRUCT;
    
}

#endif //ULTRAVERSE_BINARYLOGEVENTS_HPP
