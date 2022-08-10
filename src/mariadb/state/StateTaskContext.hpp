//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_STATETASKCONTEXT_HPP
#define ULTRAVERSE_STATETASKCONTEXT_HPP

#include "state_log_hdr.h"

namespace ultraverse::state {
    class StateTaskContext {
    public:
        state_log_time &get_start_time();
        state_log_time &get_end_time();
        state_log_time &get_hash_query_time();
        bool is_hash_matched();
        bool is_using_candidate();
        const char *get_key_column_name();
        const char *get_key_column_table_name();
        en_state_log_column_data_type get_key_column_type();
        
    private:
        state_log_time start_time = {0, 0};
        state_log_time end_time = {0, 0};
        state_log_time redo_time = {0, 0};
        state_log_time hash_query_time = {0, 0};
    
        char *key_column_table_name = NULL;
        char *key_column_name = NULL;
        char *key_column_type_str = NULL;
        en_state_log_column_data_type key_column_type = en_column_data_null;
    
        char *candidate_db;
        
        bool b_hash_matched = false;
    };
}


#endif //ULTRAVERSE_STATETASKCONTEXT_HPP
