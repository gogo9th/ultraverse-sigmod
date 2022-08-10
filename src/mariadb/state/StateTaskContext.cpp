//
// Created by cheesekun on 8/10/22.
//

#include "StateTaskContext.hpp"

namespace ultraverse::state {
    
    state_log_time &StateTaskContext::get_start_time()
    {
        return start_time;
    }
    
    state_log_time &StateTaskContext::get_end_time()
    {
        return end_time;
    }
    
    state_log_time &StateTaskContext::get_hash_query_time()
    {
        return hash_query_time;
    }
    
    bool StateTaskContext::is_hash_matched()
    {
        return b_hash_matched;
    }
    
    bool StateTaskContext::is_using_candidate()
    {
        return candidate_db == NULL ? false : true;
    }
    
    const char *StateTaskContext::get_key_column_table_name()
    {
        return key_column_table_name;
    }
    
    const char *StateTaskContext::get_key_column_name()
    {
        return key_column_name;
    }
    
    en_state_log_column_data_type StateTaskContext::get_key_column_type()
    {
        return key_column_type;
    }
    
}