//
// Created by cheesekun on 9/13/22.
//

#include <cereal/types/vector.hpp>
#include <cereal/types/memory.hpp>

#include "StateItem.h"

template <typename Archive>
void StateData::save(Archive &archive) const {
    archive(
        is_subselect,
        is_equal,
        type,
        _hash
    );
    
    if (type == en_column_data_int) {
        archive(d.ival);
    } else if (type == en_column_data_uint) {
        archive(d.uval);
    } else if (type == en_column_data_double) {
        archive(d.fval);
    } else if (type == en_column_data_string) {
        std::string strVal(d.str);
        archive(strVal);
    }
}

template <typename Archive>
void StateData::load(Archive &archive) {
    archive(
        is_subselect,
        is_equal,
        type,
        _hash
    );
    
    if (type == en_column_data_int) {
        archive(d.ival);
    } else if (type == en_column_data_uint) {
        archive(d.uval);
    } else if (type == en_column_data_double) {
        archive(d.fval);
    } else if (type == en_column_data_string) {
        std::string strVal;
        archive(strVal);
        d.str = new char[strVal.size() + 1];
        memset(d.str, 0, strVal.size() + 1);
        strncpy(d.str, strVal.c_str(), strVal.size());
    }
}

template <typename Archive>
void StateRange::ST_RANGE::serialize(Archive &archive) {
    archive(
        begin,
        end
    );
}

template <typename Archive>
void StateRange::serialize(Archive &archive) {
    archive(
        range,
        _hash
    );
}

template <typename Archive>
void StateItem::serialize(Archive &archive) {
    archive(
        condition_type,
        function_type,
        name,
        arg_list,
        data_list,
        sub_query_list,
        _rangeCache,
        _isRangeCacheBuilt
    );
}