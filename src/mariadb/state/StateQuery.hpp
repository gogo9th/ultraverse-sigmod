#ifndef STATE_QUERY_INCLUDED
#define STATE_QUERY_INCLUDED

#include <string>
#include <vector>
#include <chrono>
#include <openssl/md5.h>

#include "state_log_hdr.h"
#include "StateUtil.h"
#include "StateItem.h"
#include "StateForeign.h"
#include "StateReference.hpp"

namespace ultraverse::state {
    enum EN_USER_QUERY_TYPE : uint16_t {
        EN_USER_QUERY_NONE = 0, // 일반 쿼리
        EN_USER_QUERY_LOG,      // 상태 전환 도구로 추가됐던(Logging 된) 사용자 쿼리
        EN_USER_QUERY_CURRENT,  // 상태 전환 도구로 추가하려는 사용자 쿼리
    };
    
    class StateQuery {
    public:
        StateQuery()
            : //time({0, 0}),
            user_query(EN_USER_QUERY_NONE),
            is_failed(0),
            is_valid_query(0),
            // command(0),
            hash(0),
            xid(0),
            elapsed_seconds(0),
            is_range_valid(false),
            is_range_update(false),
            next(NULL) {
        }
        
        StateQuery &operator=(const StateQuery &q) {
            user_query = q.user_query;
            is_failed = q.is_failed;
            is_valid_query = q.is_valid_query;
            xid = q.xid;
            
            return *this;
        }
        
        bool AddReference(StateQuery *q) {
            auto iter = std::find_if(ref_list.begin(), ref_list.end(), [&q](const StateReference *r) {
                if (r == &q->ref)
                    return true;
                else
                    return false;
            });
            
            if (ref_list.end() == iter) {
                ref_list.push_back(&q->ref);
                return true;
            }
            return false;
        }
        
        void SetNext(StateQuery *q) { next = q; }
        
        size_t GetRefSize() { return ref_list.size(); }
        
        StateQuery *GetNextPtr() {
            return next;
        }
        
        StateQuery *GetNextNoti() {
            for (auto &q: ref_list) {
                q->Notify();
            }
            
            return next;
        }
        
        void MakeUpData() {
            /*
            MD5_CTX md5_ctx;
            MD5_Init(&md5_ctx);
            
            for (auto &i: write_set) {
                MD5_Update(&md5_ctx, i.c_str(), i.size());
            }
            // for (auto &t : read_set)
            // {
            //   MD5_Update(&md5_ctx, t.c_str(), t.size());
            // }
            
            unsigned char digest[MD5_DIGEST_LENGTH] = {
                0,
            };
            MD5_Final(digest, &md5_ctx);
            memcpy(&hash, digest, sizeof(hash));
             */
        }
        
        EN_USER_QUERY_TYPE user_query;
        uint16_t is_failed;
        uint16_t is_valid_query;
        uint64_t hash;
        uint64_t xid;
        
        struct st_transaction {
            st_transaction() : command(SQLCOM_END) {
            }
            
            st_transaction(const state_log_time &time, uint16_t command, const std::string &query)
                : time(time), command(command), query(query) {
            }
            
            state_log_time time;
            uint16_t command;
            std::string query;
            std::vector<std::string> read_set;
            std::vector<std::string> write_set;
            std::vector<StateForeign> foreign_set;
            std::vector<StateRange> range;
            
            // for candidate
            std::vector<StateItem> item_set;
            std::vector<StateItem> where_set;
        };
        
        std::vector<st_transaction> transactions;
        
        std::vector<std::string> read_set;
        std::vector<std::string> write_set;
        std::vector<StateForeign> foreign_set;
        std::vector<StateRange> range;
        StateReference ref; // referenced info
        std::chrono::duration<double> elapsed_seconds;
        
        bool is_range_valid;
        bool is_range_update;
    
    private:
        StateQuery *next;
        std::vector<StateReference *> ref_list; // reference info list
    };
}

#endif /* STATE_QUERY_INCLUDED */
