#include "my_global.h"
#include "mysql.h"
#include "sql_alloc.h"
#include "sql_cmd.h"

#include "StateGraphGvc.h"
#include "StateUtil.h"
#include "StateThreadPool.h"

#include <graphviz/gvc.h>
#include <graphviz/cgraph.h>
#include <graphviz/cdt.h>

#include <algorithm>

namespace ultraverse::state {
    
    StateGraphGvc::StateGraphGvc()
        : StateGraph(), node_count(0) {
        gvc = gvContext();
        g = agopen((char *) "g", Agstrictdirected, NULL);
        agattr(g, AGNODE, (char *) "shape", (char *) "record");
    }
    
    StateGraphGvc::~StateGraphGvc() {
        gvFreeLayout(gvc, g);
        agclose(g);
        gvFreeContext(gvc);
    }
    
    void StateGraphGvc::MakeOutputFilename(const std::string &type, const std::string &filepath) {
        gvLayout(gvc, g, "dot");
        gvRenderFilename(gvc, g, type.c_str(), filepath.c_str());
    }
    
    void StateGraphGvc::MakeOutputFile(const std::string &type, FILE *fp) {
        gvLayout(gvc, g, "dot");
        gvRender(gvc, g, type.c_str(), stdout);
    }
    
    Agnode_s *StateGraphGvc::CreateNode(StateQuery *query, size_t node_idx, std::stringstream &sstream) {
        std::string string_idx = "Q" + std::to_string(node_idx);
        Agnode_t *node = agnode(g, (char *) string_idx.c_str(), 1);
        SetNodeData(node, query);
        
        auto print_vector = [](const std::vector<std::string> &v) -> std::string {
            static const char *delimiter = ", ";
            std::string ret;
            
            for (auto &i: v) {
                ret += i;
                ret += delimiter;
            }
            
            if (ret.size() > 0)
                return ret.substr(0, ret.size() - strlen(delimiter));
            else
                return ret;
        };
        
        auto replace = [](std::string &str, const std::string &from, const std::string &to) -> std::string {
            std::string::size_type start_pos = 0;
            while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
                str.replace(start_pos, from.length(), to);
                start_pos += to.length();
            }
            return str;
        };
        
        sstream.str(std::string());
        sstream << "{xid " << query->xid;
        for (auto &i: query->transactions) {
            std::string convert_query = i.query;
            replace(convert_query, "<", "\\<");
            replace(convert_query, ">", "\\>");
            
            sstream << "|{"
                    << string_idx << '|'
                    << '{'
                    << i.time.sec << '.'
                    << i.time.sec_part
                    << '}' << '|'
                    << '"' << convert_query << '"' << '|'
                    << '{'
                    << print_vector(query->read_set) << '|'
                    << print_vector(query->write_set)
                    << '}'
                    << '}';
        }
        sstream << '}';
        
        agsafeset(node, (char *) "label", (char *) sstream.str().c_str(), (char *) "");
        
        if (query->write_set.size() == 0) {
            agsafeset(node, (char *) "color", (char *) "gray", (char *) "");
        } else if (query->user_query == EN_USER_QUERY_LOG) {
            // 상태 전환 도구로 추가됐던(Logging 된) 사용자 쿼리
            agsafeset(node, (char *) "color", (char *) "blue", (char *) "");
        } else if (query->user_query == EN_USER_QUERY_CURRENT) {
            // 상태 전환 도구로 추가하려는 사용자 쿼리
            agsafeset(node, (char *) "color", (char *) "red", (char *) "");
        }
        
        return node;
    }
    
    struct data_s {
        Agrec_t hdr;
        StateQuery *query;
    };
    
    void StateGraphGvc::SetNodeData(Agnode_t *node, StateQuery *query) {
        struct data_s *data = (struct data_s *) agbindrec(node, (char *) "data", sizeof(struct data_s), FALSE);
        data->query = query;
    }
    
    StateQuery *StateGraphGvc::GetNodeData(Agnode_t *node) {
        return ((struct data_s *) aggetrec(node, (char *) "data", FALSE))->query;
    }
    
    void StateGraphGvc::GetMyChildNodes(Agnode_s *node, std::vector<Agnode_s *> &nodes) {
        nodes.clear();
        
        std::list<Agnode_s *> node_queue;
        node_queue.emplace_back(node);
        
        while (node_queue.size() > 0) {
            auto p = node_queue.front();
            node_queue.pop_front();
            
            for (Agedge_s *edge = agfstout(g, p); edge; edge = agnxtout(g, edge)) {
                auto nnode = aghead(edge);
                auto iter = std::find(nodes.begin(), nodes.end(), nnode);
                if (iter == nodes.end()) {
                    nodes.emplace_back(nnode);
                    node_queue.emplace_back(nnode);
                }
            }
        }
    }
    
    void StateGraphGvc::CreateEdge(Agnode_s *node) {
        StateQuery *curr = GetNodeData(node);
        std::vector<Agnode_s *> pnode_vec;
        
        auto create_edge = [&](Agnode_s *pnode, const char *name, const char *color) {
            StateQuery *prev = StateGraphGvc::GetNodeData(pnode);
            
            //연결하지 않아도 되는 관계
            //StateTable::UpdateUserTableListFromLog 의 add_related_info 람다 함수 참고
            //트랜잭션 사용으로 인해 판단할수 없음
            // if (curr->command == SQLCOM_INSERT || curr->command == SQLCOM_INSERT_SELECT)
            // {
            //   if (prev->command == SQLCOM_UPDATE || prev->command == SQLCOM_UPDATE_MULTI)
            //   {
            //     if (strcmp(name, "write-read") == 0)
            //       return;
            //   }
            // }
            
            // 동일한 연결이 있었는지 확인
            for (Agedge_s *edge = agfstin(g, node); edge; edge = agnxtin(g, edge)) {
                if (agtail(edge) == pnode)
                    return;
            }
            
            {
                Agedge_s *edge = agedge(g, pnode, node, (char *) name, 1);
                agsafeset(edge, (char *) "color", (char *) color, (char *) "");
                
                // 간접적인 연결이 있으면 연결 해제
                for (Agedge_s *ref_edge = agfstout(g, pnode); ref_edge; ref_edge = agnxtout(g, ref_edge)) {
                    if (aghead(ref_edge) == node)
                        continue;
                    
                    GetMyChildNodes(aghead(ref_edge), pnode_vec);
                    
                    if (pnode_vec.end() != std::find(pnode_vec.begin(), pnode_vec.end(), node)) {
                        agdeledge(g, edge);
                        edge = NULL;
                        break;
                    }
                }
                
                if (edge != NULL) {
                    prev->AddReference(curr);
                    curr->ref.IncReference();
                }
            }
        };
        
        for (auto &i: curr->write_set) {
            auto iter = write_node_map.find(i);
            if (iter != write_node_map.end()) {
                create_edge(iter->second, "write-write", "black");
            }
        }
        
        for (auto &i: curr->read_set) {
            auto iter = write_node_map.find(i);
            if (iter != write_node_map.end()) {
                create_edge(iter->second, "write-read", "red");
            }
        }
        
        for (auto &i: curr->write_set) {
            write_node_map[i] = node;
        }
    }
    
    void StateGraphGvc::AddQueries(const StateTable::Query &q) {
        node_count++;
        auto node = CreateNode(q.get(), node_count, main_sstream);
        CreateEdge(node);
    }
    
    void StateGraphGvc::AddQueries(const StateTable::QueryList &list) {
        for (auto &i: list) {
            node_count++;
            auto node = CreateNode(i.get(), node_count, main_sstream);
            CreateEdge(node);
        }
    }
    
    Agnode_t *StateGraphGvc::GetNextNode(Agnode_t *node) {
        auto search = [g = g](Agnode_t *curr) -> Agnode_t * {
            Agnode_t *child = NULL;
            size_t next_count = 0;
            for (Agedge_t *e = agfstout(g, curr); e; e = agnxtout(g, e)) {
                ++next_count;
                child = aghead(e);
            }
            
            if (next_count == 1) {
                auto q = GetNodeData(child);
                
                if (q->ref.TryAccess()) {
                    return child;
                }
            }
            
            return NULL;
        };
        
        Agnode_t *next = NULL;
        while (node != NULL) {
            next = search(node);
            if (next != NULL)
                return next;
            
            node = next;
        }
        
        return NULL;
    }
    
    void StateGraphGvc::PrintNodeData(const char *hdr, Agnode_s *node) {
        // auto q = GetNodeData(node);
        // q->ref.Print(hdr, q->GetRefSize(), q->query.c_str());
    }

// 전체 노드는 병렬 실행 할 수 있어야함
// 각 노드에서 1:1 로 연결되는 노드는 분리함
//    노드의 parent 가 없으면 head
//    노드의 child 가 parent 를 1개 갖으면 연결
//    노드의 child 가 parent 를 2개 이상 갖으면 head
//      단, 마지막 parent 가 실행될 때 까지 대기했다가 실행
// 분리된 head 들은 동시에 실행하여 병렬 실행 구현
    void StateGraphGvc::AnalyzeQueries(std::vector<Agnode_s *> &head_nodes) {
        Agnode_t *curr = NULL;
        Agnode_t *next = NULL;
        for (auto &n: head_nodes) {
            curr = n;
            // PrintNodeData("curr", curr);
            
            // head 에서 부터 1:1 로 참조하는 노드 탐색
            // 1:N 으로 참조하면 제일 마지막에 참조한 노드가 연결
            while ((next = GetNextNode(curr))) {
                auto curr_query = GetNodeData(curr);
                auto next_query = GetNodeData(next);
                curr_query->SetNext(next_query);
                next_query->is_valid_query = 1;
                
                curr = next;
                // PrintNodeData("next", next);
            }
            
            // 여기서 curr 은 head 에서 부터 1:1 로 연결되는 제일 마지막 노드임
            // curr 에 연결된 노드가 있으면 head 로 만들기 위해 참조 제거
            //
            // head 는 GetHeadQueries 함수에서 연결된 노드의 참조 제거함
            // curr 이 head 라면 참조 제거를 하지 않아야 함
            if (curr != n) {
                for (Agedge_t *e = agfstout(g, curr); e; e = agnxtout(g, e)) {
                    GetNodeData(aghead(e))->ref.DecReference();
                    // PrintNodeData("detach tail", aghead(e));
                }
            }
        }
    }
    
    void StateGraphGvc::MakeQueryList(std::vector<Agnode_s *> &head_nodes) {
        for (auto &n: head_nodes) {
            for (Agedge_t *e = agfstout(g, n); e; e = agnxtout(g, e)) {
                GetNodeData(aghead(e))->ref.DecReference();
                // PrintNodeData("detach head", aghead(e));
            }
            
            auto q = GetNodeData(n);
            q->ref.NoneReference();
            query_list.push_back(q);
            // PrintNodeData("make head", n);
        }
    }
    
    std::vector<Agnode_s *> StateGraphGvc::GetHeadNodes(NodeList::iterator begin, NodeList::iterator end) {
        std::vector<Agnode_s *> ret;
        
        // 참조되지 않은 노드 탐색 (head)
        for (auto iter = begin; iter != end; ++iter) {
            auto q = GetNodeData(*iter);
            if (q->ref.IsReferenced() == false && q->write_set.size() != 0) {
                ret.push_back(*iter);
                q->is_valid_query = 1;
            }
        }
        
        return ret;
    }
    
    std::vector<StateQuery *> StateGraphGvc::GetQueries() {
        if (query_list.size() > 0)
            return query_list;
        
        debug("[StateGraphGvc::GetQueries] Analyze Query");
        
        std::list<Agnode_s *> all_nodes;
        std::vector<Agnode_s *> head_nodes;
        
        for (Agnode_t *n = agfstnode(g); n; n = agnxtnode(g, n)) {
            all_nodes.push_back(n);
        }
        
        size_t thread_count = StateThreadPool::Instance().Size();
        
        std::vector<std::future<std::vector<Agnode_s *>>> futures;
        while (true) {
            futures.clear();
            size_t job_count = std::max((all_nodes.size() / thread_count) + 1, (size_t) 10000);
            NodeList::iterator end_iter;
            size_t idx = 0;
            for (auto iter = all_nodes.begin(); iter != all_nodes.end(); iter = end_iter) {
                idx += job_count;
                if (idx + job_count >= all_nodes.size())
                    end_iter = all_nodes.end();
                else
                    end_iter = std::next(iter, job_count);
                
                futures.emplace_back(StateThreadPool::Instance().EnqueueJob(
                    std::bind(&StateGraphGvc::GetHeadNodes, this, iter, end_iter)));
            }
            
            head_nodes.clear();
            for (auto &f: futures) {
                auto nodes = f.get();
                head_nodes.insert(head_nodes.end(), nodes.begin(), nodes.end());
            }
            if (head_nodes.size() == 0)
                break;
            
            AnalyzeQueries(head_nodes);
            MakeQueryList(head_nodes);
            
            all_nodes.remove_if([](const Agnode_s *n) {
                auto q = GetNodeData((Agnode_s *) n);
                return q->is_valid_query == 1 ? true : false;
            });
        }
        
        return query_list;
    }
    
    void StateGraphGvc::PrintSummary() {
        // 트랜잭션은 다른 방식으로 출력 되어야 함
#if 0
        size_t total_query_count = 0;
        size_t total_failed_count = 0;
        std::chrono::duration<double> total_elapsed_seconds;
      
        struct result
        {
          size_t query_count;
          size_t failed_count;
          std::chrono::duration<double> elapsed_seconds;
        };
      
        std::map<uint16_t, struct result> command_map;
      
        for (Agnode_t *n = agfstnode(g); n; n = agnxtnode(g, n))
        {
          auto q = GetNodeData(n);
      
          ++total_query_count;
          total_elapsed_seconds += q->elapsed_seconds;
      
          auto iter = command_map.find(q->command);
          if (iter == command_map.end())
          {
            struct result ret;
      
            if (q->is_failed != 0)
            {
              ret.query_count = 1;
              ret.failed_count = 1;
              ret.elapsed_seconds = q->elapsed_seconds;
              command_map.insert(std::make_pair(q->command, ret));
              ++total_failed_count;
            }
            else
            {
              ret.query_count = 1;
              ret.failed_count = 0;
              ret.elapsed_seconds = q->elapsed_seconds;
              command_map.insert(std::make_pair(q->command, ret));
            }
          }
          else
          {
            ++iter->second.query_count;
            iter->second.elapsed_seconds += q->elapsed_seconds;
            if (q->is_failed != 0)
            {
              ++iter->second.failed_count;
              ++total_failed_count;
            }
          }
        }
      
        printf("REDO QUERY SUMMARY ===========================\n");
      
        printf("total query count : %ld\n", total_query_count);
        printf("total query failed count : %ld\n", total_failed_count);
        printf("total elapsed time : %ld ms\n", std::chrono::duration_cast<std::chrono::milliseconds>(total_elapsed_seconds).count());
      
        for (auto &i : command_map)
        {
          printf("%s query count : %ld\n", StateUtil::GetCommandName(i.first), i.second.query_count);
          printf("%s query failed count : %ld\n", StateUtil::GetCommandName(i.first), i.second.failed_count);
          printf("%s elapsed time : %ld ms\n", StateUtil::GetCommandName(i.first), std::chrono::duration_cast<std::chrono::milliseconds>(i.second.elapsed_seconds).count());
        }
      
        printf("==============================================\n");
#endif
    }
    
}