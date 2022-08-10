#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>

#include "nlohmann/json.hpp"

#include "StateGraphBoost.h"
#include "StateUtil.h"
#include "StateThreadPool.h"

#include <graphviz/gvc.h>
#include <graphviz/cgraph.h>
#include <graphviz/cdt.h>

namespace ultraverse::state {
    struct VertexP {
        StateQuery *query;
    };
    
    typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS, VertexP> ListGraph;
    ListGraph g;
    
    StateGraphBoost::StateGraphBoost()
        : StateGraph() {
    }
    
    StateGraphBoost::~StateGraphBoost() {
    }
    
    void StateGraphBoost::AddQueries(const StateTable::Query &q) {
        auto node_idx = add_vertex(VertexP{q.get()}, g);
        CreateEdge(node_idx);
    }
    
    void StateGraphBoost::AddQueries(const StateTable::QueryList &list) {
        for (auto &i: list) {
            auto node_idx = add_vertex(VertexP{i.get()}, g);
            CreateEdge(node_idx);
        }
    }
    
    bool StateGraphBoost::HasChildNode(size_t node_idx, size_t child_idx) {
        std::list<size_t> node_queue;
        node_queue.emplace_back(node_idx);
        
        while (node_queue.size() > 0) {
            auto p = node_queue.front();
            node_queue.pop_front();
            
            auto out = out_edges(p, g);
            for (auto iter = out.first; iter != out.second; ++iter) {
                auto target_idx = target(*iter, g);
                if (target_idx == child_idx) {
                    return true;
                }
                node_queue.emplace_back(target_idx);
            }
        }
        
        return false;
    }
    
    void StateGraphBoost::CreateEdge(size_t node_idx, size_t pnode_idx, const std::string &table) {
        auto value_map = get(&VertexP::query, g);
        StateQuery *curr = value_map[node_idx];
        StateQuery *prev = value_map[pnode_idx];
        
        auto in = in_edges(node_idx, g);
        for (auto iter = in.first; iter != in.second; ++iter) {
            auto source_idx = source(*iter, g);
            // 동일한 연결이 있었는지 확인
            if (source_idx == pnode_idx)
                return;
            
            // 바로 위에 연결된 노드가 동일한 종속성을 가지면 추가로 연결할 필요 없음
            StateQuery *source_query = value_map[source_idx];
            if (std::find(source_query->read_set.begin(), source_query->read_set.end(), table) !=
                source_query->read_set.end()) {
                return;
            }
            if (std::find(source_query->write_set.begin(), source_query->write_set.end(), table) !=
                source_query->write_set.end()) {
                return;
            }
        }
        
        auto edge = add_edge(pnode_idx, node_idx, g);
        
        // 간접적인 연결이 있으면 연결 해제
        // 종속 노드에서 부터 모두 탐색
        auto out = out_edges(pnode_idx, g);
        for (auto iter = out.first; iter != out.second; ++iter) {
            auto target_idx = target(*iter, g);
            if (target_idx == node_idx)
                continue;
            
            if (HasChildNode(target_idx, node_idx) == true) {
                remove_edge(edge.first, g);
                return;
            }
        }
        
        prev->AddReference(curr);
        curr->ref.IncReference();
    }
    
    void StateGraphBoost::CreateEdge(size_t node_idx) {
        auto value_map = get(&VertexP::query, g);
        StateQuery *curr = value_map[node_idx];
        
        std::vector<std::string> table_list;
        table_list.insert(table_list.begin(), curr->write_set.begin(), curr->write_set.end());
        table_list.insert(table_list.begin(), curr->read_set.begin(), curr->read_set.end());
        StateUtil::unique_vector(table_list);
        
        std::multimap<int, std::string> prior_map;
        for (auto &i: table_list) {
            auto iter = write_node_idx_map.find(i);
            if (iter != write_node_idx_map.end()) {
                prior_map.insert(std::pair<int, std::string>(iter->second, i));
            }
        }
        
        //가장 가까운 노드부터 추가
        for (auto &i: r_wrap(prior_map)) {
            CreateEdge(node_idx, i.first, i.second);
        }
        
        for (auto &i: curr->write_set) {
            write_node_idx_map[i] = node_idx;
        }
    }
    
    std::vector<StateQuery *> StateGraphBoost::GetQueries() {
        if (query_list.size() > 0)
            return query_list;
        
        debug("[StateGraphGvc::GetQueries] Analyze Query");
        
        std::list<size_t> all_nodes;
        std::vector<size_t> head_nodes;
        
        all_nodes.assign(g.vertex_set().begin(), g.vertex_set().end());
        
        size_t thread_count = StateThreadPool::Instance().Size();
        
        auto value_map = get(&VertexP::query, g);
        
        std::vector<std::future<std::vector<size_t>>> futures;
        while (true) {
            futures.clear();
            size_t job_count = std::min((all_nodes.size() / thread_count) + 1, (size_t) 10000);
            NodeList::iterator end_iter;
            size_t idx = 0;
            for (auto iter = all_nodes.begin(); iter != all_nodes.end(); iter = end_iter) {
                idx += job_count;
                if (idx + job_count >= all_nodes.size())
                    end_iter = all_nodes.end();
                else
                    end_iter = std::next(iter, job_count);
                
                futures.emplace_back(StateThreadPool::Instance().EnqueueJob(
                    std::bind(&StateGraphBoost::GetHeadNodes, this, iter, end_iter)));
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
            
            all_nodes.remove_if(
                [&value_map](const size_t idx) {
                    auto q = value_map[idx];
                    return q->is_valid_query == 1 ? true : false;
                });
        }
        
        return query_list;
    }
    
    void StateGraphBoost::MakeOutputFilename(const std::string &type, const std::string &filepath) {
        std::string prev_filepath = filepath + ".dot";
        std::ofstream ofstream(prev_filepath, std::ios::out | std::ios::trunc);
        if (!ofstream) {
            std::cout << "Could not open file." << std::endl;
        } else {
            class vertexWriter {
            public:
                vertexWriter(const ListGraph &graph)
                    : _Graph(graph) {};
                
                static std::string replace(std::string &str, const std::string &from, const std::string &to) {
                    std::string::size_type start_pos = 0;
                    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
                        str.replace(start_pos, from.length(), to);
                        start_pos += to.length();
                    }
                    return str;
                };
                
                static void print_vector(std::ostream &outStream, const std::vector<std::string> &v) {
                    if (v.size() == 0) {
                        return;
                    }
                    
                    outStream << v[0];
                    for (size_t i = 1; i < v.size(); ++i) {
                        outStream << ", " << v[i];
                    }
                };
                
                static void
                print_map(std::ostream &outStream, const std::map<std::string, std::vector<std::string>> &m) {
                    if (m.size() == 0) {
                        return;
                    }
                    
                    auto iter = m.begin();
                    
                    auto func = [&]() {
                        outStream << '{' << iter->first << '|';
                        vertexWriter::print_vector(outStream, iter->second);
                        outStream << '}';
                    };
                    
                    outStream << '{';
                    func();
                    ++iter;
                    for (; iter != m.end(); ++iter) {
                        outStream << '|';
                        func();
                    }
                    outStream << '}';
                };
                
                void operator()(std::ostream &outStream, size_t idx) {
                    std::map<std::string, std::vector<std::string>> read_map, write_map;
                    
                    auto q = boost::get(&VertexP::query, _Graph, idx);
                    outStream << "[label=\"{xid " << q->xid;
                    
                    outStream << "|{read set|";
                    vertexWriter::print_vector(outStream, q->read_set);
                    outStream << '}';
                    
                    outStream << "|{write set|";
                    vertexWriter::print_vector(outStream, q->write_set);
                    outStream << '}';
                    
                    for (auto &i: q->transactions) {
                        read_map.clear();
                        write_map.clear();
                        for (auto &f: i.foreign_set) {
                            if (f.access_type == en_table_access_read) {
                                for (auto &c: f.columns) {
                                    read_map[f.table].push_back(c);
                                }
                            } else {
                                for (auto &c: f.columns) {
                                    write_map[f.table].push_back(c);
                                }
                            }
                        }
                        
                        std::string convert_query = i.query;
                        vertexWriter::replace(convert_query, "@", "");
                        vertexWriter::replace(convert_query, "<", "\\<");
                        vertexWriter::replace(convert_query, ">", "\\>");
                        
                        outStream << "|{"
                                  << idx << '|'
                                  << i.time.sec << '.'
                                  << i.time.sec_part << '|'
                                  << convert_query << "|{";
                        
                        outStream << "{read columns|";
                        vertexWriter::print_map(outStream, read_map);
                        outStream << "}|{write columns|";
                        vertexWriter::print_map(outStream, write_map);
                        outStream << "}}}";
                    }
                    
                    outStream << "}\"]";
                };
            
            private:
                const ListGraph &_Graph;
            };
            
            class graphWriter {
            public:
                graphWriter() {};
                
                void operator()(std::ostream &outStream) {
                    outStream << "node[shape=record];" << std::endl;
                };
            };
            
            write_graphviz(ofstream, g, vertexWriter(g), boost::default_writer(), graphWriter());
            ofstream.close();
            
            DotConvert(type, prev_filepath, filepath);
        }
    }
    
    void StateGraphBoost::MakeEdgeOutputFile(const std::string &type, const std::string &filepath) {
        if (type == "json") {
            std::ofstream fout;
            fout.open(filepath + "." + type);
            nlohmann::json j;
            
            for (auto &e: g.m_edges) {
                auto source = boost::get(&VertexP::query, g, e.m_source);
                auto target = boost::get(&VertexP::query, g, e.m_target);
                
                nlohmann::json obj;
                obj["source_idx"] = source->xid;
                obj["target_idx"] = target->xid;
                j.emplace_back(obj);
            }
            
            fout << std::setw(4) << j << std::endl;
        } else if (type == "txt") {
            std::ofstream fout;
            fout.open(filepath + "." + type);
            
            for (auto &e: g.m_edges) {
                auto source = boost::get(&VertexP::query, g, e.m_source);
                auto target = boost::get(&VertexP::query, g, e.m_target);
                fout << source->xid << " -> " << target->xid << std::endl;
            }
        }
    }
    
    void StateGraphBoost::DotConvert(const std::string &type, const std::string &prev_filepath,
                                     const std::string &filepath) {
        // $ dot -Tsvg result.svg.dot -o result.svg
        
        FILE *fp = fopen(prev_filepath.c_str(), "r");
        if (fp == NULL) {
            return;
        }
        
        auto gvc = gvContext();
        auto g = agread(fp, 0);
        
        gvLayout(gvc, g, "dot");
        gvRenderFilename(gvc, g, type.c_str(), filepath.c_str());
        
        gvFreeLayout(gvc, g);
        agclose(g);
        gvFreeContext(gvc);
        
        unlink(prev_filepath.c_str());
    }
    
    std::vector<size_t> StateGraphBoost::GetHeadNodes(NodeList::iterator begin, NodeList::iterator end) {
        std::vector<size_t> ret;
        
        auto value_map = get(&VertexP::query, g);
        
        // 참조되지 않은 노드 탐색 (head)
        for (auto iter = begin; iter != end; ++iter) {
            auto q = value_map[*iter];
            if (q->ref.IsReferenced() == false && q->write_set.size() != 0) {
                ret.push_back(*iter);
                q->is_valid_query = 1;
            }
        }
        
        return ret;
    }

// 전체 노드는 병렬 실행 할 수 있어야함
// 각 노드에서 1:1 로 연결되는 노드는 분리함
//    노드의 parent 가 없으면 head
//    노드의 child 가 parent 를 1개 갖으면 연결
//    노드의 child 가 parent 를 2개 이상 갖으면 head
//      단, 마지막 parent 가 실행될 때 까지 대기했다가 실행
// 분리된 head 들은 동시에 실행하여 병렬 실행 구현
    void StateGraphBoost::AnalyzeQueries(std::vector<size_t> &head_nodes) {
        auto value_map = get(&VertexP::query, g);
        
        size_t curr_idx = (size_t) -1;
        size_t next_idx = (size_t) -1;
        for (auto &n: head_nodes) {
            curr_idx = n;
            
            // head 에서 부터 1:1 로 참조하는 노드 탐색
            // 1:N 으로 참조하면 제일 마지막에 참조한 노드가 연결
            while ((next_idx = GetNextNode(curr_idx)) != (size_t) -1) {
                auto curr_query = value_map[curr_idx];
                auto next_query = value_map[next_idx];
                curr_query->SetNext(next_query);
                next_query->is_valid_query = 1;
                
                curr_idx = next_idx;
            }
            
            // 여기서 curr 은 head 에서 부터 1:1 로 연결되는 제일 마지막 노드임
            // curr 에 연결된 노드가 있으면 head 로 만들기 위해 참조 제거
            //
            // head 는 GetHeadQueries 함수에서 연결된 노드의 참조 제거함
            // curr 이 head 라면 참조 제거를 하지 않아야 함
            if (curr_idx != n) {
                auto out = out_edges(curr_idx, g);
                for (auto iter = out.first; iter != out.second; ++iter) {
                    value_map[target(*iter, g)]->ref.DecReference();
                }
            }
        }
    }
    
    size_t StateGraphBoost::GetNextNode(size_t node_idx) {
        if (out_degree(node_idx, g) != 1)
            return (size_t) -1;
        
        auto value_map = get(&VertexP::query, g);
        auto out = out_edges(node_idx, g);
        node_idx = target(*out.first, g);
        
        auto q = value_map[node_idx];
        if (q->ref.TryAccess()) {
            return node_idx;
        } else {
            return (size_t) -1;
        }
    }
    
    void StateGraphBoost::MakeQueryList(std::vector<size_t> &head_nodes) {
        auto value_map = get(&VertexP::query, g);
        for (auto &n: head_nodes) {
            auto out = out_edges(n, g);
            for (auto iter = out.first; iter != out.second; ++iter) {
                value_map[target(*iter, g)]->ref.DecReference();
            }
            
            auto q = value_map[n];
            q->ref.NoneReference();
            query_list.push_back(q);
        }
    }
    
    void StateGraphBoost::PrintSummary() {
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
      
        auto value_map = get(&VertexP::query, g);
        for (const auto &i : g.vertex_set())
        {
          auto q = value_map[i];
      
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