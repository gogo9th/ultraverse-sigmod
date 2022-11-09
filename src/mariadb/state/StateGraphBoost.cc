#include <nlohmann/json.hpp>

#include "StateGraphBoost.h"
#include "StateUtil.h"
#include "StateThreadPool.h"

#include <graphviz/gvc.h>
#include <graphviz/cgraph.h>
#include <graphviz/cdt.h>

namespace ultraverse::state {
    
    StateGraphBoost::StateGraphBoost(std::shared_ptr<v2::StateChangeContext> context):
        StateGraph(),
        _logger(createLogger("StateGraphBoost")),
        _context(context)
    {
    }
    
    StateGraphBoost::~StateGraphBoost() {
    }
    
    std::pair<uint64_t, bool> StateGraphBoost::addTransaction(std::shared_ptr<v2::Transaction> transaction) {
        std::scoped_lock<std::mutex> _lock(_nodeMutex);
        auto nodeIdx = add_vertex(std::make_shared<TxnNode>(std::move(transaction)), _graph);
        
        _graph[nodeIdx]->nodeIdx = nodeIdx;
        
        bool isEntrypoint = CreateEdge(nodeIdx);
        _logger->info("inserted node #{}", nodeIdx);
        return std::make_pair(nodeIdx, isEntrypoint);
    }
    
    void StateGraphBoost::addTransactions(std::vector<std::shared_ptr<v2::Transaction>> &transactions) {
        for (auto &transaction: transactions) {
            addTransaction(transaction);
        }
    }
    
    void StateGraphBoost::removeTransaction(uint64_t nodeIdx) {
        std::scoped_lock<std::mutex> _lock(_nodeMutex);
        auto node = _graph[nodeIdx];
        
        
        if (node->transaction != nullptr) {
            node->transaction.reset();
            node->isProcessed = true;
        }
    
        clear_vertex(nodeIdx, _graph);
    }
    
    std::shared_ptr<StateGraphBoost::TxnNode> StateGraphBoost::getTxnNode(uint64_t index) {
        return _graph[index];
    }
    
    bool StateGraphBoost::HasChildNode(size_t node_idx, size_t child_idx) {
        std::list<size_t> node_queue;
        node_queue.emplace_back(node_idx);
        
        while (node_queue.size() > 0) {
            auto p = node_queue.front();
            node_queue.pop_front();
            
            auto out = out_edges(p, _graph);
            for (auto iter = out.first; iter != out.second; ++iter) {
                auto target_idx = target(*iter, _graph);
                if (target_idx == child_idx) {
                    return true;
                }
                node_queue.emplace_back(target_idx);
            }
        }
        
        return false;
    }
    
    void StateGraphBoost::CreateEdge(size_t node_idx, size_t pnode_idx, const std::string &table) {
        auto &curr = _graph[node_idx];
        auto &prev = _graph[pnode_idx];
        
        auto in = in_edges(node_idx, _graph);
        for (auto iter = in.first; iter != in.second; ++iter) {
            auto source_idx = source(*iter, _graph);
            // 동일한 연결이 있었는지 확인
            if (source_idx == pnode_idx)
                return;
            
            // 바로 위에 연결된 노드가 동일한 종속성을 가지면 추가로 연결할 필요 없음
            std::shared_ptr<v2::Transaction> &sourceTransaction = _graph[source_idx]->transaction;
            if (std::find(sourceTransaction->readSet().begin(), sourceTransaction->readSet().end(), table) !=
                sourceTransaction->readSet().end()) {
                return;
            }
            if (std::find(sourceTransaction->writeSet().begin(), sourceTransaction->writeSet().end(), table) !=
                sourceTransaction->writeSet().end()) {
                return;
            }
        }
        
        auto edge = add_edge(pnode_idx, node_idx, _graph);
        
        // 간접적인 연결이 있으면 연결 해제
        // 종속 노드에서 부터 모두 탐색
        auto out = out_edges(pnode_idx, _graph);
        for (auto iter = out.first; iter != out.second; ++iter) {
            auto target_idx = target(*iter, _graph);
            if (target_idx == node_idx)
                continue;
            
            if (HasChildNode(target_idx, node_idx)) {
                remove_edge(edge.first, _graph);
                return;
            }
        }
        
        prev->addReference(curr);
        curr->ref()->IncReference();
    }
    
    bool StateGraphBoost::CreateEdge(size_t node_idx) {
        auto &curr = _graph[node_idx];
        
        bool isEntrypoint = false;
        
        std::vector<std::string> table_list;
        table_list.insert(table_list.begin(), curr->transaction->writeSet().begin(), curr->transaction->writeSet().end());
        table_list.insert(table_list.begin(), curr->transaction->readSet().begin(), curr->transaction->readSet().end());
    
        {
            std::scoped_lock _lock(_context->contextLock);
            for (const std::string &dbTableName: curr->transaction->writeSet()) {
                auto pair = StateUserQuery::SplitDBNameAndTableName(dbTableName);
                auto dbName = pair[0];
                auto tableName = pair[1];
            
                for (const v2::ForeignKey &foreignKey: _context->foreignKeys) {
                    if (foreignKey.fromTable->getCurrentName() == tableName) {
                        table_list.push_back(dbName + "." + foreignKey.toTable->getCurrentName());
                    } else if (foreignKey.toTable->getCurrentName() == tableName) {
                        table_list.push_back(dbName + "." + foreignKey.fromTable->getCurrentName());
                    }
                }
            }
        }
        
        StateUtil::unique_vector(table_list);
        
        std::multimap<int, std::string> prior_map;
        
        if (table_list.empty()) {
            isEntrypoint = true;
        }
        
        for (auto &i: table_list) {
            auto iter = write_node_idx_map.find(i);
            if (iter != write_node_idx_map.end() && !_graph[iter->second]->isProcessed) {
                prior_map.insert(std::pair<int, std::string>(iter->second, i));
            } else {
                isEntrypoint = true;
            }
        }
        
        //가장 가까운 노드부터 추가
        for (auto &i: r_wrap(prior_map)) {
            CreateEdge(node_idx, i.first, i.second);
            _graph[node_idx]->dependencies.push_back(i.first);
            
            if (_graph[i.first]->next() == nullptr) {
                _graph[i.first]->setNext(_graph[node_idx]);
            } else {
                isEntrypoint = true;
            }
            
        }
        
        for (auto &i: table_list) {
            write_node_idx_map[i] = node_idx;
        }
        
        return isEntrypoint;
    }
    
    void StateGraphBoost::buildQueryList() {
        _logger->info("Building query list..");
        
        std::list<size_t> all_nodes;
        std::vector<size_t> head_nodes;
        
        all_nodes.assign(_graph.vertex_set().begin(), _graph.vertex_set().end());
        
        size_t thread_count = StateThreadPool::Instance().Size();
        
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
                [this](const size_t idx) {
                    auto &q = _graph[idx];
                    return q->isValid;
                });
        }
    }
    
    const std::vector<StateGraphBoost::TxnNode *> &StateGraphBoost::getTransactions() {
        if (_transactionList.empty()) {
            buildQueryList();
        }
        
        // assert(!_transactionList.empty());
        
        return _transactionList;
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
                
                static void print_set(std::ostream &outStream, const std::unordered_set<std::string> &s) {
                    if (s.size() == 0) {
                        return;
                    }
                    
                    int i = 0;
                    for (auto &val: s) {
                        outStream << val;
                        
                        if (i < s.size() - 1) {
                            outStream << ", ";
                        }
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
                    
                    auto txn = _Graph[idx]->transaction;
                    outStream << "[label=\"{xid " << txn->xid();
                    
                    outStream << "|{read set|";
                    vertexWriter::print_set(outStream, txn->readSet());
                    outStream << '}';
                    
                    outStream << "|{write set|";
                    vertexWriter::print_set(outStream, txn->writeSet());
                    outStream << '}';
                    
                    for (auto &i: txn->queries()) {
                        read_map.clear();
                        write_map.clear();
                        for (auto &f: i->foreignKeySet()) {
                            std::cerr << ("FIXME: i->foreignKeySet() is string!") << std::endl;
                            /*
                            if (f.access_type == en_table_access_read) {
                                for (auto &c: f.columns) {
                                    read_map[f.table].push_back(c);
                                }
                            } else {
                                for (auto &c: f.columns) {
                                    write_map[f.table].push_back(c);
                                }
                            }
                             */
                        }
                        
                        std::string convert_query = i->statement();
                        vertexWriter::replace(convert_query, "@", "");
                        vertexWriter::replace(convert_query, "<", "\\<");
                        vertexWriter::replace(convert_query, ">", "\\>");
                        
                        outStream << "|{"
                                  << idx << '|'
                                 // << i.time.sec << '.'
                                 // << i.time.sec_part << '|'
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
            
            write_graphviz(ofstream, _graph, vertexWriter(_graph), boost::default_writer(), graphWriter());
            ofstream.close();
            
            DotConvert(type, prev_filepath, filepath);
        }
    }
    
    void StateGraphBoost::MakeEdgeOutputFile(const std::string &type, const std::string &filepath) {
        if (type == "json") {
            std::ofstream fout;
            fout.open(filepath + "." + type);
            nlohmann::json j;
            
            for (auto &e: _graph.m_edges) {
                auto source = _graph[e.m_source]->transaction;
                auto target = _graph[e.m_target]->transaction;
                
                nlohmann::json obj;
                obj["source_idx"] = source->xid();
                obj["target_idx"] = target->xid();
                j.emplace_back(obj);
            }
            
            fout << std::setw(4) << j << std::endl;
        } else if (type == "txt") {
            std::ofstream fout;
            fout.open(filepath + "." + type);
            
            for (auto &e: _graph.m_edges) {
                auto source = _graph[e.m_source]->transaction;
                auto target = _graph[e.m_target]->transaction;
                fout << source->xid() << " -> " << target->xid() << std::endl;
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
        
        // 참조되지 않은 노드 탐색 (head)
        for (auto iter = begin; iter != end; ++iter) {
            auto &q = _graph[*iter];
            if (!q->ref()->IsReferenced() && q->transaction->writeSet().size() != 0) {
                ret.push_back(*iter);
                q->isValid = true;
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
        size_t curr_idx = (size_t) -1;
        size_t next_idx = (size_t) -1;
        for (auto &n: head_nodes) {
            curr_idx = n;
            
            // head 에서 부터 1:1 로 참조하는 노드 탐색
            // 1:N 으로 참조하면 제일 마지막에 참조한 노드가 연결
            while ((next_idx = GetNextNode(curr_idx)) != (size_t) -1) {
                auto &curr_query = _graph[curr_idx];
                auto &next_query = _graph[next_idx];
                curr_query->setNext(next_query);
                next_query->isValid = true;
                
                curr_idx = next_idx;
            }
            
            // 여기서 curr 은 head 에서 부터 1:1 로 연결되는 제일 마지막 노드임
            // curr 에 연결된 노드가 있으면 head 로 만들기 위해 참조 제거
            //
            // head 는 GetHeadQueries 함수에서 연결된 노드의 참조 제거함
            // curr 이 head 라면 참조 제거를 하지 않아야 함
            if (curr_idx != n) {
                auto out = out_edges(curr_idx, _graph);
                for (auto iter = out.first; iter != out.second; ++iter) {
                    _graph[target(*iter, _graph)]->ref()->DecReference();
                }
            }
        }
    }
    
    size_t StateGraphBoost::GetNextNode(size_t node_idx) {
        if (out_degree(node_idx, _graph) != 1)
            return (size_t) -1;
        
        auto out = out_edges(node_idx, _graph);
        node_idx = target(*out.first, _graph);
        
        auto &q = _graph[node_idx];
        if (q->ref()->TryAccess()) {
            return node_idx;
        } else {
            return (size_t) -1;
        }
    }
    
    void StateGraphBoost::MakeQueryList(std::vector<size_t> &head_nodes) {
        for (auto &n: head_nodes) {
            auto out = out_edges(n, _graph);
            for (auto iter = out.first; iter != out.second; ++iter) {
                _graph[target(*iter, _graph)]->ref()->DecReference();
            }
            
            auto &q = _graph[n];
            q->ref()->NoneReference();
            // _transactionList.push_back(&q);
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
      
        auto value_map = get(&QueryNode::query, g);
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