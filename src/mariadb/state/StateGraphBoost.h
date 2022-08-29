#ifndef STATE_GRAPH_BOOST_INCLUDED
#define STATE_GRAPH_BOOST_INCLUDED

#include <string>
#include <vector>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>

#include "StateReference.hpp"
#include "StateUserQuery.h"
#include "StateTable.h"
#include "StateGraph.h"

namespace ultraverse::state {
    class StateGraphBoost : public StateGraph {
    public:
        struct TxnNode {
            // StateQuery *query;
            std::shared_ptr<v2::Transaction> transaction;
            
            TxnNode() {
            
            }
            
            TxnNode(std::shared_ptr<v2::Transaction> transaction):
                transaction(transaction)
            {
            
            }
            
            TxnNode(const TxnNode &source):
                transaction(source.transaction)
            {
            
            }
            
            bool addReference(TxnNode &vertex) {
                auto iter = std::find_if(_refList.begin(), _refList.end(), [&vertex](const auto &r) {
                    // ??
                    return r == &vertex._ref;
                });
                
                if (iter == _refList.end()) {
                    _refList.push_back(&vertex._ref);
                    return true;
                }
                
                return false;
            }
            
            void setNext(TxnNode &vertex) {
                _next = &vertex;
            }
            
            // TODO:
            // QueryNode *notifyAndGetNext();
            
            TxnNode *next() {
                return _next;
            }
            
            StateReference &ref() {
                return _ref;
            }
            
        private:
            StateReference _ref;
            TxnNode *_next;
            std::vector<StateReference *> _refList;
        };
        
        StateGraphBoost();
        
        ~StateGraphBoost();
        
        void addTransaction(std::shared_ptr<v2::Transaction> transaction) override;
        void addTransactions(std::vector<std::shared_ptr<v2::Transaction>> &transactions) override;
        
        const std::vector<TxnNode *> &getTransactions();
        
        virtual void PrintSummary();
        
        virtual void MakeOutputFilename(const std::string &type, const std::string &filepath);
        
        virtual void MakeOutputFile(const std::string &type, FILE *fp) {};
        
        void MakeEdgeOutputFile(const std::string &type, const std::string &filepath);
    
    private:
        using ListGraph =
            boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS, TxnNode>;
        
        void buildQueryList();
        
        void CreateEdge(size_t node_idx);
        
        bool HasChildNode(size_t node_idx, size_t child_idx);
        
        size_t GetNextNode(size_t node_idx);
        
        void AnalyzeQueries(std::vector<size_t> &head_nodes);
        
        void MakeQueryList(std::vector<size_t> &head_nodes);
        
        void DotConvert(const std::string &type, const std::string &prev_filepath, const std::string &filepath);
        
        void CreateEdge(size_t node_idx, size_t pnode_idx, const std::string &table);
        
        typedef std::list<size_t> NodeList;
        
        std::vector<size_t> GetHeadNodes(NodeList::iterator begin, NodeList::iterator end);
    
        LoggerPtr _logger;
        ListGraph _graph;
        std::vector<TxnNode *> _transactionList;
        std::map<std::string, size_t> write_node_idx_map;
    };
}

#endif /* STATE_GRAPH_BOOST_INCLUDED */
