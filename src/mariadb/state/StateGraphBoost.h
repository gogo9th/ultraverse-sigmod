#ifndef STATE_GRAPH_BOOST_INCLUDED
#define STATE_GRAPH_BOOST_INCLUDED

#include <string>
#include <vector>
#include <memory>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>

#include "StateReference.hpp"
#include "StateUserQuery.h"
#include "StateTable.h"
#include "StateGraph.h"

#include "new/StateChangeContext.hpp"

namespace ultraverse::state {
    class StateGraphBoost : public StateGraph {
    public:
        struct TxnNode {
            // StateQuery *query;
            std::shared_ptr<v2::Transaction> transaction;
            
            TxnNode():
                _ref(new StateReference)
            {
            
            }
            
            TxnNode(std::shared_ptr<v2::Transaction> transaction):
                transaction(transaction),
                _ref(new StateReference)
            {
            
            }
            
            TxnNode(const TxnNode &source):
                transaction(source.transaction),
                _ref(source._ref)
            {
            
            }
            
            bool addReference(std::shared_ptr<TxnNode> vertex) {
                auto iter = std::find_if(_refList.begin(), _refList.end(), [&vertex](const auto &r) {
                    // ??
                    return r == vertex->_ref;
                });
                
                if (iter == _refList.end()) {
                    _refList.push_back(vertex->_ref);
                    return true;
                }
                
                return false;
            }
            
            void setNext(std::shared_ptr<TxnNode> vertex) {
                _next = vertex;
            }
            
            // TODO:
            // QueryNode *notifyAndGetNext();
            
            std::shared_ptr<TxnNode> next() {
                return _next;
            }
            
            std::shared_ptr<StateReference> ref() {
                return _ref;
            }
            
            bool isValid = false;
            bool isProcessed = false;
            
            uint64_t nodeIdx = 0;
            std::vector<uint64_t> dependencies;
            std::mutex processLock;
        private:
            std::shared_ptr<StateReference> _ref;
            std::shared_ptr<TxnNode> _next = nullptr;
            std::vector<std::shared_ptr<StateReference>> _refList;
            
        };
        
        StateGraphBoost(std::shared_ptr<v2::StateChangeContext> _context);
        
        ~StateGraphBoost();
        
        std::pair<uint64_t, bool> addTransaction(std::shared_ptr<v2::Transaction> transaction);
        void addTransactions(std::vector<std::shared_ptr<v2::Transaction>> &transactions);
        
        void removeTransaction(uint64_t nodeIdx);
        
        std::shared_ptr<TxnNode> getTxnNode(uint64_t index);
        
        const std::vector<TxnNode *> &getTransactions();
        
        
        virtual void PrintSummary();
        
        virtual void MakeOutputFilename(const std::string &type, const std::string &filepath);
        
        virtual void MakeOutputFile(const std::string &type, FILE *fp) {};
        
        void MakeEdgeOutputFile(const std::string &type, const std::string &filepath);
        
        void dump() {
            std::scoped_lock<std::mutex> _lock(_nodeMutex);
            boost::write_graphviz(std::cout, _graph);
        }
    
    private:
        using ListGraph =
            boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS, std::shared_ptr<TxnNode>>;
        
        void buildQueryList();
        
        bool CreateEdge(size_t node_idx);
        
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
        
        std::mutex _nodeMutex;
        
        std::shared_ptr<v2::StateChangeContext> _context;
    };
}

#endif /* STATE_GRAPH_BOOST_INCLUDED */
