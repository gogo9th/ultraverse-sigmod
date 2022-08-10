#ifndef STATE_GRAPH_GVC_INCLUDED
#define STATE_GRAPH_GVC_INCLUDED

#include <string>
#include <vector>
#include <list>
#include <map>
#include <sstream>

#include "StateUserQuery.h"
#include "StateTable.h"
#include "StateGraph.h"

namespace ultraverse::state {
    
    struct GVC_s;
    struct Agnode_s;
    struct Agraph_s;
    
    class StateGraphGvc : public StateGraph {
    public:
        StateGraphGvc();
        
        ~StateGraphGvc();
        
        virtual void AddQueries(const StateTable::Query &q);
        
        virtual void AddQueries(const StateTable::QueryList &list);
        
        virtual std::vector<StateQuery *> GetQueries();
        
        virtual void PrintSummary();
        
        virtual void MakeOutputFilename(const std::string &type, const std::string &filepath);
        
        virtual void MakeOutputFile(const std::string &type, FILE *fp);
    
    private:
        Agnode_s *CreateNode(StateQuery *query, size_t node_idx, std::stringstream &sstream);
        
        void CreateEdge(Agnode_s *node);
        
        Agnode_s *GetNextNode(Agnode_s *node);
        
        static void SetNodeData(Agnode_s *node, StateQuery *query);
        
        static StateQuery *GetNodeData(Agnode_s *node);
        
        static void PrintNodeData(const char *hdr, Agnode_s *node);
        
        typedef std::list<Agnode_s *> NodeList;
        
        std::vector<Agnode_s *> GetHeadNodes(NodeList::iterator begin, NodeList::iterator end);
        
        void AnalyzeQueries(std::vector<Agnode_s *> &head_nodes);
        
        void MakeQueryList(std::vector<Agnode_s *> &head_nodes);
        
        void GetMyChildNodes(Agnode_s *node, std::vector<Agnode_s *> &nodes);
        
        GVC_s *gvc;
        Agraph_s *g;
        
        size_t node_count;
        std::stringstream main_sstream;
        
        std::vector<StateQuery *> query_list;
        std::map<std::string, Agnode_s *> write_node_map;
    };
    
}

#endif /* STATE_GRAPH_GVC_INCLUDED */