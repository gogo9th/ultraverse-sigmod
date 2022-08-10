#ifndef STATE_GRAPH_BOOST_INCLUDED
#define STATE_GRAPH_BOOST_INCLUDED

#include <string>
#include <vector>

#include "StateUserQuery.h"
#include "StateTable.h"
#include "StateGraph.h"

class StateGraphBoost : public StateGraph
{
public:
  StateGraphBoost();
  ~StateGraphBoost();

  virtual void AddQueries(const StateTable::Query &q);
  virtual void AddQueries(const StateTable::QueryList &list);
  virtual std::vector<StateQuery *> GetQueries();

  virtual void PrintSummary();
  virtual void MakeOutputFilename(const std::string &type, const std::string &filepath);
  virtual void MakeOutputFile(const std::string &type, FILE *fp) {};
  void MakeEdgeOutputFile(const std::string &type, const std::string &filepath);

private:
  void CreateEdge(size_t node_idx);
  bool HasChildNode(size_t node_idx, size_t child_idx);
  size_t GetNextNode(size_t node_idx);
  void AnalyzeQueries(std::vector<size_t> &head_nodes);
  void MakeQueryList(std::vector<size_t> &head_nodes);
  void DotConvert(const std::string &type, const std::string &prev_filepath, const std::string &filepath);

  void CreateEdge(size_t node_idx, size_t pnode_idx, const std::string &table);

  typedef std::list<size_t> NodeList;
  std::vector<size_t> GetHeadNodes(NodeList::iterator begin, NodeList::iterator end);

  std::vector<StateQuery *> query_list;
  std::map<std::string, size_t> write_node_idx_map;
};

#endif /* STATE_GRAPH_BOOST_INCLUDED */
