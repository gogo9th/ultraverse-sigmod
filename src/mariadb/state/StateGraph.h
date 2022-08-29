#ifndef STATE_GRAPH_INCLUDED
#define STATE_GRAPH_INCLUDED

#include <string>
#include <vector>

#include "new/Query.hpp"
#include "new/Transaction.hpp"

#include "StateUserQuery.h"
#include "StateTable.h"

namespace ultraverse::state {
    class StateGraph {
    public:
        StateGraph() {}
        
        virtual ~StateGraph() {}
        
        virtual void addTransaction(std::shared_ptr<v2::Transaction> query) = 0;
        virtual void addTransactions(std::vector<std::shared_ptr<v2::Transaction>> &transactions) = 0;
        
        virtual void AddQueries(const StateTable::Query &q) = 0;
        
        virtual void AddQueries(const StateTable::QueryList &list) = 0;
        
        virtual std::vector<StateQuery *> GetQueries() = 0;
        
        virtual void PrintSummary() = 0;
        
        virtual void MakeOutputFilename(const std::string &type, const std::string &filepath) = 0;
        
        virtual void MakeOutputFile(const std::string &type, FILE *fp) = 0;
    };
}

#endif /* STATE_GRAPH_INCLUDED */
