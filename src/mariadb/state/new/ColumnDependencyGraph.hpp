//
// Created by cheesekun on 10/27/22.
//

#ifndef ULTRAVERSE_COLUMNDEPENDENCYGRAPH_HPP
#define ULTRAVERSE_COLUMNDEPENDENCYGRAPH_HPP

#include <boost/graph/adjacency_list.hpp>

#include "Query.hpp"

#include "utils/log.hpp"

namespace ultraverse::state::v2 {
    
    enum ColumnAccessType {
        READ,
        WRITE
    };
    
    struct ColumnDependencyNode {
        ColumnSet columnSet;
        ColumnAccessType accessType;
        size_t hash;
        
        template <typename Archive>
        void serialize(Archive &archive);
    };
    
    /**
     * target.writeSet = { Transactions.*, Accounts.balance } // 1
     *
     * ...
     *
     * query11.writeSet = { Users.* } // 2
     * query12.writeSet = { Accounts.* } // 3
     * query13.readSet = { Transactions.* } // 4
     *
     * 1 -> 3
     * 1 -> 4
     */
    class ColumnDependencyGraph {
    public:
        using Graph =
            boost::adjacency_list<boost::setS, boost::vecS, boost::undirectedS, std::shared_ptr<ColumnDependencyNode>>;
        
        ColumnDependencyGraph();
        
        /**
         * @return 그래프 자체에 변화가 있을 시 true를 반환한다.
         */
        bool add(const ColumnSet &columnSet, ColumnAccessType accessType);
        void clear();
        
        [[nodiscard]]
        bool isRelated(const ColumnSet &a, const ColumnSet &b) const;
        [[nodiscard]]
        bool isRelated(size_t hashA, size_t hashB) const;
    
        template <typename Archive>
        void save(Archive &archive) const;
    
        template <typename Archive>
        void load(Archive &archive);
    private:
        LoggerPtr _logger;
        
        Graph _graph;
        std::map<size_t, int> _nodeMap;
    };
}

#include "ColumnDependencyGraph.cereal.cpp"

#endif //ULTRAVERSE_COLUMNDEPENDENCYGRAPH_HPP
