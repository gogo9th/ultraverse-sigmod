//
// Created by cheesekun on 1/21/26.
//

#ifndef ULTRAVERSE_TAINT_ANALYZER_HPP
#define ULTRAVERSE_TAINT_ANALYZER_HPP

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "mariadb/state/new/StateChangeContext.hpp"
#include "mariadb/state/new/Transaction.hpp"

namespace ultraverse::state::v2 {
    class StateCluster;
    class RelationshipResolver;
}

namespace ultraverse::state::v2::analysis {
    class TaintAnalyzer {
    public:
        struct ColumnRW {
            ColumnSet read;
            ColumnSet write;
        };

        static ColumnRW collectColumnRW(const Transaction &transaction);

        static bool isColumnRelated(const std::string &columnA,
                                    const std::string &columnB,
                                    const std::vector<ForeignKey> &foreignKeys);

        static bool columnSetsRelated(const ColumnSet &taintedWrites,
                                      const ColumnSet &candidateColumns,
                                      const std::vector<ForeignKey> &foreignKeys);

        static bool hasKeyColumnItems(const Transaction &transaction,
                                      const StateCluster &cluster,
                                      const RelationshipResolver &resolver);
    };
}

#endif // ULTRAVERSE_TAINT_ANALYZER_HPP
