//
// Created by cheesekun on 1/21/26.
//

#include "TaintAnalyzer.hpp"

#include <algorithm>

#include "mariadb/state/new/cluster/RowCluster.hpp"
#include "mariadb/state/new/cluster/StateCluster.hpp"
#include "mariadb/state/new/cluster/StateRelationshipResolver.hpp"
#include "utils/StringUtil.hpp"

namespace ultraverse::state::v2::analysis {
    TaintAnalyzer::ColumnRW TaintAnalyzer::collectColumnRW(const Transaction &transaction) {
        ColumnRW rw;
        for (const auto &query : transaction.queries()) {
            if (query->flags() & Query::FLAG_IS_DDL) {
                continue;
            }
            rw.read.insert(query->readColumns().begin(), query->readColumns().end());
            rw.write.insert(query->writeColumns().begin(), query->writeColumns().end());
        }
        return rw;
    }

    bool TaintAnalyzer::isColumnRelated(const std::string &columnA,
                                        const std::string &columnB,
                                        const std::vector<ForeignKey> &foreignKeys) {
        const auto resolvedA = ultraverse::state::v2::RowCluster::resolveForeignKey(columnA, foreignKeys);
        const auto resolvedB = ultraverse::state::v2::RowCluster::resolveForeignKey(columnB, foreignKeys);

        const auto vecA = ultraverse::utility::splitTableName(resolvedA);
        const auto vecB = ultraverse::utility::splitTableName(resolvedB);

        const auto &tableA = vecA.first;
        const auto &colA = vecA.second;
        const auto &tableB = vecB.first;
        const auto &colB = vecB.second;

        if (tableA.empty() || tableB.empty()) {
            return resolvedA == resolvedB;
        }

        if (tableA == tableB && (colA == colB || colA == "*" || colB == "*")) {
            return true;
        }

        if (colA == "*" || colB == "*") {
            const auto it = std::find_if(foreignKeys.begin(), foreignKeys.end(), [&tableA, &tableB](const ForeignKey &fk) {
                return (
                    (fk.fromTable->getCurrentName() == tableA && fk.toTable->getCurrentName() == tableB) ||
                    (fk.fromTable->getCurrentName() == tableB && fk.toTable->getCurrentName() == tableA)
                );
            });

            if (it != foreignKeys.end()) {
                if (colA == "*" && colB == "*") {
                    return true;
                }

                if (colA == "*") {
                    return it->fromColumn == colB || it->toColumn == colB;
                }

                if (colB == "*") {
                    return it->fromColumn == colA || it->toColumn == colA;
                }
            }
        }

        return false;
    }

    bool TaintAnalyzer::columnSetsRelated(const ColumnSet &taintedWrites,
                                          const ColumnSet &candidateColumns,
                                          const std::vector<ForeignKey> &foreignKeys) {
        if (taintedWrites.empty() || candidateColumns.empty()) {
            return false;
        }

        for (const auto &tainted : taintedWrites) {
            for (const auto &column : candidateColumns) {
                if (isColumnRelated(tainted, column, foreignKeys)) {
                    return true;
                }
            }
        }

        return false;
    }

    bool TaintAnalyzer::hasKeyColumnItems(const Transaction &transaction,
                                          const StateCluster &cluster,
                                          const RelationshipResolver &resolver) {
        for (const auto &query : transaction.queries()) {
            if (query->flags() & Query::FLAG_IS_DDL) {
                continue;
            }

            for (const auto &item : query->readSet()) {
                if (cluster.isKeyColumnItem(resolver, item)) {
                    return true;
                }
            }

            for (const auto &item : query->writeSet()) {
                if (cluster.isKeyColumnItem(resolver, item)) {
                    return true;
                }
            }
        }

        return false;
    }
}
