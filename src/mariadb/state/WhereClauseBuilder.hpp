#ifndef ULTRAVERSE_WHERE_CLAUSE_BUILDER_HPP
#define ULTRAVERSE_WHERE_CLAUSE_BUILDER_HPP

#include <functional>
#include <string>
#include <vector>

#include <ultparser_query.pb.h>

#include "mariadb/state/StateItem.h"
#include "utils/log.hpp"

namespace ultraverse::state {

struct WhereClauseOptions {
    std::string primaryTable;
    std::vector<std::string> tableNames;
    LoggerPtr logger;

    std::function<void(const std::string&)> onReadColumn;
    std::function<void(const std::string&, const ultparser::DMLQueryExpr&)> onValueExpr;
    std::function<bool(const std::string&,
                       const std::string&,
                       std::vector<StateData>&)> resolveIdentifier;
    std::function<bool(const std::string&,
                       const std::string&,
                       std::vector<std::string>&)> resolveColumnIdentifier;
    std::function<void(const std::string&, const std::string&)> onUnresolvedIdentifier;
};

std::vector<StateItem> buildWhereItems(const ultparser::DMLQueryExpr& expr,
                                       const WhereClauseOptions& options);

} // namespace ultraverse::state

#endif // ULTRAVERSE_WHERE_CLAUSE_BUILDER_HPP
