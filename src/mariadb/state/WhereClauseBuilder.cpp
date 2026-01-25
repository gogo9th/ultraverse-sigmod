#include "mariadb/state/WhereClauseBuilder.hpp"

#include <iterator>
#include <stdexcept>

#include "utils/StringUtil.hpp"

namespace ultraverse::state {

namespace {

void processRValue(StateItem& item,
                   const ultparser::DMLQueryExpr& right,
                   const WhereClauseOptions& options,
                   std::vector<StateItem>& extraItems) {
    if (right.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
        const std::string& identifierName = right.identifier();
        std::vector<StateData> values;
        if (options.resolveIdentifier &&
            options.resolveIdentifier(item.name, identifierName, values)) {
            item.data_list.insert(item.data_list.end(), values.begin(), values.end());
        } else {
            std::vector<std::string> columns;
            if (options.resolveColumnIdentifier &&
                options.resolveColumnIdentifier(item.name, identifierName, columns) &&
                !columns.empty()) {
                item.function_type = FUNCTION_WILDCARD;
                item.data_list.clear();
                for (const auto &column : columns) {
                    if (options.onReadColumn) {
                        options.onReadColumn(column);
                    }
                    extraItems.emplace_back(StateItem::Wildcard(column));
                }
            } else {
                if (options.logger) {
                    options.logger->warn("cannot map value for {}", item.name);
                }
                if (options.onUnresolvedIdentifier) {
                    options.onUnresolvedIdentifier(identifierName, item.name);
                }
            }
        }
        return;
    }

    switch (right.value_type()) {
        case ultparser::DMLQueryExpr::IDENTIFIER:
            return;
        case ultparser::DMLQueryExpr::INTEGER: {
            StateData data;
            data.Set(right.integer());
            item.data_list.emplace_back(std::move(data));
        } break;
        case ultparser::DMLQueryExpr::DOUBLE: {
            StateData data;
            data.Set(right.double_());
            item.data_list.emplace_back(std::move(data));
        } break;
        case ultparser::DMLQueryExpr::DECIMAL: {
            StateData data;
            data.SetDecimal(right.decimal());
            item.data_list.emplace_back(std::move(data));
        } break;
        case ultparser::DMLQueryExpr::STRING: {
            StateData data;
            data.Set(right.string().c_str(), right.string().size());
            item.data_list.emplace_back(std::move(data));
        } break;
        case ultparser::DMLQueryExpr::BOOL: {
            StateData data;
            data.Set(right.bool_() ? static_cast<int64_t>(1) : 0);
            item.data_list.emplace_back(std::move(data));
        } break;
        case ultparser::DMLQueryExpr::NULL_: {
            if (options.logger) {
                options.logger->error("putting NULL value in StateData is not supported yet");
            }
            throw std::runtime_error("putting NULL value in StateData is not supported yet");
        } break;
        case ultparser::DMLQueryExpr::LIST: {
            for (const auto& child : right.value_list()) {
                processRValue(item, child, options, extraItems);
            }
        } break;
        case ultparser::DMLQueryExpr::FUNCTION: {
            if (options.logger) {
                options.logger->trace("processing function rvalue for {}", item.name);
            }
            const auto tablePair = utility::splitTableName(item.name);
            if (options.onValueExpr) {
                options.onValueExpr(tablePair.first, right);
            }
        } break;
        case ultparser::DMLQueryExpr::SUBQUERY: {
            if (!right.has_subquery()) {
                if (options.logger) {
                    options.logger->warn("subquery rvalue has no payload for {}", item.name);
                }
                return;
            }
            if (options.logger) {
                options.logger->debug("processing subquery rvalue for {}", item.name);
            }
            const auto tablePair = utility::splitTableName(item.name);
            if (options.onValueExpr) {
                options.onValueExpr(tablePair.first, right);
            }
        } break;
        default:
            throw std::runtime_error("unsupported right side of where expression");
    }
}

} // namespace

std::vector<StateItem> buildWhereItems(const ultparser::DMLQueryExpr& expr,
                                       const WhereClauseOptions& options) {
    std::vector<StateItem> items;
    std::vector<StateItem> extraItems;

    std::function<void(const ultparser::DMLQueryExpr&, StateItem&)> visitNode;
    visitNode = [&options, &visitNode, &extraItems](const ultparser::DMLQueryExpr& node, StateItem& parent) {
        if (node.value_type() == ultparser::DMLQueryExpr::SUBQUERY) {
            if (options.logger) {
                options.logger->debug("where clause contains subquery expression");
            }
            if (options.onValueExpr) {
                options.onValueExpr(options.primaryTable, node);
            }
            return;
        }

        if (node.operator_() == ultparser::DMLQueryExpr::AND ||
            node.operator_() == ultparser::DMLQueryExpr::OR) {
            parent.condition_type = node.operator_() == ultparser::DMLQueryExpr::AND
                                        ? EN_CONDITION_AND
                                        : EN_CONDITION_OR;

            for (const auto& child : node.expressions()) {
                StateItem item;
                visitNode(child, item);
                parent.arg_list.emplace_back(std::move(item));
            }
            return;
        }

        if (!node.has_left() ||
            node.left().value_type() != ultparser::DMLQueryExpr::IDENTIFIER) {
            if (options.logger) {
                options.logger->warn("left side of where expression is not an identifier");
            }
            return;
        }

        std::string left = utility::toLower(node.left().identifier());
        if (left.find('.') == std::string::npos) {
            left = options.primaryTable + "." + left;
        }

        parent.name = left;

        switch (node.operator_()) {
            case ultparser::DMLQueryExpr_Operator_EQ:
                parent.function_type = FUNCTION_EQ;
                break;
            case ultparser::DMLQueryExpr_Operator_NEQ:
                parent.function_type = FUNCTION_NE;
                break;
            case ultparser::DMLQueryExpr_Operator_LT:
                parent.function_type = FUNCTION_LT;
                break;
            case ultparser::DMLQueryExpr_Operator_LTE:
                parent.function_type = FUNCTION_LE;
                break;
            case ultparser::DMLQueryExpr_Operator_GT:
                parent.function_type = FUNCTION_GT;
                break;
            case ultparser::DMLQueryExpr_Operator_GTE:
                parent.function_type = FUNCTION_GE;
                break;
            case ultparser::DMLQueryExpr_Operator_LIKE:
                if (options.logger) {
                    options.logger->warn("LIKE operator is not supported yet");
                }
                parent.function_type = FUNCTION_EQ;
                break;
            case ultparser::DMLQueryExpr_Operator_NOT_LIKE:
                if (options.logger) {
                    options.logger->warn("NOT LIKE operator is not supported yet");
                }
                parent.function_type = FUNCTION_NE;
                break;
            case ultparser::DMLQueryExpr_Operator_IN:
                parent.function_type = FUNCTION_EQ;
                break;
            case ultparser::DMLQueryExpr_Operator_NOT_IN:
                parent.function_type = FUNCTION_NE;
                break;
            case ultparser::DMLQueryExpr_Operator_BETWEEN:
                parent.function_type = FUNCTION_EQ;
                break;
            case ultparser::DMLQueryExpr_Operator_NOT_BETWEEN:
                parent.function_type = FUNCTION_NE;
                break;
            default:
                if (options.logger) {
                    options.logger->warn("unsupported operator: {}", static_cast<int>(node.operator_()));
                }
                return;
        }

        if (node.has_right()) {
            processRValue(parent, node.right(), options, extraItems);
        }

        if (options.onReadColumn) {
            options.onReadColumn(left);
        }
    };

    std::function<void(StateItem&)> flatInsertNode;
    flatInsertNode = [&items, &flatInsertNode](StateItem& item) {
        if (item.condition_type == EN_CONDITION_AND ||
            item.condition_type == EN_CONDITION_OR) {
            for (auto& child : item.arg_list) {
                flatInsertNode(child);
            }
        } else {
            items.emplace_back(item);
        }
    };

    StateItem rootItem;
    visitNode(expr, rootItem);
    flatInsertNode(rootItem);

    if (!extraItems.empty()) {
        items.insert(items.end(),
                     std::make_move_iterator(extraItems.begin()),
                     std::make_move_iterator(extraItems.end()));
    }

    return items;
}

} // namespace ultraverse::state
