//
// Created by cheesekun on 6/22/23.
//

#include "StateRelationshipResolver.hpp"

#include "utils/StringUtil.hpp"

namespace ultraverse::state::v2 {
    StateRelationshipResolver::StateRelationshipResolver(const StateChangePlan &plan, const StateChangeContext &context):
        _plan(plan),
        _context(context)
    {
    }
    
    std::optional<std::string> StateRelationshipResolver::resolveColumnAlias(const std::string &exprName) const {
        bool found = false;
        std::string _exprName = exprName;
        
        while (true) {
            auto it = std::find_if(
                _plan.columnAliases().begin(), _plan.columnAliases().end(),
                [&_exprName](const auto &pair) { return utility::toLower(pair.first) == utility::toLower(_exprName); }
            );
            
            if (it == _plan.columnAliases().end()) {
                return found ? std::make_optional(utility::toLower(_exprName)) : std::nullopt;
            }
            
            found = true;
            _exprName = it->second;
        }
    }
    
    std::optional<std::string> StateRelationshipResolver::resolveForeignKey(const std::string &exprName) const {
        bool found = false;
        std::string _exprName = exprName;
        
        while (true) {
            auto vec = utility::splitTableName(_exprName);
            auto tableName  = std::move(utility::toLower(vec.first));
            auto columnName = std::move(utility::toLower(vec.second));
            
            auto it = std::find_if(
                _context.foreignKeys.cbegin(), _context.foreignKeys.cend(),
                [&tableName, &columnName](auto &foreignKey) {
                    return foreignKey.fromTable->getCurrentName() == tableName && columnName == foreignKey.fromColumn;
                }
            );
            
            if (it == _context.foreignKeys.cend()) {
                return found ? std::make_optional(utility::toLower(_exprName)) : std::nullopt;
            }
            
            found = true;
            _exprName = it->toTable->getCurrentName() + "." + it->toColumn;
        }
    }
    
    std::optional<StateItem> StateRelationshipResolver::resolveRowAlias(const StateItem &alias) const {
        const auto &name = alias.name;
        auto range = alias.MakeRange2();
        
        auto keyIt = _rowAliasTable.find(name);
        
        if (keyIt == _rowAliasTable.end()) {
            return std::nullopt;
        }
        
        auto &mappingTable = keyIt->second;
        auto it = mappingTable.find(range);
        
        if (it == mappingTable.end()) {
            return std::nullopt;
        }
        
        return std::make_optional<StateItem>(it->second.real);
    }
    
    void StateRelationshipResolver::addRowAlias(StateItem &alias, StateItem &real) {
        const auto &name = alias.name;
        auto range = alias.MakeRange2();
        
        _rowAliasTable[name].insert(std::make_pair(range, RowAlias { alias, real }));
    }
}