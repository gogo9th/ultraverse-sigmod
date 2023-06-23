//
// Created by cheesekun on 6/22/23.
//

#include "StateRelationshipResolver.hpp"

#include "utils/StringUtil.hpp"

namespace ultraverse::state::v2 {
    
    std::optional<std::string> RelationshipResolver::resolveChain(const std::string &columnExpr) const {
        std::string _columnExpr = columnExpr;
        
        while (true) {
            auto alias = resolveColumnAlias(_columnExpr);
            auto foreignKey = resolveForeignKey(alias.has_value() ? alias.value() : _columnExpr);
            
            if (foreignKey.has_value()) {
                _columnExpr = foreignKey.value();
                continue;
            }
            
            if (alias.has_value()) {
                return alias.value();
            } else if (_columnExpr == columnExpr) {
                return std::nullopt;
            } else {
                return _columnExpr;
            }
        }
    }
    
    std::optional<StateItem> RelationshipResolver::resolveRowChain(const StateItem &item) const {
        StateItem _item = item;
        
        while (true) {
            auto alias = resolveRowAlias(_item);
            auto foreignKey = resolveForeignKey(alias.has_value() ? alias.value().name : _item.name);
            
            if (foreignKey.has_value()) {
                // Alias -> FK -> Alias -> Real ...
                // Alias -> FK -> Real
                
                auto fkItem = alias.has_value() ? alias.value() : _item;
                fkItem.name = foreignKey.value();
                _item = fkItem;
                continue;
            }
            
            if (alias.has_value()) {
                return alias.value();
            } else if (_item.name == item.name) {
                return std::nullopt;
            } else {
                return _item;
            }
        }
    }
    
    
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
    
    CachedRelationshipResolver::CachedRelationshipResolver(const RelationshipResolver &resolver, int maxRowElements):
        _resolver(resolver),
        _maxRowElements(maxRowElements)
    {
    }
    
    std::optional<std::string> CachedRelationshipResolver::resolveColumnAlias(const std::string &columnExpr) const {
        _cacheLock.lock();
        auto it = _aliasCache.find(columnExpr);
        bool found = it != _aliasCache.end();
        _cacheLock.unlock();
        
        if (!found) {
            auto retval = _resolver.resolveColumnAlias(columnExpr);
            if (retval.has_value()) {
                std::scoped_lock _lock(_cacheLock);
                _aliasCache.emplace(columnExpr, retval.value());
            }
            
            return retval;
        }
        
        return it->second;
    }
    
    std::optional<std::string> CachedRelationshipResolver::resolveForeignKey(const std::string &columnExpr) const {
        return _resolver.resolveForeignKey(columnExpr);
    }
    
    std::optional<std::string> CachedRelationshipResolver::resolveChain(const std::string &columnExpr) const {
        _cacheLock.lock();
        auto it = _chainCache.find(columnExpr);
        bool found = it != _chainCache.end();
        _cacheLock.unlock();
        
        if (!found) {
            auto retval = _resolver.resolveColumnAlias(columnExpr);
            if (retval.has_value()) {
                std::scoped_lock _lock(_cacheLock);
                _chainCache.emplace(columnExpr, retval.value());
            }
            
            return retval;
        }
        
        return it->second;
    }
    
    std::optional<StateItem> CachedRelationshipResolver::resolveRowAlias(const StateItem &item) const {
        auto range = item.MakeRange2();
        
        _cacheLock.lock();
        auto &cacheMap = _rowAliasCache[item.name];
        auto it = cacheMap.find(range);
        bool found = it != cacheMap.end();
        _cacheLock.unlock();
        
        if (!found) {
            auto retval = RelationshipResolver::resolveRowAlias(item);
            
            if (retval.has_value()) {
                std::scoped_lock _lock(_cacheLock);
                
                if (isGCRequired(cacheMap)) {
                    gc(cacheMap);
                }
                
                cacheMap.emplace(range, std::make_pair(1, retval.value()));
            }
        }
        
        return it->second.second;
    }
    
    std::optional<StateItem> CachedRelationshipResolver::resolveRowChain(const StateItem &item) const {
        auto range = item.MakeRange2();
        
        _cacheLock.lock();
        auto &cacheMap = _rowChainCache[item.name];
        auto it = cacheMap.find(range);
        bool found = it != cacheMap.end();
        _cacheLock.unlock();
        
        if (!found) {
            auto retval = RelationshipResolver::resolveRowAlias(item);
            
            if (retval.has_value()) {
                std::scoped_lock _lock(_cacheLock);
                
                if (isGCRequired(cacheMap)) {
                    gc(cacheMap);
                }
                
                cacheMap.emplace(range, std::make_pair(1, retval.value()));
            }
        }
        
        return it->second.second;

    }
    
    void CachedRelationshipResolver::clearCache() {
        std::scoped_lock _lock(_cacheLock);
        _aliasCache.clear();
        _chainCache.clear();
        _rowAliasCache.clear();
    }
    
    bool CachedRelationshipResolver::isGCRequired(const CachedRelationshipResolver::RowCacheMap &rowCacheMap) const {
        return rowCacheMap.size() > _maxRowElements;
    }
    
    void CachedRelationshipResolver::gc(CachedRelationshipResolver::RowCacheMap &rowCacheMap) {
        std::vector<const StateRange *> keys;
        keys.reserve(rowCacheMap.size());
        
        std::transform(
            rowCacheMap.begin(), rowCacheMap.end(), std::back_inserter(keys),
            [](auto &pair) -> const StateRange * { return &pair.first; }
        );
        
        std::sort(
            keys.begin(), keys.end(),
            [&rowCacheMap](const auto &lhs, const auto &rhs) {
                return rowCacheMap.at(*lhs).first < rowCacheMap.at(*rhs).first;
            }
        );
        
        // 하위 5% 제거
        int keysToRemove = (int) ((double) keys.size() * 0.05);
        for (int i = 0; i <= keysToRemove; i++) {
            rowCacheMap.erase(*keys[i]);
        }
        
        // 모든 카운터를 0으로 리셋 (다음 GC를 위해)
        for (auto &pair: rowCacheMap) {
            pair.second.first = 0;
        }
    }
}