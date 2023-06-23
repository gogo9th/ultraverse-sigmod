//
// Created by cheesekun on 6/22/23.
//

#include "StateRelationshipResolver.hpp"

#include "utils/StringUtil.hpp"

namespace ultraverse::state::v2 {
    
    std::string RelationshipResolver::resolveChain(const std::string &columnExpr) const {
        std::string _columnExpr = columnExpr;
        
        while (true) {
            auto alias = std::move(resolveColumnAlias(_columnExpr));
            auto foreignKey = std::move(resolveForeignKey(!alias.empty() ? alias : _columnExpr));
            
            if (!foreignKey.empty()) {
                _columnExpr = std::move(foreignKey);
                continue;
            }
            
            if (!alias.empty()) {
                return std::move(alias);
            } else if (_columnExpr == columnExpr) {
                return std::move(std::string());
            } else {
                return std::move(_columnExpr);
            }
        }
    }
    
    std::shared_ptr<StateItem> RelationshipResolver::resolveRowChain(const StateItem &item) const {
        std::shared_ptr<StateItem> _item = std::make_shared<StateItem>(item);
        
        while (true) {
            auto alias = std::move(resolveRowAlias(*_item));
            auto foreignKey = std::move(resolveForeignKey(alias != nullptr ? alias->name : _item->name));
            
            if (!foreignKey.empty()) {
                // Alias -> FK -> Alias -> Real ...
                // Alias -> FK -> Real
                
                auto fkItem = alias != nullptr ? alias : _item;
                fkItem->name = std::move(foreignKey);
                _item = fkItem;
                continue;
            }
            
            if (alias != nullptr) {
                return std::move(alias);
            } else if (_item->name == item.name) {
                return nullptr;
            } else {
                return std::move(_item);
            }
        }
    }
    
    
    StateRelationshipResolver::StateRelationshipResolver(const StateChangePlan &plan, const StateChangeContext &context):
        _plan(plan),
        _context(context)
    {
    }
    
    std::string StateRelationshipResolver::resolveColumnAlias(const std::string &exprName) const {
        bool found = false;
        std::string _exprName = exprName;
        
        while (true) {
            auto it = std::find_if(
                _plan.columnAliases().begin(), _plan.columnAliases().end(),
                [&_exprName](const auto &pair) { return std::move(utility::toLower(pair.first)) == std::move(utility::toLower(_exprName)); }
            );
            
            if (it == _plan.columnAliases().end()) {
                return found ? std::move(utility::toLower(_exprName)) : std::move(std::string());
            }
            
            found = true;
            _exprName = it->second;
        }
    }
    
    std::string StateRelationshipResolver::resolveForeignKey(const std::string &exprName) const {
        bool found = false;
        std::string _exprName = exprName;
        
        while (true) {
            auto vec = std::move(utility::splitTableName(_exprName));
            auto tableName  = std::move(utility::toLower(vec.first));
            auto columnName = std::move(utility::toLower(vec.second));
            
            auto it = std::find_if(
                _context.foreignKeys.cbegin(), _context.foreignKeys.cend(),
                [&tableName, &columnName](auto &foreignKey) {
                    return foreignKey.fromTable->getCurrentName() == tableName && columnName == foreignKey.fromColumn;
                }
            );
            
            if (it == _context.foreignKeys.cend()) {
                return found ? std::move(utility::toLower(_exprName)) : std::move(std::string());
            }
            
            found = true;
            _exprName = std::move(it->toTable->getCurrentName() + "." + it->toColumn);
        }
    }
    
    std::shared_ptr<StateItem> StateRelationshipResolver::resolveRowAlias(const StateItem &alias) const {
        const auto &name = alias.name;
        const auto &range = alias.MakeRange2();
        
        auto keyIt = _rowAliasTable.find(name);
        
        if (keyIt == _rowAliasTable.end()) {
            return nullptr;
        }
        
        auto &mappingTable = keyIt->second;
        auto it = mappingTable.find(range);
        
        if (it == mappingTable.end()) {
            return nullptr;
        }
        
        return std::make_shared<StateItem>(it->second.real);
    }
    
    void StateRelationshipResolver::addRowAlias(StateItem &alias, StateItem &real) {
        const auto &name = alias.name;
        const auto &range = alias.MakeRange2();
        
        _rowAliasTable[name].insert(std::make_pair(range, RowAlias { alias, real }));
    }
    
    CachedRelationshipResolver::CachedRelationshipResolver(const RelationshipResolver &resolver, int maxRowElements):
        _resolver(resolver),
        _maxRowElements(maxRowElements)
    {
    }
    
    std::string CachedRelationshipResolver::resolveColumnAlias(const std::string &columnExpr) const {
        _cacheLock.lock();
        auto it = _aliasCache.find(columnExpr);
        bool found = it != _aliasCache.end();
        _cacheLock.unlock();
        
        if (!found) {
            auto retval = std::move(_resolver.resolveColumnAlias(columnExpr));
            
            std::scoped_lock _lock(_cacheLock);
            _aliasCache.emplace(columnExpr, retval);
            
            return std::move(retval);
        }
        
        return std::move(it->second);
    }
    
    std::string CachedRelationshipResolver::resolveForeignKey(const std::string &columnExpr) const {
        return _resolver.resolveForeignKey(columnExpr);
    }
    
    std::string CachedRelationshipResolver::resolveChain(const std::string &columnExpr) const {
        _cacheLock.lock();
        auto it = _chainCache.find(columnExpr);
        bool found = it != _chainCache.end();
        _cacheLock.unlock();
        
        if (!found) {
            auto retval = std::move(_resolver.resolveColumnAlias(columnExpr));
            
            std::scoped_lock _lock(_cacheLock);
            _chainCache.emplace(columnExpr, retval);
            
            return std::move(retval);
        }
        
        return std::move(it->second);
    }
    
    std::shared_ptr<StateItem> CachedRelationshipResolver::resolveRowAlias(const StateItem &item) const {
        size_t hash = item.MakeRange2().hash();
        auto &cacheMap = _rowAliasCache[item.name];
        
        {
            _cacheLock.lock();
            auto it = cacheMap.find(hash);
            _cacheLock.unlock();
            
            if (it == cacheMap.end()) {
                goto NOT_FOUND;
            }
            
            return it->second.second;
        }
        
        NOT_FOUND:
        auto retval = std::move(_resolver.resolveRowAlias(item));
        
        {
            std::scoped_lock _lock(_cacheLock);
            
            if (isGCRequired(cacheMap)) {
                gc(cacheMap);
            }
            
            cacheMap.emplace(hash, std::make_pair(1, retval));
        }
        
        return std::move(retval);
    }
    
    std::shared_ptr<StateItem> CachedRelationshipResolver::resolveRowChain(const StateItem &item) const {
        size_t hash = item.MakeRange2().hash();
        auto &cacheMap = _rowAliasCache[item.name];
        
        {
            _cacheLock.lock();
            auto it = cacheMap.find(hash);
            _cacheLock.unlock();
            
            if (it == cacheMap.end()) {
                goto NOT_FOUND;
            }
            
            return it->second.second;
        }
        
        NOT_FOUND:
        auto retval = std::move(_resolver.resolveRowChain(item));
        
        {
            std::scoped_lock _lock(_cacheLock);
            
            if (isGCRequired(cacheMap)) {
                gc(cacheMap);
            }
            
            cacheMap.emplace(hash, std::make_pair(1, retval));
        }
        
        return std::move(retval);
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
        /*
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
         */
    }
}