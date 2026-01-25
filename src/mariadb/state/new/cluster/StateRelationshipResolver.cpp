//
// Created by cheesekun on 6/22/23.
//

#include "StateRelationshipResolver.hpp"

#include <unordered_set>

#include "utils/StringUtil.hpp"

#include "ultraverse_state.pb.h"

namespace ultraverse::state::v2 {

    void RowAlias::toProtobuf(ultraverse::state::v2::proto::RowAlias *out) const {
        if (out == nullptr) {
            return;
        }

        out->Clear();
        alias.toProtobuf(out->mutable_alias());
        real.toProtobuf(out->mutable_real());
    }

    void RowAlias::fromProtobuf(const ultraverse::state::v2::proto::RowAlias &msg) {
        alias.fromProtobuf(msg.alias());
        real.fromProtobuf(msg.real());
    }
    
    std::string RelationshipResolver::resolveChain(const std::string &columnExpr) const {
        std::string _columnExpr = columnExpr;
        std::unordered_set<std::string> visited;
        
        while (true) {
            if (!visited.insert(_columnExpr).second) {
                return std::string();
            }
            
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
        std::unordered_set<std::string> visited;
        
        while (true) {
            if (!visited.insert(_item->name).second) {
                return nullptr;
            }
            
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
        std::string _exprName = utility::toLower(exprName);
        std::unordered_set<std::string> visited;
        
        while (true) {
            if (!visited.insert(_exprName).second) {
                return std::string();
            }
            
            auto it = std::find_if(
                _plan.columnAliases().begin(), _plan.columnAliases().end(),
                [&_exprName](const auto &pair) { return std::move(utility::toLower(pair.first)) == _exprName; }
            );
            
            if (it == _plan.columnAliases().end()) {
                return found ? std::move(_exprName) : std::move(std::string());
            }
            
            found = true;
            _exprName = utility::toLower(it->second);
        }
    }
    
    std::string StateRelationshipResolver::resolveForeignKey(const std::string &exprName) const {
        bool found = false;
        std::string _exprName = utility::toLower(exprName);
        std::unordered_set<std::string> visited;
        
        while (true) {
            if (!visited.insert(_exprName).second) {
                return std::string();
            }
            
            auto vec = std::move(utility::splitTableName(_exprName));
            auto tableName  = std::move(vec.first);
            auto columnName = std::move(vec.second);
            
            auto it = std::find_if(
                _context.foreignKeys.cbegin(), _context.foreignKeys.cend(),
                [&tableName, &columnName](auto &foreignKey) {
                    return utility::toLower(foreignKey.fromTable->getCurrentName()) == tableName
                        && utility::toLower(foreignKey.fromColumn) == columnName;
                }
            );
            
            if (it == _context.foreignKeys.cend()) {
                return found ? std::move(_exprName) : std::move(std::string());
            }
            
            found = true;
            _exprName = std::move(utility::toLower(it->toTable->getCurrentName() + "." + it->toColumn));
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
    
    void StateRelationshipResolver::addRowAlias(const StateItem &alias, const StateItem &real) {
        const auto &name = alias.name;
        const auto &range = alias.MakeRange2();
        
        _rowAliasTable[name].insert(std::make_pair(range, RowAlias { alias, real }));
    }
    
    bool StateRelationshipResolver::addTransaction(Transaction &transaction) {
        bool changed = false;
        for (const auto &pair: _plan.columnAliases()) {
            const auto &alias = pair.first;
            const auto &real = pair.second;
            
            auto itBegin = transaction.writeSet_begin();
            auto itEnd = transaction.writeSet_end();
            
            auto aliasIt = std::find_if(itBegin, itEnd, [alias](const auto &item) { return item.name == alias; });
            auto itemIt = std::find_if(itBegin, itEnd, [real](const auto &item) { return item.name == real; });
            
            if (aliasIt != itEnd && itemIt != itEnd) {
                // std::cerr << "adding alias: " << (*aliasIt).MakeRange2().MakeWhereQuery((*aliasIt).name) << " => " << (*itemIt).MakeRange2().MakeWhereQuery((*itemIt).name) << std::endl;
                addRowAlias(*aliasIt, *itemIt);
                changed = true;
            }
        }
        return changed;
    }
    
    CachedRelationshipResolver::CachedRelationshipResolver(const RelationshipResolver &resolver, int maxRowElements):
        _resolver(resolver),
        _maxRowElements(maxRowElements)
    {
    }
    
    std::string CachedRelationshipResolver::resolveColumnAlias(const std::string &columnExpr) const {
        {
            std::shared_lock<std::shared_mutex> _lock(_cacheLock);
            auto it = _aliasCache.find(columnExpr);
            if (it != _aliasCache.end()) {
                return it->second;
            }
        }
        
        auto retval = _resolver.resolveColumnAlias(columnExpr);
        
        {
            std::unique_lock<std::shared_mutex> _lock(_cacheLock);
            _aliasCache.emplace(columnExpr, retval);
        }
        
        return retval;
    }
    
    std::string CachedRelationshipResolver::resolveForeignKey(const std::string &columnExpr) const {
        return _resolver.resolveForeignKey(columnExpr);
    }
    
    std::string CachedRelationshipResolver::resolveChain(const std::string &columnExpr) const {
        {
            std::shared_lock<std::shared_mutex> _lock(_cacheLock);
            auto it = _chainCache.find(columnExpr);
            if (it != _chainCache.end()) {
                return it->second;
            }
        }
        
        auto retval = _resolver.resolveChain(columnExpr);
        
        {
            std::unique_lock<std::shared_mutex> _lock(_cacheLock);
            _chainCache.emplace(columnExpr, retval);
        }
        
        return retval;
    }
    
    std::shared_ptr<StateItem> CachedRelationshipResolver::resolveRowAlias(const StateItem &item) const {
        size_t hash = item.MakeRange2().hash();
        
        {
            std::shared_lock<std::shared_mutex> _lock(_cacheLock);
            auto outerIt = _rowAliasCache.find(item.name);
            if (outerIt != _rowAliasCache.end()) {
                auto it = outerIt->second.find(hash);
                if (it != outerIt->second.end()) {
                    auto cached = it->second.second;
                    _lock.unlock();
                    {
                        std::unique_lock<std::shared_mutex> _writeLock(_cacheLock);
                        auto outerIt2 = _rowAliasCache.find(item.name);
                        if (outerIt2 != _rowAliasCache.end()) {
                            auto it2 = outerIt2->second.find(hash);
                            if (it2 != outerIt2->second.end()) {
                                it2->second.first++;
                            }
                        }
                    }
                    return cached;
                }
            }
        }
        
        auto retval = _resolver.resolveRowAlias(item);
        
        {
            std::unique_lock<std::shared_mutex> _lock(_cacheLock);
            auto &cacheMap = _rowAliasCache[item.name];
            
            if (isGCRequired(cacheMap)) {
                gc(cacheMap);
            }
            
            cacheMap.emplace(hash, std::make_pair(1, retval));
        }
        
        return retval;
    }
    
    std::shared_ptr<StateItem> CachedRelationshipResolver::resolveRowChain(const StateItem &item) const {
        size_t hash = item.MakeRange2().hash();
        
        {
            std::shared_lock<std::shared_mutex> _lock(_cacheLock);
            auto outerIt = _rowChainCache.find(item.name);
            if (outerIt != _rowChainCache.end()) {
                auto it = outerIt->second.find(hash);
                if (it != outerIt->second.end()) {
                    auto cached = it->second.second;
                    _lock.unlock();
                    {
                        std::unique_lock<std::shared_mutex> _writeLock(_cacheLock);
                        auto outerIt2 = _rowChainCache.find(item.name);
                        if (outerIt2 != _rowChainCache.end()) {
                            auto it2 = outerIt2->second.find(hash);
                            if (it2 != outerIt2->second.end()) {
                                it2->second.first++;
                            }
                        }
                    }
                    return cached;
                }
            }
        }
        
        auto retval = _resolver.resolveRowChain(item);
        
        {
            std::unique_lock<std::shared_mutex> _lock(_cacheLock);
            auto &cacheMap = _rowChainCache[item.name];
            
            if (isGCRequired(cacheMap)) {
                gc(cacheMap);
            }
            
            cacheMap.emplace(hash, std::make_pair(1, retval));
        }
        
        return retval;
    }
    
    void CachedRelationshipResolver::clearCache() {
        std::unique_lock<std::shared_mutex> _lock(_cacheLock);
        _aliasCache.clear();
        _chainCache.clear();
        _rowAliasCache.clear();
        _rowChainCache.clear();
    }
    
    bool CachedRelationshipResolver::isGCRequired(const CachedRelationshipResolver::RowCacheMap &rowCacheMap) const {
        return rowCacheMap.size() > _maxRowElements;
    }
    
    void CachedRelationshipResolver::gc(CachedRelationshipResolver::RowCacheMap &rowCacheMap) {
        /*
        std::cerr << "performing gc" << std::endl;
        rowCacheMap.clear();
        
        std::vector<size_t> keys;
        keys.reserve(rowCacheMap.size());
        
        
        std::transform(
            rowCacheMap.begin(), rowCacheMap.end(), std::back_inserter(keys),
            [](auto &pair) -> size_t { return pair.first; }
        );
        
        std::sort(
            keys.begin(), keys.end(),
            [&rowCacheMap](const auto &lhs, const auto &rhs) {
                return rowCacheMap.at(lhs).first < rowCacheMap.at(rhs).first;
            }
        );
        
        // 하위 5% 제거
        int keysToRemove = (int) ((double) keys.size() * 0.05);
        for (int i = 0; i <= keysToRemove; i++) {
            rowCacheMap.erase(keys[i]);
        }
        
        // 모든 카운터를 0으로 리셋 (다음 GC를 위해)
        for (auto &pair: rowCacheMap) {
            pair.second.first = 0;
        }
         */
    }
}
