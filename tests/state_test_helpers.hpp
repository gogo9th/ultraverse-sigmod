#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../src/mariadb/DBHandle.hpp"
#include "../src/mariadb/DBHandlePoolAdapter.hpp"
#include "../src/mariadb/state/new/Transaction.hpp"
#include "../src/mariadb/state/new/Query.hpp"
#include "../src/mariadb/state/new/cluster/StateRelationshipResolver.hpp"
#include "../src/mariadb/state/new/graph/RowGraph.hpp"
#include "../src/utils/StringUtil.hpp"

namespace ultraverse::state::v2::test_helpers {
    class NoopRelationshipResolver final : public RelationshipResolver {
    public:
        std::string resolveColumnAlias(const std::string &columnExpr) const override {
            return std::string();
        }

        std::string resolveForeignKey(const std::string &columnExpr) const override {
            return std::string();
        }

        std::shared_ptr<StateItem> resolveRowAlias(const StateItem &item) const override {
            return nullptr;
        }
    };

    class MockedRelationshipResolver final : public RelationshipResolver {
    public:
        void addColumnAlias(const std::string &alias, const std::string &real) {
            _columnAliases[alias] = real;
        }

        void addForeignKey(const std::string &from, const std::string &to) {
            _foreignKeys[from] = to;
        }

        void addRowAlias(const StateItem &alias, const StateItem &real) {
            StateRange range = alias.MakeRange2();
            _rowAliasTable[alias.name].emplace(range, RowAlias{alias, real});
        }

        std::string resolveColumnAlias(const std::string &columnExpr) const override {
            auto it = _columnAliases.find(columnExpr);
            if (it == _columnAliases.end()) {
                return std::string();
            }
            return it->second;
        }

        std::string resolveForeignKey(const std::string &columnExpr) const override {
            auto it = _foreignKeys.find(columnExpr);
            if (it == _foreignKeys.end()) {
                return std::string();
            }
            return it->second;
        }

        std::shared_ptr<StateItem> resolveRowAlias(const StateItem &item) const override {
            auto keyIt = _rowAliasTable.find(item.name);
            if (keyIt == _rowAliasTable.end()) {
                return nullptr;
            }

            StateRange range = item.MakeRange2();
            auto rangeIt = keyIt->second.find(range);
            if (rangeIt == keyIt->second.end()) {
                return nullptr;
            }

            return std::make_shared<StateItem>(rangeIt->second.real);
        }

    private:
        std::unordered_map<std::string, std::string> _columnAliases;
        std::unordered_map<std::string, std::string> _foreignKeys;
        std::unordered_map<std::string, std::unordered_map<StateRange, RowAlias>> _rowAliasTable;
    };

    class MockedDBHandleLease final: public ultraverse::mariadb::DBHandleLeaseBase {
    public:
        MockedDBHandleLease(
            std::shared_ptr<ultraverse::mariadb::MockedDBHandle> handle,
            std::function<void()> releaser
        ):
            _handle(std::move(handle)),
            _releaser(std::move(releaser))
        {
        }

        ~MockedDBHandleLease() override {
            if (_releaser) {
                _releaser();
            }
        }

        ultraverse::mariadb::DBHandle &get() override {
            return *_handle;
        }

    private:
        std::shared_ptr<ultraverse::mariadb::MockedDBHandle> _handle;
        std::function<void()> _releaser;
    };

    class MockedDBHandlePool final: public ultraverse::mariadb::DBHandlePoolBase {
    public:
        MockedDBHandlePool(
            int poolSize,
            std::shared_ptr<ultraverse::mariadb::MockedDBHandle::SharedState> sharedState
        ):
            _poolSize(poolSize),
            _sharedState(std::move(sharedState))
        {
            for (int i = 0; i < poolSize; i++) {
                _handles.push(std::make_shared<ultraverse::mariadb::MockedDBHandle>(_sharedState));
            }
        }

        std::unique_ptr<ultraverse::mariadb::DBHandleLeaseBase> take() override {
            std::unique_lock lock(_mutex);
            _condvar.wait(lock, [this]() { return !_handles.empty(); });

            auto handle = _handles.front();
            _handles.pop();
            lock.unlock();

            return std::make_unique<MockedDBHandleLease>(handle, [this, handle]() {
                std::scoped_lock lock(_mutex);
                _handles.push(handle);
                _condvar.notify_one();
            });
        }

        int poolSize() const override {
            return _poolSize;
        }

    private:
        int _poolSize;
        std::shared_ptr<ultraverse::mariadb::MockedDBHandle::SharedState> _sharedState;
        std::mutex _mutex;
        std::condition_variable _condvar;
        std::queue<std::shared_ptr<ultraverse::mariadb::MockedDBHandle>> _handles;
    };

    inline StateItem makeEq(const std::string &name, int64_t value) {
        return StateItem::EQ(name, StateData{value});
    }

    inline StateItem makeEqStr(const std::string &name, const std::string &value) {
        return StateItem::EQ(name, StateData{value});
    }

    inline StateItem makeBetween(const std::string &name, int64_t begin, int64_t end) {
        StateItem item;
        item.name = name;
        item.function_type = FUNCTION_BETWEEN;
        item.data_list.emplace_back(StateData{begin});
        item.data_list.emplace_back(StateData{end});
        return item;
    }

    inline std::shared_ptr<Query> makeQuery(
        const std::string &db,
        std::vector<StateItem> readItems,
        std::vector<StateItem> writeItems
    ) {
        auto query = std::make_shared<Query>();
        query->setDatabase(db);
        query->readSet().insert(query->readSet().end(), readItems.begin(), readItems.end());
        query->writeSet().insert(query->writeSet().end(), writeItems.begin(), writeItems.end());
        for (const auto &item : query->readSet()) {
            if (!item.name.empty()) {
                query->readColumns().insert(ultraverse::utility::toLower(item.name));
            }
        }
        for (const auto &item : query->writeSet()) {
            if (!item.name.empty()) {
                query->writeColumns().insert(ultraverse::utility::toLower(item.name));
            }
        }
        return query;
    }

    inline std::shared_ptr<Transaction> makeTxn(
        gid_t gid,
        const std::string &db,
        std::vector<StateItem> readItems,
        std::vector<StateItem> writeItems
    ) {
        auto txn = std::make_shared<Transaction>();
        txn->setGid(gid);
        auto query = makeQuery(db, std::move(readItems), std::move(writeItems));
        *txn << query;
        return txn;
    }

    inline bool waitUntilReady(RowGraph &graph, RowGraphId id, std::chrono::milliseconds timeout) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            auto node = graph.nodeFor(id);
            if (node != nullptr && node->ready) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return false;
    }

    inline bool waitUntilAllReady(RowGraph &graph, const std::vector<RowGraphId> &ids, std::chrono::milliseconds timeout) {
        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            bool allReady = true;
            for (auto id : ids) {
                auto node = graph.nodeFor(id);
                if (node == nullptr || !node->ready) {
                    allReady = false;
                    break;
                }
            }
            if (allReady) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return false;
    }

    inline std::unordered_set<gid_t> entrypointGids(RowGraph &graph) {
        std::unordered_set<gid_t> gids;
        auto eps = graph.entrypoints();
        for (auto id : eps) {
            auto node = graph.nodeFor(id);
            if (node != nullptr && node->transaction != nullptr) {
                gids.insert(node->transaction->gid());
            }
        }
        return gids;
    }
}
