//
// Created by cheesekun on 6/20/23.
//

#define CATCH_CONFIG_MAIN

#include <memory>

#include <catch2/catch.hpp>

#include "../src/mariadb/state/new/Query.hpp"
#include "../src/mariadb/state/new/Transaction.hpp"
#include "../src/mariadb/state/new/cluster/StateCluster.hpp"

using namespace ultraverse::state::v2;

/**
 * 유닛 테스트를 위한 RelationshipResolver 구현체 (mocked)
 */
class MockedRelationshipResolver: public RelationshipResolver {
public:
    std::optional<std::string> resolveColumnAlias(const std::string &columnExpr) const override {
        static const std::map<std::string, std::string> aliases {
            { "users.id_str", "users.id" },
            { "posts.uuid", "posts.id" }
        };
        
        auto it = aliases.find(columnExpr);
        
        if (it == aliases.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }
    
    std::optional<std::string> resolveForeignKey(const std::string &columnExpr) const override {
        static const std::map<std::string, std::string> foreignKeys {
            { "posts.author", "users.id" },
            { "posts.author_str", "users.id_str" }
        };
        
        auto it = foreignKeys.find(columnExpr);
        
        if (it == foreignKeys.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }
    
    std::optional<StateItem> resolveRowAlias(const StateItem &item) const override {
        static const std::map<
            std::string,
            std::unordered_map<StateRange, RowAlias>
        > rowAliasTable {
            {
                "users.id_str",
                {
                    {
                        StateRange { "000001" },
                        RowAlias { StateItem::EQ("users.id_str", StateData { "000001" }), StateItem::EQ("users.id", StateData { (int64_t) 1 }) }
                    }
                }
            },
            {
                "posts.uuid",
                {
                    {
                        StateRange { "4443d265-fb0a-4dca-8f71-e82b176118df" },
                        RowAlias { StateItem::EQ("posts.uuid", StateData { "4443d265-fb0a-4dca-8f71-e82b176118df" }), StateItem::EQ("posts.id", StateData { (int64_t) 1 }) }
                    }
                }
            }
        };
        
        auto keyIt = rowAliasTable.find(item.name);
        
        if (keyIt == rowAliasTable.end()) {
            return std::nullopt;
        }
        
        auto rangeIt = keyIt->second.find(item.MakeRange2());
        
        if (rangeIt == keyIt->second.end()) {
            return std::nullopt;
        }
        
        return rangeIt->second.real;
    }
};

std::shared_ptr<Transaction> sampleTransaction1() {
    auto transaction = std::make_shared<Transaction>();
    transaction->setGid(1);
    
    std::shared_ptr<Query> query1 = std::make_shared<Query>();
    {
        query1->setDatabase("test");
        query1->setStatement("INSERT INTO `users` (`name`, `id_str`) VALUES ('cheesekun', '000001')");
        
        query1->setAffectedRows(1);
        
        query1->itemSet().emplace_back(StateItem::EQ("users.id", (int64_t) 1));
        query1->itemSet().emplace_back(StateItem::EQ("users.name", StateData { "cheesekun" }));
        query1->itemSet().emplace_back(StateItem::EQ("users.id_str", StateData { "000001" }));
    }
    
    *transaction << query1;
    
    return transaction;
}

std::shared_ptr<Transaction> sampleTransaction2() {
    auto transaction = std::make_shared<Transaction>();
    transaction->setGid(2);
    
    std::shared_ptr<Query> query2 = std::make_shared<Query>();
    {
        query2->setDatabase("test");
        query2->setStatement("UPDATE `users` SET `name` = 'test1234' WHERE `id` = 1");
        query2->setAffectedRows(1);
        
        query2->itemSet().emplace_back(StateItem::EQ("users.id", (int64_t) 1));
        query2->itemSet().emplace_back(StateItem::EQ("users.name", StateData { "test1234" }));
        query2->itemSet().emplace_back(StateItem::EQ("users.id_str", StateData { "000001" }));
        
        query2->whereSet().emplace_back(StateItem::EQ("users.id", (int64_t) 1));
    }
    
    *transaction << query2;
    
    return transaction;
}


std::shared_ptr<Transaction> sampleTransaction3() {
    auto transaction = std::make_shared<Transaction>();
    transaction->setGid(3);
    
    std::shared_ptr<Query> query = std::make_shared<Query>();
    {
        query->setDatabase("test");
        query->setStatement("INSERT INTO `posts` (author, author_str, uuid, content) VALUES (1, '000001', '4443d265-fb0a-4dca-8f71-e82b176118df', '집가고 싶어 ㅠㅠ')");
        query->setAffectedRows(1);
        
        query->itemSet().emplace_back(StateItem::EQ("posts.id", (int64_t) 1));
        query->itemSet().emplace_back(StateItem::EQ("posts.author", (int64_t) 1));
        query->itemSet().emplace_back(StateItem::EQ("posts.author_str", StateData { "000001" }));
        query->itemSet().emplace_back(StateItem::EQ("posts.uuid", StateData { "4443d265-fb0a-4dca-8f71-e82b176118df" }));
        query->itemSet().emplace_back(StateItem::EQ("posts.content", StateData { "집가고 싶어 ㅠㅠ" }));
    }
    
    *transaction << query;
    
    return transaction;
}


std::shared_ptr<Transaction> sampleTransaction4() {
    auto transaction = std::make_shared<Transaction>();
    transaction->setGid(4);
    
    std::shared_ptr<Query> query = std::make_shared<Query>();
    {
        query->setDatabase("test");
        query->setStatement("INSERT INTO `posts` (author, author_str, uuid, content) VALUES (1, '000001', '4443d265-fb0a-4dca-8f71-e82b176118df', '집가고 싶어 ㅠㅠ')");
        query->setAffectedRows(1);
        
        query->itemSet().emplace_back(StateItem::EQ("posts.id", (int64_t) 1));
        query->itemSet().emplace_back(StateItem::EQ("posts.author", (int64_t) 1));
        query->itemSet().emplace_back(StateItem::EQ("posts.author_str", StateData { "000001" }));
        query->itemSet().emplace_back(StateItem::EQ("posts.uuid", StateData { "4443d265-fb0a-4dca-8f71-e82b176118df" }));
        query->itemSet().emplace_back(StateItem::EQ("posts.content", StateData { "집가고 싶어 ㅠㅠ" }));
    }
    
    *transaction << query;
    
    return transaction;
}

TEST_CASE("StateItem::MakeRange2() should create StateRange correctly", "[StateItem]") {
    StateRange randomRange { (int64_t) 0xCAFEBABE };
    
    SECTION("when function_type is FUNCTION_EQ, with single value") {
        StateItem item;
        item.name = "users.id";
        item.function_type = FUNCTION_EQ;
        
        StateData data;
        data.Set((int64_t) 1);
        
        item.data_list.emplace_back(data);
        
        auto range = item.MakeRange2();
        StateRange expectedRange { (int64_t) 1 };
        
        REQUIRE(range == expectedRange);
        REQUIRE(range != randomRange);
    }
    
    SECTION("when function_type is FUNCTION_BETWEEN, with two values") {
        StateItem item;
        item.name = "users.id";
        item.function_type = FUNCTION_BETWEEN;
        
        StateData data1;
        data1.Set((int64_t) 1);
        
        StateData data2;
        data2.Set((int64_t) 10);
        
        item.data_list.emplace_back(data1);
        item.data_list.emplace_back(data2);
        
        auto range = item.MakeRange2();
        
        StateRange expectedRange;
        expectedRange.SetBetween((int64_t) 1, (int64_t) 10);
        
        REQUIRE(range == expectedRange);
        REQUIRE(range != randomRange);
    }
    
    WARN("TODO: should write more tests for other function types");
    WARN("TODO: should write tests for AND / OR");
}

TEST_CASE("StateCluster::insert()", "[StateCluster]") {
    MockedRelationshipResolver resolver;
    
    SECTION("should insert transaction into cluster correctly") {
        auto txn1 = sampleTransaction1();
        auto txn2 = sampleTransaction2();
        
        StateCluster cluster({"users.id"});
        
        REQUIRE(
            std::find(
                cluster.keyColumns().begin(),
                cluster.keyColumns().end(),
                "users.id"
            ) != cluster.keyColumns().end()
        );
        
        cluster.insert(txn1, resolver);
        cluster.insert(txn2, resolver);
        
        REQUIRE(cluster.clusters().find("users.id") != cluster.clusters().end());
        
        auto &usersIdCluster = cluster.clusters().at("users.id");
        
        auto readGids = usersIdCluster.read.at(StateRange{1});
        auto writeGids = usersIdCluster.write.at(StateRange{1});
        
        // INSERT를 행하여 1번 사용자를 추가한 트랜잭션은 user.id = 1에 대한 writeGids에 포함되어야 한다.
        REQUIRE(std::find(writeGids.begin(), writeGids.end(), txn1->gid()) != writeGids.end());
        
        // WHERE 절에 1번 사용자를 조회하여 업데이트한 트랜잭션은 user.id = 1에 대한 readGids에 포함되어야 한다.
        REQUIRE(std::find(readGids.begin(), readGids.end(), txn2->gid()) != readGids.end());
    }
    
    SECTION("should insert transaction into cluster correctly, with multiple key columns") {
        auto txn1 = sampleTransaction1();
        auto txn2 = sampleTransaction2();
        auto txn3 = sampleTransaction3();
        
        StateCluster cluster({"users.id", "posts.id"});
        
        cluster.insert(txn1, resolver);
        cluster.insert(txn2, resolver);
        cluster.insert(txn3, resolver);
        
        REQUIRE(cluster.clusters().find("users.id") != cluster.clusters().end());
        REQUIRE(cluster.clusters().find("posts.id") != cluster.clusters().end());
        
        {
            auto &usersIdCluster = cluster.clusters().at("users.id");
            
            auto readGids = usersIdCluster.read.at(StateRange{1});
            auto writeGids = usersIdCluster.write.at(StateRange{1});
            
            // INSERT를 행하여 1번 사용자를 추가한 트랜잭션은 user.id = 1에 대한 writeGids에 포함되어야 한다.
            REQUIRE(std::find(writeGids.begin(), writeGids.end(), txn1->gid()) != writeGids.end());
            
            // WHERE 절에 1번 사용자를 조회하여 업데이트한 트랜잭션은 user.id = 1에 대한 readGids에 포함되어야 한다.
            REQUIRE(std::find(readGids.begin(), readGids.end(), txn2->gid()) != readGids.end());
        }
        {
            auto &postsIdCluster = cluster.clusters().at("posts.id");
            auto writeGids = postsIdCluster.write.at(StateRange{1});
            
            // INSERT를 행하여 1번 포스트 추가한 트랜잭션은 posts.id = 1에 대한 writeGids에 포함되어야 한다.
            REQUIRE(std::find(writeGids.begin(), writeGids.end(), txn3->gid()) != writeGids.end());
        }
    }
    
    SECTION("should match transactions correctly") {
        /*
         * 이 테스트에서는 다음과 같이 클러스터가 구성되어 있어야 한다.
         *
         * Cluster[users.id] {
         *     [users.id = 1] {
         *         READ  { gid(2) }
         *         WRITE { gid(1) }
         *     }
         * }
         *
         * Cluster[posts.id] {
         *     [posts.id = 1] {
         *         READ  { }
         *         WRITE { gid(3) }
         *     }
         * }
         */
        
        
        auto txn1 = sampleTransaction1();
        auto txn2 = sampleTransaction2();
        auto txn3 = sampleTransaction3();
        
        StateCluster cluster({"users.id", "posts.id"});
        
        cluster.insert(txn1, resolver);
        cluster.insert(txn2, resolver);
        cluster.insert(txn3, resolver);
        
        // 1번 트랜잭션과 users.id 클러스터의 WRITE 섹션을 매칭 시도한다.
        auto match1 = cluster.match(StateCluster::WRITE, "users.id", txn1);
        
        // 매칭은 성공적으로 이루어져야 한다.
        REQUIRE(match1.has_value());
        // 매칭된 클러스터의 범위는 1이어야 한다. (users.id = 1을 건드리기 때문에)
        REQUIRE(match1.value() == StateRange { 1 });
        
        auto gids1 = cluster.clusters()
            .at("users.id")
            .write
            .at(match1.value());
        
        // 클러스터의 gid 목록에는 1번 트랜잭션의 gid가 포함되어야 한다.
        REQUIRE(std::find(gids1.begin(), gids1.end(), txn1->gid()) != gids1.end());
        
        // 2번 트랜잭션과 users.id 클러스터의 READ 섹션을 매칭 시도한다.
        auto match2 = cluster.match(StateCluster::READ, "users.id", txn2);
        
        // 매칭은 성공적으로 이루어져야 한다.
        REQUIRE(match2.has_value());
        // 매칭된 클러스터의 범위는 1이어야 한다. (users.id = 1을 건드리기 때문에)
        REQUIRE(match2.value() == StateRange { 1 });
        
        auto gids2 = cluster.clusters()
            .at("users.id")
            .read
            .at(match2.value());
        
        // 클러스터의 gid 목록에는 2번 트랜잭션의 gid가 포함되어야 한다.
        REQUIRE(std::find(gids2.begin(), gids2.end(), txn2->gid()) != gids2.end());
        
        // 3번 트랜잭션과 posts.id 클러스터의 WRITE 섹션을 매칭 시도한다.
        auto match3 = cluster.match(StateCluster::WRITE, "posts.id", txn3);
        
        // 매칭은 성공적으로 이루어져야 한다.
        REQUIRE(match3.has_value());
        // 매칭된 클러스터의 범위는 1이어야 한다. (posts.id = 1을 건드리기 때문에)
        REQUIRE(match3.value() == StateRange { 1 });
        
        auto gids3 = cluster.clusters()
            .at("posts.id")
            .write
            .at(match3.value());
        
        // 클러스터의 gid 목록에는 3번 트랜잭션의 gid가 포함되어야 한다.
        REQUIRE(std::find(gids3.begin(), gids3.end(), txn3->gid()) != gids3.end());
        
        // 3번 트랜잭션과 users.id 클러스터의 READ 섹션을 매칭 시도한다.
        auto bad_match = cluster.match(StateCluster::WRITE, "users.id", txn3);
        
        // 3번 트랜잭션은 users.id = 1을 읽는 쿼리가 없으므로 매칭이 실패해야 한다.
        REQUIRE_FALSE(bad_match.has_value());
    }
    
}