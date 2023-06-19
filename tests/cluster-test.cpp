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


std::shared_ptr<Transaction> sampleTransaction1() {
    auto transaction = std::make_shared<Transaction>();
    transaction->setGid(1);
    
    std::shared_ptr<Query> query1 = std::make_shared<Query>();
    {
        query1->setDatabase("test");
        query1->setStatement("INSERT INTO `users` (`name`) VALUES ('cheesekun')");
        query1->setAffectedRows(1);
        
        {
            StateItem item;
            item.name = "users.name";
            item.function_type = FUNCTION_EQ;
            
            std::string value = "cheesekun";
            StateData data;
            data.Set(value.c_str(), value.size());
            
            item.data_list.emplace_back(data);
            query1->itemSet().emplace_back(item);
        }
        
        {
            StateItem item;
            item.name = "users.id";
            item.function_type = FUNCTION_EQ;
            
            StateData data;
            data.Set((int64_t) 1);
            
            item.data_list.emplace_back(data);
            query1->itemSet().emplace_back(item);
        }
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
        
        {
            StateItem item;
            item.name = "users.name";
            item.function_type = FUNCTION_EQ;
            
            std::string value = "test1234";
            StateData data;
            data.Set(value.c_str(), value.size());
            
            item.data_list.emplace_back(data);
            query2->itemSet().emplace_back(item);
        }
        
        {
            StateItem item;
            item.name = "users.id";
            item.function_type = FUNCTION_EQ;
            
            StateData data;
            data.Set((int64_t) 1);
            
            item.data_list.emplace_back(data);
            query2->whereSet().emplace_back(item);
        }
    }
    
    *transaction << query2;
    
    return transaction;
}

TEST_CASE("test", "[StateCluster]") {
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
    
    cluster << txn1;
    cluster << txn2;
    

    REQUIRE(cluster.clusters().find("users.id") != cluster.clusters().end());
    
    auto &usersIdCluster = cluster.clusters().at("users.id");
    
    
    StateData data;
    data.Set((int64_t) 1);
    
    auto readGids = usersIdCluster.read.at(data);
    auto writeGids = usersIdCluster.write.at(data);
    
    // INSERT를 행하여 1번 사용자를 추가한 트랜잭션은 user.id = 1에 대한 writeGids에 포함되어야 한다.
    REQUIRE(std::find(writeGids.begin(), writeGids.end(), txn1->gid()) != writeGids.end());
    
    // WHERE 절에 1번 사용자를 조회하여 업데이트한 트랜잭션은 user.id = 1에 대한 readGids에 포함되어야 한다.
    // 이거 맞아요??
    REQUIRE(std::find(readGids.begin(), readGids.end(), txn2->gid()) != readGids.end());
}