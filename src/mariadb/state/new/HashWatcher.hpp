//
// Created by cheesekun on 10/30/22.
//

#ifndef ULTRAVERSE_HASHWATCHER_HPP
#define ULTRAVERSE_HASHWATCHER_HPP

#include <string>
#include <unordered_map>
#include <queue>

#include <atomic>

#include "../StateHash.hpp"

#include "mariadb/DBEvent.hpp"
#include "utils/log.hpp"
#include "mariadb/binlog/BinaryLogSequentialReader.hpp"

namespace ultraverse::state::v2 {
    class HashWatcher {
    public:
        HashWatcher(const std::string &basePath, const std::string &binlogName, const std::string &database);
        
        void start();
        void stop();
        
        void setHash(const std::string &tableName, const StateHash &hash);
        void queue(const std::string &tableName, const StateHash &hash);
        
        bool isHashMatched(const std::string &tableName);
        
    private:
        void watcherThreadMain();
        
        void processTableMapEvent(std::shared_ptr<mariadb::TableMapEvent> event);
        void processRowEvent(std::shared_ptr<mariadb::RowEvent> event);
        
        LoggerPtr _logger;
        
        std::string _binlogName;
        std::string _database;
    
        std::thread _watcherThread;
        
        bool _isThreadRunning;
        bool _isWatcherEnabled;
        
        std::mutex _mutex;
        
        std::unordered_map<uint64_t, std::shared_ptr<mariadb::TableMapEvent>> _tableMap;
        std::unordered_map<std::string, StateHash> _hashState;
        std::unordered_map<std::string, bool> _matchState;
        std::unordered_map<std::string, std::queue<StateHash>> _hashQueue;
        mariadb::BinaryLogSequentialReader _binlogReader;
    };
}

#endif //ULTRAVERSE_HASHWATCHER_HPP
