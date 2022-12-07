//
// Created by cheesekun on 10/30/22.
//

#include "HashWatcher.hpp"

namespace ultraverse::state::v2 {
    HashWatcher::HashWatcher(const std::string &basePath, const std::string &binlogName, const std::string &database):
        _logger(createLogger("HashWatcher")),
        _binlogName(binlogName),
        _database(database),
        _binlogReader(basePath, binlogName)
    {
        _hashQueue.reserve(100);
    }
    
    void HashWatcher::start() {
        _isThreadRunning = true;
        _watcherThread = std::thread(&HashWatcher::watcherThreadMain, this);
    }
    
    void HashWatcher::stop() {
        _isThreadRunning = false;
        
        if (_watcherThread.joinable()) {
            _binlogReader.terminate();
            _watcherThread.join();
        }
    }
    
    void HashWatcher::setHash(const std::string &tableName, const StateHash &hash) {
        _hashState[tableName] = hash;
    }
    
    void HashWatcher::queue(const std::string &tableName, const StateHash &hash) {
        std::scoped_lock<std::mutex> _scopedLock(_mutex);
        _hashQueue[tableName].push(hash);
    }
    
    bool HashWatcher::isHashMatched(const std::string &tableName) {
        return _matchState[tableName];
    }
    
    void HashWatcher::watcherThreadMain() {
        while (_isThreadRunning) {
            if (!_binlogReader.next()) {
                return;
            }
            
            auto event = _binlogReader.currentEvent();
            if (event == nullptr) {
                continue;
            }
            
            switch (event->eventType()) {
                case event_type::QUERY: {
                    auto queryEvent = std::dynamic_pointer_cast<mariadb::QueryEvent>(event);
                    if (queryEvent->statement() == fmt::format("/* ULTRAVERSE_HASHWATCHER_START_{} */ CREATE TABLE __ULTRAVERSE_HASHWATCHER_START__( dummy INTEGER )", _database)) {
                        _isWatcherEnabled = true;
                    }
                }
                    break;
                case event_type::TXNID:
                    break;
                case event_type::TABLE_MAP:
                    processTableMapEvent(std::dynamic_pointer_cast<mariadb::TableMapEvent>(event));
                    break;
                case event_type::ROW_EVENT: {
                    if (_isWatcherEnabled) {
                        processRowEvent(std::dynamic_pointer_cast<mariadb::RowEvent>(event));
                    }
                }
                    break;
            }
        }
    }
    
    void HashWatcher::processTableMapEvent(std::shared_ptr<mariadb::TableMapEvent> event) {
        if (event->database() != _database) {
            return;
        }
        
        _logger->debug("[ROW] read row event: table id {} will be mapped with {}.{}", event->tableId(), event->database(), event->table());
        
        auto it = std::find_if(_tableMap.begin(), _tableMap.end(), [&event](auto &prevEvent) {
            return (
                prevEvent.second->database() == event->database() &&
                prevEvent.second->table() == event->table()
            );
        });
        
        if (it != _tableMap.end()) {
            _tableMap.erase(it);
        }
        
        _tableMap[event->tableId()] = event;
    }
    
    void HashWatcher::processRowEvent(std::shared_ptr<mariadb::RowEvent> event) {
        _logger->trace("[ROW] processing row event");
        
        if (_tableMap.find(event->tableId()) == _tableMap.end()) {
            return;
        }
    
        auto table = _tableMap[event->tableId()];
        auto &hash = _hashState[table->table()];
        auto &queue = _hashQueue[table->table()];
        
        int count = 0;
        while (queue.empty()) {
            if (!_isThreadRunning) {
                return;
            }
            
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(10ms);
            
            count++;
            
            if (count > 10) {
                _logger->warn("table desync: {}", table->table());
                return;
            }
        }
        
        if (_matchState[table->table()]) {
            return;
        }

        assert(hash.isInitialized());

        event->mapToTable(*table);
    
        for (int i = 0; i < event->affectedRows(); i++) {
            switch (event->type()) {
                case mariadb::RowEvent::INSERT:
                    hash += event->rowSet(i);
                    break;
                case mariadb::RowEvent::DELETE:
                    hash -= event->rowSet(i);
                    break;
                case mariadb::RowEvent::UPDATE:
                    hash -= event->rowSet(i);
                    hash += event->changeSet(i);
                    break;
            }
        }
    
    
        _mutex.lock();
        auto expectedHash = queue.front();
        queue.pop();
        
        if (hash == expectedHash) {
            _logger->trace("hash matched");
            _matchState[table->table()] = true;
        }
        _mutex.unlock();
    }
}