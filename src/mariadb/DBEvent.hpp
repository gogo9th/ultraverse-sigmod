//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_MARIADB_DBEVENT_HPP
#define ULTRAVERSE_MARIADB_DBEVENT_HPP

#include <mysql/mysql.h>

#include <cereal/access.hpp>

#include "base/DBEvent.hpp"

namespace ultraverse::mariadb {
    using namespace ultraverse;
    
    class TransactionIDEvent: public base::TransactionIDEventBase {
    public:
        TransactionIDEvent(uint64_t xid, uint64_t timestamp);
    
        uint64_t timestamp() override;
        uint64_t transactionId() override;
        
    private:
        uint64_t _timestamp;
        uint64_t _transactionId;
    };
    
    class QueryEvent: public base::QueryEventBase {
    public:
        QueryEvent(
            const std::string &schema,
            const std::string &statement,
            uint64_t timestamp
        );
    
        uint64_t timestamp() override;
    
        const int64_t error() override;
    
        const std::string &statement() override;
        const std::string &database() override;
    
    private:
        uint64_t _timestamp;
        
        int64_t _error;
        
        std::string _statement;
        std::string _database;
    };
    
    class TableMapEvent: public base::DBEvent {
    public:
        TableMapEvent(
            uint64_t tableId,
            std::string database,
            std::string table,
            std::vector<std::pair<column_type::Value, int>> columns,
            std::vector<std::string> columnNames,
            uint64_t timestamp
        );
        TableMapEvent() : _timestamp(0), _tableId(0) {};
        
        event_type::Value eventType() override {
            return event_type::TABLE_MAP;
        }
       
        uint64_t timestamp() override;
        
        uint64_t tableId() const;
        
        std::string database() const;
        std::string table() const;
        
        column_type::Value typeOf(int columnIndex) const;
        int sizeOf(int columnIndex) const;
        std::string nameOf(int columnIndex) const;

        template <typename Archive>
        void serialize(Archive &archive);
        
    private:
        uint64_t _timestamp;
        uint64_t _tableId;
        
        std::string _database;
        std::string _table;
        std::vector<std::pair<column_type::Value, int>> _columns;
        std::vector<std::string> _columnNames;
    };
    
    class RowEvent: public base::DBEvent {
    public:
        enum Type {
            INSERT,
            UPDATE,
            DELETE
        };
        
        explicit RowEvent(Type type, uint64_t tableId, int columns,
                          std::shared_ptr<uint8_t> rowData, int dataSize,
                          uint64_t timestamp, uint16_t flags);
        
        
        event_type::Value eventType() override {
            return event_type::ROW_EVENT;
        }
        
        uint64_t timestamp() override;
        
        Type type() const;
        
        uint64_t tableId() const;
        
        uint16_t flags() const;
        
        void mapToTable(TableMapEvent &tableMapEvent);
        
        /**
         * @note call mapToTable() first.
         */
        [[nodiscard]]
        int affectedRows() const;
    
        /**
         * @note call mapToTable() first.
         */
        std::string rowSet(int at);
        /**
         * @note call mapToTable() first.
         */
        std::string changeSet(int at);
        
        const std::vector<StateItem> &itemSet() const;
        const std::vector<StateItem> &updateSet() const;
    private:
        std::pair<std::string, int> readRow(TableMapEvent &tableMapEvent, int basePos, bool isUpdate);
        
        template <typename T>
        inline T readValue(int offset) {
            T value = 0;
            memcpy(&value, _rowData.get() + offset, sizeof(T));
            
            return value;
        }
        
        Type _type;
        uint16_t _flags;
        
        uint64_t _timestamp;
        uint64_t _tableId;
        int _columns;
        
        std::shared_ptr<uint8_t> _rowData;
        int _dataSize;
        
        int _affectedRows;
        
        
        std::vector<std::string> _rowSet;
        std::vector<std::string> _changeSet;
        
        std::vector<StateItem> _itemSet;
        std::vector<StateItem> _updateSet;
    };
    
    class RowQueryEvent: public base::DBEvent {
    public:
        RowQueryEvent(
            const std::string &statement,
            uint64_t timestamp
        );
        
        event_type::Value eventType() override {
            return event_type::ROW_QUERY;
        }
        
        uint64_t timestamp() override;
        
        std::string statement();
        
    private:
        std::string _statement;
        uint64_t _timestamp;
    };
    
    

}

#include "DBEvent.cereal.cpp"

#endif //ULTRAVERSE_MARIADB_DBEVENT_HPP
