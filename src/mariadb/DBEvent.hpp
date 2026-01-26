//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_MARIADB_DBEVENT_HPP
#define ULTRAVERSE_MARIADB_DBEVENT_HPP

#include <mysql/mysql.h>
#include <field_types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

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

        QueryEvent(
            const std::string &schema,
            const ultparser::Query &pbStatement,
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

    class IntVarEvent: public base::DBEvent {
    public:
        enum Type: uint8_t {
            INVALID = 0,
            LAST_INSERT_ID = 1,
            INSERT_ID = 2
        };

        IntVarEvent(Type type, uint64_t value, uint64_t timestamp);

        event_type::Value eventType() override {
            return event_type::INTVAR;
        }

        uint64_t timestamp() override;
        Type type() const;
        uint64_t value() const;

    private:
        Type _type;
        uint64_t _value;
        uint64_t _timestamp;
    };

    class RandEvent: public base::DBEvent {
    public:
        RandEvent(uint64_t seed1, uint64_t seed2, uint64_t timestamp);

        event_type::Value eventType() override {
            return event_type::RAND;
        }

        uint64_t timestamp() override;
        uint64_t seed1() const;
        uint64_t seed2() const;

    private:
        uint64_t _seed1;
        uint64_t _seed2;
        uint64_t _timestamp;
    };

    class UserVarEvent: public base::DBEvent {
    public:
        enum ValueType: uint8_t {
            STRING = 0,
            REAL = 1,
            INT = 2,
            DECIMAL = 3
        };

        UserVarEvent(std::string name,
                     ValueType type,
                     bool isNull,
                     bool isUnsigned,
                     uint32_t charset,
                     std::string value,
                     uint64_t timestamp);

        event_type::Value eventType() override {
            return event_type::USER_VAR;
        }

        uint64_t timestamp() override;
        const std::string &name() const;
        ValueType type() const;
        bool isNull() const;
        bool isUnsigned() const;
        uint32_t charset() const;
        const std::string &value() const;

    private:
        std::string _name;
        ValueType _type;
        bool _isNull;
        bool _isUnsigned;
        uint32_t _charset;
        std::string _value;
        uint64_t _timestamp;
    };
    
    class TableMapEvent: public base::DBEvent {
    public:
        TableMapEvent(
            uint64_t tableId,
            std::string database,
            std::string table,
            std::vector<std::pair<column_type::Value, int>> columns,
            std::vector<std::string> columnNames,
            std::vector<uint8_t> unsignedFlags,
            std::vector<enum_field_types> mysqlTypes,
            std::vector<uint16_t> mysqlMetadata,
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

        int columnCount() const;
        
        column_type::Value typeOf(int columnIndex) const;
        int sizeOf(int columnIndex) const;
        std::string nameOf(int columnIndex) const;
        bool isUnsigned(int columnIndex) const;
        enum_field_types mysqlTypeOf(int columnIndex) const;
        uint16_t mysqlMetadataOf(int columnIndex) const;

        // Serialization removed: protobuf-based serialization is handled by state log types.
        
    private:
        uint64_t _timestamp;
        uint64_t _tableId;
        
        std::string _database;
        std::string _table;
        std::vector<std::pair<column_type::Value, int>> _columns;
        std::vector<std::string> _columnNames;
        std::vector<uint8_t> _unsignedFlags;
        std::vector<enum_field_types> _mysqlTypes;
        std::vector<uint16_t> _mysqlMetadata;
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
        explicit RowEvent(Type type, uint64_t tableId, int columns,
                          std::vector<uint8_t> columnsBeforeImage,
                          std::vector<uint8_t> columnsAfterImage,
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
        std::pair<std::string, int> readRow(TableMapEvent &tableMapEvent, int basePos,
                                            const std::vector<uint8_t> &columnsBitmap,
                                            int columnsBitmapCount, bool isUpdate);
        
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

        std::vector<uint8_t> _columnsBeforeImage;
        std::vector<uint8_t> _columnsAfterImage;
        int _columnsBeforeCount;
        int _columnsAfterCount;
        
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

#endif //ULTRAVERSE_MARIADB_DBEVENT_HPP
