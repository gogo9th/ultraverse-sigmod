//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DBEVENT_HPP
#define ULTRAVERSE_DBEVENT_HPP

#include <cstdint>
#include <cstdio>

#include <string>
#include <unordered_set>
#include <set>
#include <vector>

#include <ultparser_query.pb.h>

#include "mariadb/state/state_log_hdr.h"
#include "mariadb/state/StateItem.h"

#include "utils/log.hpp"

/**
 * @brief 추상화된 DB 이벤트 타입
 */
namespace ultraverse::event_type {
    enum Value {
        UNKNOWN = 0,
        LOG_ROTATION = 1,
        
        TXNID = 10,
        QUERY = 11,
        
        ROW_EVENT = 20,
        ROW_QUERY = 21,
        TABLE_MAP = 22,

        INTVAR = 30,
        RAND = 31,
        USER_VAR = 32,
    };
}

namespace ultraverse::column_type {
    enum Value {
        STRING = 0,
        INTEGER = 1,
        FLOAT = 2,
        DATETIME = 3,
        DECIMAL = 4,
    };
}

namespace ultraverse::base {
    /**
     * @brief 여러 DB 소프트웨어를 지원하기 위한 binlog 이벤트 추상화 클래스
     */
    class DBEvent {
    public:
        
        /** @brief 이벤트 타입을 반환한다 */
        virtual event_type::Value eventType() = 0;
        /** @brief 이 이벤트가 실행된 시간을 반환한다 */
        virtual uint64_t timestamp() = 0;
        
        const char *rawObject() {
            return nullptr;
        };
        
        size_t rawObjectSize() {
            return 0;
        };
    };
    
    /**
     * @brief 트랜잭션 종료 (ID 발행) 이벤트
     */
    class TransactionIDEventBase: public DBEvent {
    public:
        event_type::Value eventType() override {
            return event_type::TXNID;
        }
        
        virtual uint64_t transactionId() = 0;
    };
    
    /**
     * @brief 쿼리 실행 이벤트
     */
    class QueryEventBase: public DBEvent {
    public:
        enum QueryType {
            UNKNOWN = 0,
            
            // DML
            SELECT = 1,
            INSERT = 2,
            UPDATE = 3,
            DELETE = 4,
            
            // DDL
            DDL_UNKNOWN = 10,
            CREATE_TABLE = 11,
            ALTER_TABLE = 12,
            DROP_TABLE = 13,
            RENAME_TABLE = 14,
            TRUNCATE_TABLE = 15,
        };
        
        event_type::Value eventType() override {
            return event_type::QUERY;
        }
        
        QueryEventBase();
        
        /**
         * @brief QueryEventBase::parse() 의 오류 코드를 반환한다.
         */
        virtual const int64_t error() = 0;
        
        /**
         * @brief statement (쿼리문)을 반환한다.
         */
        virtual const std::string &statement() = 0;
        /**
         * @brief 쿼리가 실행된 데이터베이스 이름을 반환한다.
         */
        virtual const std::string &database() = 0;
        
        /**
         * @brief SQL statement를 파싱 시도한다.
         * @note 이 메소드는 _itemSet, _variableSet, _whereSet, _varMap 을 채운다.
         */
        bool parse();
        
        /**
         * @brief _itemSet, _whereSet으로부터 _readSet, _writeSet을 채운다.
         */
        void buildRWSet(const std::vector<std::string> &keyColumns);
        
        /**
         * @brief DDL 쿼리인지 여부를 반환한다. (parse() 결과 기준)
         */
        bool isDDL() const;
        /**
         * @brief DML 쿼리인지 여부를 반환한다. (parse() 결과 기준)
         */
        bool isDML() const;
        
        std::vector<StateItem> &itemSet();
    
        /**
         * @brief 이 쿼리의 실행 결과 (row image)를 반환한다.
         */
        std::vector<StateItem> &readSet();
        /**
         * @brief WHERE 절의 row image를 반환한다.
         */
        std::vector<StateItem> &writeSet();
        /**
         * @brief SQL 변수의 row image를 반환한다.
         */
        std::vector<StateItem> &variableSet();

        QueryType queryType() const;

        void columnRWSet(std::set<std::string> &readColumns, std::set<std::string> &writeColumns) const;
        
    protected:
        LoggerPtr _logger;
        
        bool processDDL(const ultparser::DDLQuery &ddlQuery);
        bool processDML(const ultparser::DMLQuery &dmlQuery);
        
        bool processSelect(const ultparser::DMLQuery &dmlQuery);
        bool processInsert(const ultparser::DMLQuery &dmlQuery);
        bool processUpdate(const ultparser::DMLQuery &dmlQuery);
        bool processDelete(const ultparser::DMLQuery &dmlQuery);
        
        bool processWhere(const ultparser::DMLQuery &dmlQuery, const ultparser::DMLQueryExpr &expr);
        void processExprForColumns(const std::string &primaryTable, const ultparser::DMLQueryExpr &expr, bool qualifyUnqualified = true);
    private:
        StateItem *findStateItem(const std::string &name);
        
        QueryType _queryType;
        
        std::unordered_set<std::string> _relatedTables;
    
        std::unordered_set<std::string> _readColumns;
        std::unordered_set<std::string> _writeColumns;
        std::vector<StateItem> _readItems;
        std::vector<StateItem> _writeItems;
    
        std::vector<StateItem> _itemSet;
        std::vector<StateItem> _variableSet;
        std::vector<StateItem> _whereSet;
    };
}

#endif //ULTRAVERSE_DBEVENT_HPP
