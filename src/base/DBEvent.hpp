//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DBEVENT_HPP
#define ULTRAVERSE_DBEVENT_HPP

#include <cstdint>
#include <cstdio>

#include <string>
#include <unordered_set>

#include <ultparser_query.pb.h>

#include "SQLParser.h"
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
         * @brief tokenize를 시도한다.
         * @return 실패 시 false를 반환한다.
         */
        bool tokenize();
        /**
         * @brief SQL statement를 파싱 시도한다.
         */
        bool parse();
        
        
        /**
         * @deprecated 더 이상 사용되지 않는다.
         */
        bool parseSelect();
        /**
         * @brief DDL (CREATE TABLE ... 등)을 파싱한다.
         * @deprecated 더 이상 사용해선 안된다. QueryEventBase::parse()로 통합되어야 한다.
         */
        bool parseDDL(int limit = -1);
        
        /**
         * @brief QueryEventBase::tokenize() 의 결과물에 액세스한다.
         * @return
         */
        std::vector<int16_t> tokens() const;
        /**
         * @brief QueryEventBase::tokenize() 의 결과물에 액세스한다.
         */
        std::vector<size_t> tokenPos() const;
        
        /**
         * @brief DDL 쿼리인지 여부를 반환한다.
         */
        bool isDDL() const;
        /**
         * @brief DML 쿼리인지 여부를 반환한다.
         */
        bool isDML() const;
    
        /**
         * @brief 이 쿼리가 읽기 액세스하는 테이블 컬럼의 목록을 반환한다.
         */
        std::unordered_set<std::string> &readSet();
        /**
         * @brief 이 쿼리가 쓰기 액세스하는 테이블 컬럼의 목록을 반환한다.
         */
        std::unordered_set<std::string> &writeSet();
    
        /**
         * @brief 이 쿼리의 실행 결과 (row image)를 반환한다.
         */
        std::vector<StateItem> &itemSet();
        /**
         * @brief WHERE 절의 row image를 반환한다.
         */
        std::vector<StateItem> &whereSet();
        /**
         * @brief SQL 변수의 row image를 반환한다.
         */
        std::vector<StateItem> &variableSet();
        /**
         * TODO: 문서화 필요
         */
        std::vector<StateItem> &varMap();
        
        
        /** @deprecated */
        std::unordered_map<std::string, StateData> &sqlVarMap();
        
    protected:
        LoggerPtr _logger;
        
        bool processDDL(const ultparser::DDLQuery &ddlQuery);
        bool processDML(const ultparser::DMLQuery &dmlQuery);
        
        bool processSelect(const ultparser::DMLQuery &dmlQuery);
        bool processInsert(const ultparser::DMLQuery &dmlQuery);
        bool processUpdate(const ultparser::DMLQuery &dmlQuery);
        bool processDelete(const ultparser::DMLQuery &dmlQuery);
        
        bool processWhere(const std::string &primaryTable, const ultparser::DMLQueryExpr &expr);
        
        void processRValue(StateItem &item, const ultparser::DMLQueryExpr &right);
    private:
        void extractReadWriteSet(const hsql::InsertStatement *insert);
        void extractReadWriteSet(const hsql::DeleteStatement *del);
        void extractReadWriteSet(const hsql::UpdateStatement *update);
        void extractReadWriteSet(const hsql::SelectStatement *select);
        
        void walkExpr(const hsql::Expr *expr, StateItem &parent, std::vector<std::string> &readSet, const std::string &rootTable, bool isRoot);
        
        StateItem *findStateItem(const std::string &name);
        
        std::vector<int16_t> _tokens;
        std::vector<size_t> _tokenPos;
    
        std::unordered_set<std::string> _readSet;
        std::unordered_set<std::string> _writeSet;
    
        /** @deprecated use _insertSet, _deleteSet instead. */
        std::vector<StateItem> _itemSet;
        std::vector<StateItem> _variableSet;
        
        std::vector<StateItem> _insertSet;
        std::vector<StateItem> _deleteSet;
        std::vector<StateItem> _whereSet;
        std::vector<StateItem> _varMap;
        
        std::unordered_map<std::string, StateData> _sqlVarMap;
        
        hsql::SQLParserResult _parseResult;
    };
}

#endif //ULTRAVERSE_DBEVENT_HPP
