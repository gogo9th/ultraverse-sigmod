//
// Created by cheesekun on 3/16/23.
//

#ifndef ULTRAVERSE_PROCMATCHER_HPP
#define ULTRAVERSE_PROCMATCHER_HPP

#include <string>
#include <vector>
#include <optional>
#include <map>

#include <ultparser_query.pb.h>

#include "Query.hpp"

#include "../StateItem.h"
#include "utils/log.hpp"

class ProcCall;

namespace ultraverse::state::v2 {
    
    // 변수 값 상태
    struct VariableValue {
        enum State { KNOWN, UNKNOWN, UNDEFINED };
        State state = UNDEFINED;
        StateData data;

        static VariableValue known(const StateData& d) {
            return VariableValue{KNOWN, d};
        }
        static VariableValue unknown() {
            return VariableValue{UNKNOWN, StateData()};
        }
    };

    // trace 결과
    struct TraceResult {
        std::vector<StateItem> readSet;
        std::vector<StateItem> writeSet;
        std::vector<std::string> unresolvedVars;  // 디버깅용
    };

    using SymbolTable = std::unordered_map<std::string, VariableValue>;

    class ProcMatcher {
    public:
        static void load(const std::string &procedureDefinition, ProcMatcher &instance);
        static std::unordered_set<std::string> extractTableColumns(const std::string &primaryTable, const ultparser::DMLQueryExpr &expr);
        
        
        explicit ProcMatcher(const std::string &procedureDefinition);
        
        
        /**
         * @deprecated 삭제 예정입니다
         */
        ProcMatcher(const std::vector<std::string> &procedureCodes);
        
        /**
         * 주어진 쿼리가 프로시저의 몇번째 줄인지 매칭을 시도합니다.
         * @return 성공적으로 매칭된 경우 프로시저 소스 코드의 라인 넘버,
         *         매칭에 실패한 경우에는 음수를 반환합니다.
         */
        int matchForward(const std::string &statement, int fromIndex);
        
        
        TraceResult trace(
            const std::map<std::string, StateData>& initialVariables,
            const std::vector<std::string>& keyColumns = {}
        ) const;
        
        
        std::vector<StateItem> variableSet(const ProcCall &procCall) const;
        std::vector<std::shared_ptr<Query>> asQuery(int index, const ProcCall &procCall, const std::vector<std::string> &keyColumns) const;
        
        const std::vector<std::string> &parameters() const;
        const std::vector<std::shared_ptr<ultparser::Query>> codes() const;
        
        const std::unordered_set<std::string> &readSet() const;
        const std::unordered_set<std::string> &writeSet() const;
    private:
        void traceStatement(
            const ultparser::Query& stmt,
            SymbolTable& symbols,
            TraceResult& result,
            const std::vector<std::string>& keyColumns
        ) const;
        
        VariableValue evaluateExpr(
            const ultparser::DMLQueryExpr& expr,
            const SymbolTable& symbols
        ) const;
        
        static bool isComplexExpression(const ultparser::DMLQueryExpr& expr);
        
        StateItem resolveExprToStateItem(
            const std::string& columnName,
            const ultparser::DMLQueryExpr& expr,
            const SymbolTable& symbols,
            std::vector<std::string>& unresolvedVars
        ) const;
        
        std::vector<StateItem> buildWhereItemSet(
            const std::string& primaryTable,
            const ultparser::DMLQueryExpr& whereExpr,
            const SymbolTable& symbols,
            std::vector<std::string>& unresolvedVars
        ) const;
        
        void extractRWSets();
        void extractRWSets(const ultparser::Query &query);
        
        LoggerPtr _logger;
        std::string _definition;
        
        std::vector<std::shared_ptr<ultparser::Query>> _codes;
        
        std::vector<std::string> _parameters;
        
        std::unordered_set<std::string> _readSet;
        std::unordered_set<std::string> _writeSet;
        
        /**
         * Map<VariableName, DefaultValue>
         */
        std::unordered_map<std::string, std::optional<StateData>> _variables;
    };
}

#endif //ULTRAVERSE_PROCMATCHER_HPP
