//
// Created by cheesekun on 3/16/23.
//

#ifndef ULTRAVERSE_PROCMATCHER_HPP
#define ULTRAVERSE_PROCMATCHER_HPP

#include <string>
#include <vector>
#include <optional>

#include <ultparser_query.pb.h>

#include "Query.hpp"

#include "../StateItem.h"
#include "utils/log.hpp"

class ProcCall;

namespace ultraverse::state::v2 {
    
    class ProcMatcher {
    public:
        static void load(const std::string &procedureDefinition, ProcMatcher &instance);
        static std::unordered_set<std::string> extractTableColumns(const std::string &primaryTable, const ultparser::DMLQueryExpr &expr);
        
        
        /**
         * @brief wrapper function of ult_sanitize_select_into
         * @note 이 function은 tidb/parser의 파서가 SELECT col INTO var 를 해석하지 못하는 문제를 우회하기 위한 것이기 때문에 오래 사용되어선 안됩니다.
         *       때문에, 이 benchbase가 아닌 다른 케이스에서 문제를 일으킬 수 있습니다
         *       TODO: tidb upstream에 SELECT col INTO var를 해석하지 못하는 문제에 대해 버그 리포트를 제출하십시오.
         */
        static std::string sanitizeSelectInto(const std::string &procedureDefinition);
        /**
         * @brief wrapper function fo ult_normalize_procedure_code
         * @note 이 function은 tidb/parser의 파서가 SELECT col INTO var 를 해석하지 못하는 문제를 우회하기 위한 것이기 때문에 오래 사용되어선 안됩니다.
         *       때문에, 이 benchbase가 아닌 다른 케이스에서 문제를 일으킬 수 있습니다
         *       TODO: tidb upstream에 SELECT col INTO var를 해석하지 못하는 문제에 대해 버그 리포트를 제출하십시오.
         */
        static std::string normalizeProcedureCode(const std::string &procedureDefinition);
        
        /**
         * @brief wrapper function of ult_extract_
         * this function extracts all variable assignments (SELECT colA INTO varA FROM ...) of each statement.
         *
         * eg) 아래와 같은 프로시저가 있다고 가정하자.
         *
         *     CREATE PROCEDURE TestProcA( ... )
         *     BEGIN
         *         DECLARE varA INT; -- (1)
         *         DECLARE varB INT; -- (2)
         *         DECLARE varC INT; -- (3)
         *
         *         SELECT colA INTO varA FROM table1; -- (4)
         *         INSERT table2 VALUES (varA); -- (5)
         *
         *         SELECT colA, colB, colC INTO varA, varB, varC FROM table1; -- (6)
         *
         *         SELECT colA, colB, colC INTO varA FROM table1; -- (7)
         *
         *     END
         *
         * 이 경우, 이 함수는 다음과 같은 결과를 반환한다.
         *     std::vector<...> result = {
         *         { }, // (1)
         *         { }, // (2)
         *         { }, // (3)
         *         { { "colA", "varA" } }, // (4)
         *         { }, // (5)
         *         { { "colA", "varA" }, { "colB", "varB" }, { "colC", "varC" } }, // (6)
         *         { { "colA", "varA" }, { "colB", "varA" }, { "colC", "varA" } }, // (7)
         *     }
         *
         * NOTE: this will count DECLARE as statement
         */
        static std::vector<std::unordered_map<std::string, std::string>> extractVariableAssignments(const std::string &procedureDefinition);
        
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
        
        
        std::vector<StateItem> variableSet(const ProcCall &procCall) const;
        std::vector<std::shared_ptr<Query>> asQuery(int index, const ProcCall &procCall, const std::vector<std::string> &keyColumns) const;
        
        const std::vector<std::string> &parameters() const;
        const std::vector<std::shared_ptr<ultparser::Query>> codes() const;
        
        const std::unordered_set<std::string> &readSet() const;
        const std::unordered_set<std::string> &writeSet() const;
    private:
        void extractRWSets();
        void extractRWSets(const ultparser::Query &query);
        
        LoggerPtr _logger;
        std::string _definition;
        
        std::vector<std::shared_ptr<ultparser::Query>> _codes;
        
        std::vector<std::unordered_map<std::string, std::string>> _variableAssignments;
        
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
