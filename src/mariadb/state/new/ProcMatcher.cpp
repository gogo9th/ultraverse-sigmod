//
// Created by cheesekun on 3/16/23.
//

#include <cstdint>
#include <cmath>
#include <optional>
#include <cctype>

#include <libultparser/libultparser.h>
#include <ultparser_query.pb.h>

#include "ProcMatcher.hpp"

#include "mariadb/state/WhereClauseBuilder.hpp"

#include "utils/StringUtil.hpp"


namespace ultraverse::state::v2 {

    namespace {
        struct ParsedParam {
            std::string name;
            ProcMatcher::ParamDirection direction = ProcMatcher::ParamDirection::UNKNOWN;
        };

        bool isWhitespace(char ch) {
            return std::isspace(static_cast<unsigned char>(ch)) != 0;
        }

        std::string trimCopy(const std::string &value) {
            size_t start = 0;
            while (start < value.size() && isWhitespace(value[start])) {
                start++;
            }
            if (start == value.size()) {
                return "";
            }
            size_t end = value.size() - 1;
            while (end > start && isWhitespace(value[end])) {
                end--;
            }
            return value.substr(start, end - start + 1);
        }

        std::string extractParamName(const std::string &segment, size_t start) {
            if (start >= segment.size()) {
                return "";
            }
            if (segment[start] == '`') {
                size_t end = segment.find('`', start + 1);
                if (end == std::string::npos || end <= start + 1) {
                    return "";
                }
                return segment.substr(start + 1, end - start - 1);
            }
            size_t end = start;
            while (end < segment.size() && !isWhitespace(segment[end])) {
                end++;
            }
            if (end == start) {
                return "";
            }
            return segment.substr(start, end - start);
        }

        std::vector<ParsedParam> parseProcedureParams(const std::string &definition) {
            std::vector<ParsedParam> params;
            if (definition.empty()) {
                return params;
            }

            std::string lower = utility::toLower(definition);
            size_t procPos = lower.find("procedure");
            if (procPos == std::string::npos) {
                return params;
            }

            size_t openPos = lower.find('(', procPos);
            if (openPos == std::string::npos) {
                return params;
            }

            size_t depth = 1;
            size_t closePos = std::string::npos;
            for (size_t i = openPos + 1; i < definition.size(); i++) {
                char ch = definition[i];
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth--;
                    if (depth == 0) {
                        closePos = i;
                        break;
                    }
                }
            }

            if (closePos == std::string::npos || closePos <= openPos + 1) {
                return params;
            }

            std::string paramList = definition.substr(openPos + 1, closePos - openPos - 1);
            size_t segmentStart = 0;
            depth = 0;
            for (size_t i = 0; i <= paramList.size(); i++) {
                if (i == paramList.size() || (paramList[i] == ',' && depth == 0)) {
                    std::string segment = trimCopy(paramList.substr(segmentStart, i - segmentStart));
                    segmentStart = i + 1;

                    if (segment.empty()) {
                        continue;
                    }

                    std::string lowerSeg = utility::toLower(segment);
                    size_t pos = 0;
                    while (pos < segment.size() && isWhitespace(segment[pos])) {
                        pos++;
                    }

                    ProcMatcher::ParamDirection direction = ProcMatcher::ParamDirection::IN;
                    const auto consumeToken = [&](const std::string &token) -> bool {
                        if (lowerSeg.compare(pos, token.size(), token) != 0) {
                            return false;
                        }
                        size_t end = pos + token.size();
                        if (end < lowerSeg.size() && !isWhitespace(lowerSeg[end])) {
                            return false;
                        }
                        pos = end;
                        return true;
                    };

                    if (consumeToken("inout")) {
                        direction = ProcMatcher::ParamDirection::INOUT;
                    } else if (consumeToken("out")) {
                        direction = ProcMatcher::ParamDirection::OUT;
                    } else if (consumeToken("in")) {
                        direction = ProcMatcher::ParamDirection::IN;
                    }

                    while (pos < segment.size() && isWhitespace(segment[pos])) {
                        pos++;
                    }

                    std::string name = extractParamName(segment, pos);
                    if (name.empty()) {
                        continue;
                    }

                    ParsedParam param;
                    param.name = std::move(name);
                    param.direction = direction;
                    params.push_back(std::move(param));
                    continue;
                }

                if (paramList[i] == '(') {
                    depth++;
                } else if (paramList[i] == ')') {
                    if (depth > 0) {
                        depth--;
                    }
                }
            }

            return params;
        }

        std::string normalizeVariableName(const std::string& name) {
            if (name.empty()) {
                return name;
            }
            if (name[0] == '@') {
                return utility::toLower(name.substr(1));
            }
            return utility::toLower(name);
        }

        // 두 KNOWN StateData에 대해 산술 연산 수행
        // 지원 타입: INTEGER(int64_t), DOUBLE
        // 반환: 계산 성공 시 StateData, 실패 시 std::nullopt
        std::optional<StateData> computeArithmetic(
            ultparser::DMLQueryExpr::Operator op,
            const StateData& left,
            const StateData& right
        ) {
            const auto leftType = left.Type();
            const auto rightType = right.Type();

            const bool leftIsInt = leftType == en_column_data_int || leftType == en_column_data_uint;
            const bool rightIsInt = rightType == en_column_data_int || rightType == en_column_data_uint;
            const bool leftIsDouble = leftType == en_column_data_double;
            const bool rightIsDouble = rightType == en_column_data_double;

            if ((!leftIsInt && !leftIsDouble) || (!rightIsInt && !rightIsDouble)) {
                return std::nullopt;
            }

            const bool useDouble = leftIsDouble || rightIsDouble;

            if (useDouble) {
                double lhs = 0.0;
                double rhs = 0.0;
                if (leftIsDouble) {
                    if (!left.Get(lhs)) {
                        return std::nullopt;
                    }
                } else {
                    int64_t tmp = 0;
                    if (!left.Get(tmp)) {
                        return std::nullopt;
                    }
                    lhs = static_cast<double>(tmp);
                }
                if (rightIsDouble) {
                    if (!right.Get(rhs)) {
                        return std::nullopt;
                    }
                } else {
                    int64_t tmp = 0;
                    if (!right.Get(tmp)) {
                        return std::nullopt;
                    }
                    rhs = static_cast<double>(tmp);
                }

                if ((op == ultparser::DMLQueryExpr::DIV || op == ultparser::DMLQueryExpr::MOD) && rhs == 0.0) {
                    return std::nullopt;
                }

                double result = 0.0;
                switch (op) {
                    case ultparser::DMLQueryExpr::PLUS:
                        result = lhs + rhs;
                        break;
                    case ultparser::DMLQueryExpr::MINUS:
                        result = lhs - rhs;
                        break;
                    case ultparser::DMLQueryExpr::MUL:
                        result = lhs * rhs;
                        break;
                    case ultparser::DMLQueryExpr::DIV:
                        result = lhs / rhs;
                        break;
                    case ultparser::DMLQueryExpr::MOD:
                        result = std::fmod(lhs, rhs);
                        break;
                    default:
                        return std::nullopt;
                }

                return StateData(result);
            }

            int64_t lhs = 0;
            int64_t rhs = 0;
            if (!left.Get(lhs) || !right.Get(rhs)) {
                return std::nullopt;
            }

            if ((op == ultparser::DMLQueryExpr::DIV || op == ultparser::DMLQueryExpr::MOD) && rhs == 0) {
                return std::nullopt;
            }

            int64_t result = 0;
            switch (op) {
                case ultparser::DMLQueryExpr::PLUS:
                    result = lhs + rhs;
                    break;
                case ultparser::DMLQueryExpr::MINUS:
                    result = lhs - rhs;
                    break;
                case ultparser::DMLQueryExpr::MUL:
                    result = lhs * rhs;
                    break;
                case ultparser::DMLQueryExpr::DIV:
                    result = lhs / rhs;
                    break;
                case ultparser::DMLQueryExpr::MOD:
                    result = lhs % rhs;
                    break;
                default:
                    return std::nullopt;
            }

            return StateData(result);
        }
    } // namespace
    
    void ProcMatcher::load(const std::string &procedureDefinition, ProcMatcher &instance) {
        static const auto logger = createLogger("ProcMatcher");
        static thread_local uintptr_t s_parser = 0;

        if (s_parser == 0) {
            s_parser = ult_sql_parser_create();
        }
        
        logger->debug(procedureDefinition);
        ultparser::ParseResult parseResult;
        
        char *parseResultCStr = nullptr;
        
        int64_t parseResultCStrSize = ult_sql_parse_new(
            s_parser,
            (char *) procedureDefinition.c_str(),
            static_cast<int64_t>(procedureDefinition.size()),
            &parseResultCStr
        );
        
        if (parseResultCStrSize <= 0) {
            goto FAILURE;
        }
        
        if (!parseResult.ParseFromArray(parseResultCStr, parseResultCStrSize)) {
            free(parseResultCStr);
            
            goto FAILURE;
        }
        
        free(parseResultCStr);
        
        if (parseResult.result() != ultparser::ParseResult::SUCCESS) {
            logger->error("parser error: {}", parseResult.error());
            goto FAILURE;
        }
        
        if (!parseResult.warnings().empty()) {
            for (const auto &warning: parseResult.warnings()) {
                logger->warn("parser warning: {}", warning);
            }
        }
        
        {
            const auto &_procedureInfo = parseResult.statements()[0];
            assert(_procedureInfo.type() == ultparser::Query::PROCEDURE);
            
            const auto &procedureInfo = _procedureInfo.procedure();
            const auto parsedParams = parseProcedureParams(procedureDefinition);
            std::unordered_map<std::string, ProcMatcher::ParamDirection> parsedDirectionMap;
            for (const auto &param : parsedParams) {
                parsedDirectionMap[utility::toLower(param.name)] = param.direction;
            }
            
            for (const auto &parameter: procedureInfo.parameters()) {
                const auto name = parameter.name();
                instance._parameters.push_back(name);

                ProcMatcher::ParamDirection direction = ProcMatcher::ParamDirection::IN;
                if (!parsedDirectionMap.empty()) {
                    const auto it = parsedDirectionMap.find(utility::toLower(name));
                    if (it != parsedDirectionMap.end()) {
                        direction = it->second;
                    } else {
                        direction = ProcMatcher::ParamDirection::UNKNOWN;
                    }
                }
                instance._parameterDirections.push_back(direction);
                instance._parameterDirectionMap[utility::toLower(name)] = direction;
            }

            for (const auto &variable: procedureInfo.variables()) {
                LocalVariableDef def;
                def.name = variable.name();
                if (variable.has_default_value()) {
                    def.defaultExpr = variable.default_value();
                }
                instance._localVariables.push_back(std::move(def));
            }
         
            for (const auto &statement: procedureInfo.statements()) {
                instance._codes.push_back(std::make_shared<ultparser::Query>(statement));
            }
        }
        
        return;
        
        FAILURE:
        logger->error("Failed to parse procedure definition");
    }
    
    std::unordered_set<std::string> ProcMatcher::extractTableColumns(const std::string &primaryTable, const ultparser::DMLQueryExpr &expr) {
        std::unordered_set<std::string> columns;
        if (expr.operator_() == ultparser::DMLQueryExpr::AND || expr.operator_() == ultparser::DMLQueryExpr::OR) {
            for (const auto &child: expr.expressions()) {
                auto _columns = std::move(extractTableColumns(primaryTable, child));
                columns.insert(_columns.begin(), _columns.end());
            }
        } else {
            if (expr.left().value_type() != ultparser::DMLQueryExpr::IDENTIFIER) {
                // FIXME: CONCAT(users.id, users.name) = 'foo' is not supported yet.
                return columns;
            }
            
            std::string left = utility::toLower(expr.left().identifier());
            const auto &right = expr.right();
            
            if (left.find('.') == std::string::npos) {
                columns.insert(primaryTable + "." + left);
            } else {
                columns.insert(left);
            }
            
            
            // FIXME: 이거 rvalue는 read고 lvalue는 무조건 w 아님?
            if (right.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                if (right.identifier().find('.') == std::string::npos) {
                    columns.insert(utility::toLower(primaryTable + "." + right.identifier()));
                } else {
                    columns.insert(utility::toLower(right.identifier()));
                }
            }
        }
        
        return std::move(columns);
    }
    
    ProcMatcher::ProcMatcher(const std::string &procedureDefinition):
        _logger(createLogger("ProcMatcher")),
        _definition(procedureDefinition),
        _codes()
    {
        load(procedureDefinition, *this);
        extractRWSets();
    }
    
    ProcMatcher::ProcMatcher(const std::vector<std::string> &procedureCodes)
    {
    }
    
    void ProcMatcher::extractRWSets() {
        for (const auto &statement: _codes) {
            extractRWSets(*statement);
        }
    }
    
    void ProcMatcher::extractRWSets(const ultparser::Query &statement) {
        const auto insertReadSet = [this](std::unordered_set<std::string> readSet) {
            _readSet.insert(readSet.begin(), readSet.end());
        };
        
        const auto insertWriteSet = [this](std::unordered_set<std::string> writeSet) {
            _writeSet.insert(writeSet.begin(), writeSet.end());
        };
        
        if (statement.type() == ultparser::Query::DML) {
            const auto &dml = statement.dml();
            
            for (const auto &expr: dml.update_or_write()) {
                insertWriteSet(extractTableColumns(dml.table().real().identifier(), expr));
            }
            
            if (dml.has_where()) {
                insertReadSet(extractTableColumns(dml.table().real().identifier(), dml.where()));
            }
        }
        
        if (statement.type() == ultparser::Query::IF) {
            const auto &block = statement.if_block();
            
            insertReadSet(extractTableColumns("", block.condition()));
            
            for (const auto &q: block.then_block()) {
                extractRWSets(q);
            }
            
            for (const auto &q: block.else_block()) {
                extractRWSets(q);
            }
        }
        
        if (statement.type() == ultparser::Query::WHILE) {
            const auto &block = statement.while_block();
            
            insertReadSet(extractTableColumns("", block.condition()));
            
            for (const auto &q: block.block()) {
                extractRWSets(q);
            }
        }
    }
    
    TraceResult ProcMatcher::trace(
        const std::map<std::string, StateData>& initialVariables,
        const std::vector<std::string>& keyColumns
    ) const {
        TraceResult result;
        SymbolTable symbols;

        // Preload hinted/initial variables (params, locals, @vars) into symbols.
        for (const auto& entry : initialVariables) {
            const auto normalized = normalizeVariableName(entry.first);
            if (!normalized.empty()) {
                symbols[normalized] = VariableValue::known(entry.second);
            }
        }
        
        // 1. 프로시저 파라미터를 심볼 테이블에 초기화
        for (const auto& paramName : _parameters) {
            const auto normalizedName = normalizeVariableName(paramName);
            if (symbols.find(normalizedName) == symbols.end()) {
                symbols[normalizedName] = VariableValue{VariableValue::UNDEFINED, StateData()};
                result.unresolvedVars.push_back(normalizedName);
            }
        }

        // 1-1. DECLARE 변수 초기화
        for (const auto& varDef : _localVariables) {
            const auto normalizedName = normalizeVariableName(varDef.name);
            if (symbols.find(normalizedName) != symbols.end()) {
                continue;
            }
            VariableValue value = VariableValue::unknown();
            if (varDef.defaultExpr.has_value()) {
                value = evaluateExpr(varDef.defaultExpr.value(), symbols);
            }
            symbols[normalizedName] = value;
        }
        
        // 2. 각 문장을 순회하며 분석
        for (const auto& code : _codes) {
            traceStatement(*code, symbols, result, keyColumns);
        }
        
        return result;
    }
    
    void ProcMatcher::traceStatement(
        const ultparser::Query& stmt,
        SymbolTable& symbols,
        TraceResult& result,
        const std::vector<std::string>& keyColumns
    ) const {
        if (stmt.type() == ultparser::Query::SET) {
            const auto& setQuery = stmt.set();
            for (const auto& assignment : setQuery.assignments()) {
                const std::string varName = normalizeVariableName(assignment.name());
                if (assignment.has_value()) {
                    symbols[varName] = evaluateExpr(assignment.value(), symbols);
                } else {
                    symbols[varName] = VariableValue::unknown();
                }
            }
            return;
        }
        
        if (stmt.type() == ultparser::Query::DML) {
            const auto& dml = stmt.dml();
            const auto& primaryTable = dml.table().real().identifier();
            const auto isKeyColumn = [&](const std::string& colName) -> bool {
                if (keyColumns.empty()) {
                    return true;
                }
                for (const auto& kc : keyColumns) {
                    if (colName == kc) {
                        return true;
                    }
                    const std::string suffix = "." + kc;
                    if (colName.size() >= suffix.size() &&
                        colName.compare(colName.size() - suffix.size(), suffix.size(), suffix) == 0) {
                        return true;
                    }
                }
                return false;
            };
            
            // SELECT의 WHERE 절 → readSet
            if (dml.type() == ultparser::DMLQuery::SELECT && dml.has_where()) {
                std::vector<std::string> tableNames;
                if (!primaryTable.empty()) {
                    tableNames.push_back(primaryTable);
                }
                for (const auto &join : dml.join()) {
                    const std::string joinTable = join.real().identifier();
                    if (!joinTable.empty()) {
                        tableNames.push_back(joinTable);
                    }
                }
                auto whereItems = buildWhereItemSet(primaryTable, tableNames, dml.where(), symbols, result.unresolvedVars);
                result.readSet.insert(result.readSet.end(), whereItems.begin(), whereItems.end());
            }
            
            // SELECT INTO 처리: 결과를 변수에 저장하는 경우
            if (dml.type() == ultparser::DMLQuery::SELECT) {
                // into_variables()에 변수명들이 있음
                for (const auto& varName : dml.into_variables()) {
                    const auto normalizedName = normalizeVariableName(varName);
                    auto it = symbols.find(normalizedName);
                    if (it != symbols.end() && it->second.state == VariableValue::KNOWN) {
                        continue;
                    }
                    // SELECT 결과는 런타임에만 알 수 있으므로 UNKNOWN
                    symbols[normalizedName] = VariableValue::unknown();
                }
            }
            // UPDATE 처리: WHERE → readSet, key column → writeSet
            else if (dml.type() == ultparser::DMLQuery::UPDATE) {
                // WHERE 절 → readSet
                if (dml.has_where()) {
                    std::vector<std::string> tableNames;
                    if (!primaryTable.empty()) {
                        tableNames.push_back(primaryTable);
                    }
                    for (const auto &join : dml.join()) {
                        const std::string joinTable = join.real().identifier();
                        if (!joinTable.empty()) {
                            tableNames.push_back(joinTable);
                        }
                    }
                    auto whereItems = buildWhereItemSet(primaryTable, tableNames, dml.where(), symbols, result.unresolvedVars);
                    result.readSet.insert(result.readSet.end(), whereItems.begin(), whereItems.end());
                }
                
                // UPDATE SET 절: key column만 writeSet에 추가
                for (const auto& expr : dml.update_or_write()) {
                    if (expr.has_left() && expr.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                        std::string colName = expr.left().identifier();
                        if (colName.find('.') == std::string::npos) {
                            colName = primaryTable + "." + colName;
                        }
                        
                        if (isKeyColumn(colName) && expr.has_right()) {
                            result.writeSet.push_back(resolveExprToStateItem(colName, expr.right(), symbols, result.unresolvedVars));
                        }
                    }
                }
            }
            // DELETE 처리: WHERE → readSet + writeSet
            else if (dml.type() == ultparser::DMLQuery::DELETE) {
                if (dml.has_where()) {
                    std::vector<std::string> tableNames;
                    if (!primaryTable.empty()) {
                        tableNames.push_back(primaryTable);
                    }
                    for (const auto &join : dml.join()) {
                        const std::string joinTable = join.real().identifier();
                        if (!joinTable.empty()) {
                            tableNames.push_back(joinTable);
                        }
                    }
                    auto whereItems = buildWhereItemSet(primaryTable, tableNames, dml.where(), symbols, result.unresolvedVars);
                    result.readSet.insert(result.readSet.end(), whereItems.begin(), whereItems.end());
                    result.writeSet.insert(result.writeSet.end(), whereItems.begin(), whereItems.end());
                } else {
                    // WHERE 없으면 전체 테이블 wildcard
                    result.writeSet.push_back(StateItem::Wildcard(primaryTable + ".*"));
                }
            }
            // INSERT 처리: key column → writeSet
            else if (dml.type() == ultparser::DMLQuery::INSERT) {
                for (const auto& expr : dml.update_or_write()) {
                    if (expr.has_left() && expr.left().value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                        std::string colName = expr.left().identifier();
                        if (colName.find('.') == std::string::npos) {
                            colName = primaryTable + "." + colName;
                        }
                        
                        if (isKeyColumn(colName) && expr.has_right()) {
                            result.writeSet.push_back(resolveExprToStateItem(colName, expr.right(), symbols, result.unresolvedVars));
                        }
                    }
                }
            }
            
            return;
        }
        
        // IF 블록 처리: 양쪽 분기 모두 분석 (union)
        if (stmt.type() == ultparser::Query::IF) {
            const auto& ifBlock = stmt.if_block();
            
            // then 블록의 모든 문장 재귀 처리
            for (const auto& query : ifBlock.then_block()) {
                traceStatement(query, symbols, result, keyColumns);
            }
            
            // else 블록의 모든 문장 재귀 처리
            for (const auto& query : ifBlock.else_block()) {
                traceStatement(query, symbols, result, keyColumns);
            }
            
            return;
        }
        
        // WHILE 블록 처리: 1회 순회 가정
        if (stmt.type() == ultparser::Query::WHILE) {
            const auto& whileBlock = stmt.while_block();
            
            // 블록 내 모든 문장 재귀 처리
            for (const auto& query : whileBlock.block()) {
                traceStatement(query, symbols, result, keyColumns);
            }
            
            return;
        }
        
        // TODO: 다른 문장 타입 처리 (다음 태스크에서)
    }
    
    VariableValue ProcMatcher::evaluateExpr(
        const ultparser::DMLQueryExpr& expr,
        const SymbolTable& symbols
    ) const {
        auto op = expr.operator_();
        
        // 사칙연산 처리
        if (op == ultparser::DMLQueryExpr::PLUS ||
            op == ultparser::DMLQueryExpr::MINUS ||
            op == ultparser::DMLQueryExpr::MUL ||
            op == ultparser::DMLQueryExpr::DIV ||
            op == ultparser::DMLQueryExpr::MOD) {
            auto leftVal = evaluateExpr(expr.left(), symbols);
            auto rightVal = evaluateExpr(expr.right(), symbols);
            
            if (leftVal.state == VariableValue::KNOWN &&
                rightVal.state == VariableValue::KNOWN) {
                auto result = computeArithmetic(op, leftVal.data, rightVal.data);
                if (result.has_value()) {
                    return VariableValue::known(*result);
                }
            }
            return VariableValue::unknown();
        }
        
        // 함수 호출은 UNKNOWN
        if (expr.value_type() == ultparser::DMLQueryExpr::FUNCTION) {
            return VariableValue::unknown();
        }
        
        // 복잡한 표현식은 UNKNOWN 반환
        if (isComplexExpression(expr)) {
            return VariableValue::unknown();
        }
        
        // VALUE 타입인 경우
        if (op == ultparser::DMLQueryExpr::VALUE) {
            switch (expr.value_type()) {
                case ultparser::DMLQueryExpr::INTEGER:
                    return VariableValue::known(StateData(expr.integer()));
                case ultparser::DMLQueryExpr::STRING:
                    return VariableValue::known(StateData(expr.string()));
                case ultparser::DMLQueryExpr::DOUBLE:
                    return VariableValue::known(StateData(expr.double_()));
                case ultparser::DMLQueryExpr::DECIMAL:
                    return VariableValue::known(StateData(expr.decimal()));
                case ultparser::DMLQueryExpr::IDENTIFIER: {
                    const auto& id = expr.identifier();
                    if (id.empty()) {
                        return VariableValue::unknown();
                    }

                    if (id.find('.') != std::string::npos) {
                        return VariableValue::unknown();
                    }

                    const auto normalized = normalizeVariableName(id);
                    auto it = symbols.find(normalized);
                    if (it != symbols.end()) {
                        return it->second;
                    }

                    if (id[0] == '@') {
                        return VariableValue{VariableValue::UNDEFINED, StateData()};
                    }

                    return VariableValue::unknown();
                }
                default:
                    return VariableValue::unknown();
            }
        }
        
        return VariableValue::unknown();
    }
    
    bool ProcMatcher::isComplexExpression(const ultparser::DMLQueryExpr& expr) {
        auto op = expr.operator_();
        // 산술 연산자가 있으면 복잡한 표현식
        if (op == ultparser::DMLQueryExpr::PLUS ||
            op == ultparser::DMLQueryExpr::MINUS ||
            op == ultparser::DMLQueryExpr::MUL ||
            op == ultparser::DMLQueryExpr::DIV ||
            op == ultparser::DMLQueryExpr::MOD) {
            return true;
        }
        // 함수 호출도 복잡한 표현식
        if (expr.value_type() == ultparser::DMLQueryExpr::FUNCTION) {
            return true;
        }
        return false;
    }
    
    StateItem ProcMatcher::resolveExprToStateItem(
        const std::string& columnName,
        const ultparser::DMLQueryExpr& expr,
        const SymbolTable& symbols,
        std::vector<std::string>& unresolvedVars
    ) const {
        auto value = evaluateExpr(expr, symbols);
        
        switch (value.state) {
            case VariableValue::KNOWN:
                return StateItem::EQ(columnName, value.data);
            case VariableValue::UNKNOWN:
                return StateItem::Wildcard(columnName);
            case VariableValue::UNDEFINED:
                // 표현식이 변수 참조이고 미정의면 unresolvedVars에 기록
                if (expr.value_type() == ultparser::DMLQueryExpr::IDENTIFIER) {
                    const auto& id = expr.identifier();
                    if (!id.empty() && id[0] == '@') {
                        unresolvedVars.push_back(id.substr(1));
                    }
                }
                return StateItem::Wildcard(columnName);
        }
        return StateItem::Wildcard(columnName);
    }
    
    std::vector<StateItem> ProcMatcher::buildWhereItemSet(
        const std::string& primaryTable,
        const std::vector<std::string>& tableNames,
        const ultparser::DMLQueryExpr& whereExpr,
        const SymbolTable& symbols,
        std::vector<std::string>& unresolvedVars
    ) const {
        ::ultraverse::state::WhereClauseOptions options;
        options.primaryTable = primaryTable;
        options.tableNames = tableNames;
        options.logger = _logger;
        options.resolveIdentifier = [&symbols, &unresolvedVars](
            const std::string&,
            const std::string& identifier,
            std::vector<StateData>& outValues
        ) -> bool {
            if (identifier.empty()) {
                return false;
            }
            if (identifier.find('.') != std::string::npos) {
                return false;
            }
            const auto normalized = normalizeVariableName(identifier);
            auto it = symbols.find(normalized);
            if (it == symbols.end()) {
                if (!identifier.empty() && identifier[0] == '@') {
                    unresolvedVars.push_back(normalized);
                }
                return false;
            }
            if (it->second.state == VariableValue::KNOWN) {
                outValues.push_back(it->second.data);
                return true;
            }
            if (it->second.state == VariableValue::UNDEFINED && !identifier.empty() && identifier[0] == '@') {
                unresolvedVars.push_back(normalized);
            }
            return false;
        };
        options.resolveColumnIdentifier = [&symbols, &tableNames](
            const std::string&,
            const std::string& identifier,
            std::vector<std::string>& outColumns
        ) -> bool {
            if (identifier.empty()) {
                return false;
            }
            if (!identifier.empty() && identifier[0] == '@') {
                return false;
            }
            const auto normalizedVar = normalizeVariableName(identifier);
            if (!normalizedVar.empty() && symbols.find(normalizedVar) != symbols.end()) {
                return false;
            }
            const auto normalized = utility::toLower(identifier);
            if (normalized.find('.') != std::string::npos) {
                outColumns.push_back(normalized);
                return true;
            }
            for (const auto &table : tableNames) {
                if (table.empty()) {
                    continue;
                }
                outColumns.push_back(utility::toLower(table + "." + normalized));
            }
            return !outColumns.empty();
        };

        return ::ultraverse::state::buildWhereItems(whereExpr, options);
    }
    
    const std::vector<std::string> &ProcMatcher::parameters() const {
        return _parameters;
    }

    const std::vector<ProcMatcher::ParamDirection> &ProcMatcher::parameterDirections() const {
        return _parameterDirections;
    }

    ProcMatcher::ParamDirection ProcMatcher::parameterDirection(size_t index) const {
        if (index >= _parameterDirections.size()) {
            return ParamDirection::UNKNOWN;
        }
        return _parameterDirections[index];
    }

    ProcMatcher::ParamDirection ProcMatcher::parameterDirection(const std::string &name) const {
        auto it = _parameterDirectionMap.find(utility::toLower(name));
        if (it == _parameterDirectionMap.end()) {
            return ParamDirection::UNKNOWN;
        }
        return it->second;
    }
    
    const std::vector<std::shared_ptr<ultparser::Query>> ProcMatcher::codes() const {
        return _codes;
    }
    
    const std::unordered_set<std::string> &ProcMatcher::readSet() const {
        return _readSet;
    }
    
    const std::unordered_set<std::string> &ProcMatcher::writeSet() const {
        return _writeSet;
    }
}
