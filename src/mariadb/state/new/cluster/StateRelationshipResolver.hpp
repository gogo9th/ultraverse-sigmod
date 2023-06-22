//
// Created by cheesekun on 6/22/23.
//

#ifndef ULTRAVERSE_STATERELATIONSHIPRESOLVER_HPP
#define ULTRAVERSE_STATERELATIONSHIPRESOLVER_HPP


#include <string>
#include <optional>

#include "../../StateItem.h"
#include "../StateChangePlan.hpp"
#include "../StateChangeContext.hpp"

namespace ultraverse::state::v2 {
    
    struct RowAlias {
        StateItem alias;
        StateItem real;
        
        template <typename Archive>
        void serialize(Archive &archive);
    };
    
    class RelationshipResolver {
    public:
        
        /**
         * @brief 상태 전환 프로그램이 참조해야 할 "실제" 컬럼 이름을 반환한다.
         *
         * @returns "실제" 컬럼 이름. (optional)
         * @note 'null'을 표현하기 위해 std::optional을 사용한다.
         */
        virtual std::optional<std::string> resolveColumnAlias(const std::string &columnExpr) const = 0;
        
        /**
         * @brief 주어진 컬럼이 외래 키 관계로 레퍼런싱 하고 있는 컬럼 이름을 반환한다.
         *
         * @returns 외래 키 관계로 참조된 컬럼 이름. (optional)
         * @note 'null'을 표현하기 위해 std::optional을 사용한다.
         */
        virtual std::optional<std::string> resolveForeignKey(const std::string &columnExpr) const = 0;
        
        /**
         * @brief 주어진 Row Element와 연관된 "실제 컬럼"의 Row Element를 반환한다.
         *
         * @example orders.user_id_str => users.id 로 alias 관계가 설정된 경우 상태 전환 프로그램이 동작하면서 내부적으로 다음과 같은 매핑 테이블이 생성된다.
         *      +-----------------------+----------------+
         *      | orders.user_id_str    | users.id       |
         *      +-----------------------+----------------+
         *      | "000042"              | 42             |
         *      +-----------------------+----------------+
         *      | "000043"              | 43             |
         *      +-----------------------+----------------+
         *      | ...                   | ...            |
         *      +-----------------------+----------------+
         *
         *      따라서,
         *      resolveRowAlias(StateItem { orders.user_id_str EQ "000042" }) 를 호출하면
         *          => StateItem { users.id EQ 42 } 를 반환한다
         *
         * @return 매핑된 "실제 컬럼"의 Row Element. (optional)
         * @note 'null'을 표현하기 위해 std::optional을 사용한다.
         */
        virtual std::optional<StateItem> resolveRowAlias(const StateItem &item) const = 0;
    };
    
    class StateRelationshipResolver: public RelationshipResolver {
    public:
        using AliasedColumn = std::string;
        /**
         * RowAliasTable["orders.user_id_str"][StateRange { "000042" }] 로 접근하면 다음과 같은 RowAlias를 얻을 수 있는 것을 목표로 한다.
         *   => RowAlias {
         *      alias: StateItem { orders.user_id_str EQ "000042" },
         *      real: StateItem { users.id EQ 42 }
         *   }
         */
        using RowAliasTable = std::map<
            AliasedColumn,
            std::unordered_map<StateRange, RowAlias>
        >;
    public:
        StateRelationshipResolver(const StateChangePlan &plan, const StateChangeContext &context);

        virtual std::optional<std::string> resolveColumnAlias(const std::string &columnExpr) const override;
        virtual std::optional<std::string> resolveForeignKey(const std::string &columnExpr) const override;
        
        virtual std::optional<StateItem> resolveRowAlias(const StateItem &alias) const override;
        
        void addRowAlias(StateItem &alias, StateItem &real);
        
    private:
        const StateChangePlan &_plan;
        const StateChangeContext &_context;
        
        RowAliasTable _rowAliasTable;
    };
}


#endif //ULTRAVERSE_STATERELATIONSHIPRESOLVER_HPP
