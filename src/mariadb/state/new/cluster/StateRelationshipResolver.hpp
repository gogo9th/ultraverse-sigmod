//
// Created by cheesekun on 6/22/23.
//

#ifndef ULTRAVERSE_STATERELATIONSHIPRESOLVER_HPP
#define ULTRAVERSE_STATERELATIONSHIPRESOLVER_HPP


#include <string>
#include <optional>
#include <shared_mutex>

#include "../../StateItem.h"
#include "../StateChangePlan.hpp"
#include "../StateChangeContext.hpp"

#include "mariadb/state/new/proto/ultraverse_state_fwd.hpp"

namespace ultraverse::state::v2 {
    
    struct RowAlias {
        StateItem alias;
        StateItem real;
        
        template <typename Archive>
        void serialize(Archive &archive);

        void toProtobuf(ultraverse::state::v2::proto::RowAlias *out) const;
        void fromProtobuf(const ultraverse::state::v2::proto::RowAlias &msg);
    };
    
    /**
     * @brief FK / Alias 관계를 해결(resolve)하기 위한 인터페이스
     *
     * @details MySQL같은 관계형 DB에서는 다음과 같은 형태로 테이블 간 관계를 정의하여 사용하는 경우가 있다.
     * <pre>
     *   users.id -> posts.user_id
     *   users.id -> comments.user_id
     *   users.id -> likes.user_id
     *   ...
     * </pre>
     *
     * 그 외에도, 다음과 같은 형태로 Alias 관계를 정의하여 사용하는 경우가 있다.
     * <pre>
     *   users.id -> users.handle_name
     *   (users.id = 1) == (users.handle_name == '@butter')
     *
     *   @butter는 users.id가 1인 사용자의 핸들 이름이다.
     *   이 경우, users.id와 users.handle_name은 Alias 관계로 정의되어 있다고 볼 수 있다.
     * </pre>
     *
     * 그리고, Alias와 FK 관계를 함께 사용하는 경우도 있다.
     * <pre>
     *   users.id -> users.handle_name
     *   (users.id = 1) == (users.handle_name == '@butter')
     *
     *   posts.author -> users.handle_name -> users.id
     *   (posts.author = '@butter') == (users.handle_name = '@butter') == (users.id = 1)
     * </pre>
     *
     * 이 클래스는 이러한 관계를 해결하여 원래 컬럼 이름을 찾아주는 역할을 한다.
     */
    class RelationshipResolver {
    public:
        
        /**
         * @brief 상태 전환 프로그램이 참조해야 할 "실제" 컬럼 이름을 반환한다.
         *
         * @returns "실제" 컬럼 이름. (optional)
         * @note 'null'을 표현하기 위해 std::optional을 사용한다.
         */
        virtual std::string resolveColumnAlias(const std::string &columnExpr) const = 0;
        
        /**
         * @brief 주어진 컬럼이 외래 키 관계로 레퍼런싱 하고 있는 컬럼 이름을 반환한다.
         *
         * @returns 외래 키 관계로 참조된 컬럼 이름. (optional)
         * @note 'null'을 표현하기 위해 std::optional을 사용한다.
         */
        virtual std::string resolveForeignKey(const std::string &columnExpr) const = 0;
        
        /**
         * @brief 주어진 Row Element와 연관된 "실제 컬럼"의 Row Element를 반환한다.
         * @copilot 위 resolveColumnAlias(), resolveForeignKey() 함수를 사용하여 "실제 컬럼"을 찾는다.
         *          예시 의사 코드를 여기다 적어둘테니, 참고하면서 구현하면 좋을 것 같다. (적절히 수정해서)
         *
         *          var columnExpr = columnExpr;
         *
         *          while (true) {
         *              var alias = resolveColumnAlias(columnExpr);
         *              var foreignKey = resolveForeignKey(alias.isPresent() ? alias.get() : columnExpr);
         *
         *              if (foreignKey.isPresent()) {
         *                  columnExpr = foreignKey.get();
         *                  continue;
         *              } else {
         *                  return alias.isPresent() ? alias.get() : columnExpr;
         *              }
         *          }
         */
        virtual std::string resolveChain(const std::string &columnExpr) const;
        
        /**
         * @brief 주어진 Row Element와 연관된 "실제 컬럼"의 Row Element를 반환한다.
         *
         * @example users.user_id_str => users.id 로 alias 관계가 설정된 경우 상태 전환 프로그램이 동작하면서 내부적으로 다음과 같은 매핑 테이블이 생성된다.
         *      +-----------------------+----------------+
         *      | users.user_id_str     | users.id       |
         *      +-----------------------+----------------+
         *      | "000042"              | 42             |
         *      +-----------------------+----------------+
         *      | "000043"              | 43             |
         *      +-----------------------+----------------+
         *      | ...                   | ...            |
         *      +-----------------------+----------------+
         *
         *      따라서,
         *      resolveRowAlias(StateItem { users.user_id_str EQ "000042" }) 를 호출하면
         *          => StateItem { users.id EQ 42 } 를 반환한다
         *
         * @return 매핑된 "실제 컬럼"의 Row Element. (optional)
         * @note 'null'을 표현하기 위해 std::optional을 사용한다.
         */
        virtual std::shared_ptr<StateItem> resolveRowAlias(const StateItem &item) const = 0;
        
        virtual std::shared_ptr<StateItem> resolveRowChain(const StateItem &item) const;
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

        virtual std::string resolveColumnAlias(const std::string &columnExpr) const override;
        virtual std::string resolveForeignKey(const std::string &columnExpr) const override;
        
        virtual std::shared_ptr<StateItem> resolveRowAlias(const StateItem &alias) const override;
        
        void addRowAlias(const StateItem &alias, const StateItem &real);
        bool addTransaction(Transaction &transaction);
        
    private:
        const StateChangePlan &_plan;
        const StateChangeContext &_context;
        
        RowAliasTable _rowAliasTable;
    };
    
    /**
     * @brief 캐시 레이어를 추가한 RelationshipResolver
     * @details RelationshipResolver의 resolveColumnAlias(), resolveChain() 함수는 매번 동일한 결과를 반환한다.
     *         따라서, 이를 캐시하여 성능을 향상시킨다.
     */
    class CachedRelationshipResolver: public RelationshipResolver {
    public:
        using RowCacheMap = std::unordered_map<size_t, std::pair<int, std::shared_ptr<StateItem>>>;
        
        CachedRelationshipResolver(const RelationshipResolver &resolver, int maxRowElements);
        
        /**
         * @brief 캐시된 결과를 반환 시도한다. (없으면 원 구현체의 함수를 호출한다.)
         */
        virtual std::string resolveColumnAlias(const std::string &columnExpr) const override;
        /**
         * @brief 단순 FK 테이블 조회이므로 캐시하지 않고 원 구현체의 함수를 호출한다.
         */
        virtual std::string resolveForeignKey(const std::string &columnExpr) const override;
        /**
         * @brief 캐시된 결과를 반환 시도한다. (없으면 원 구현체의 함수를 호출한다.)
         */
        virtual std::string resolveChain(const std::string &columnExpr) const override;
        /**
         * @brief 원 구현체의 함수를 호출한다.
         */
        virtual std::shared_ptr<StateItem> resolveRowAlias(const StateItem &item) const override;
        /**
         * @brief 원 구현체의 함수를 호출한다.
         */
        virtual std::shared_ptr<StateItem> resolveRowChain(const StateItem &item) const override;
        
        void clearCache();
        
    private:
        bool isGCRequired(const RowCacheMap &rowCacheMap) const;
        /**
         * @brief 레퍼런스 카운트 단위로 정렬하여 하위 5%를 제거한다.
         * @note 이 메소드는 락을 걸지 않으므로 호출하는 쪽에서 락을 걸어야 한다.
         */
        static void gc(RowCacheMap &rowCacheMap) ;
        
        const RelationshipResolver &_resolver;
        
        int _maxRowElements;
        
        mutable std::shared_mutex _cacheLock;
        mutable std::unordered_map<std::string, std::string> _aliasCache;
        mutable std::unordered_map<std::string, std::string> _chainCache;
        mutable std::unordered_map<std::string, RowCacheMap> _rowAliasCache;
        mutable std::unordered_map<std::string, RowCacheMap> _rowChainCache;
    };
}


#endif //ULTRAVERSE_STATERELATIONSHIPRESOLVER_HPP
