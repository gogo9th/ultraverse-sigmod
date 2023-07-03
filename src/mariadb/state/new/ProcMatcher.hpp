//
// Created by cheesekun on 3/16/23.
//

#ifndef ULTRAVERSE_PROCMATCHER_HPP
#define ULTRAVERSE_PROCMATCHER_HPP

#include <string>
#include <vector>

namespace ultraverse::state::v2 {
    class ProcMatcher {
    public:
        /**
         * @param procedureCodes 프로시저 소스 코드
         */
        ProcMatcher(const std::vector<std::string> &procedureCodes);
        
        /**
         * 주어진 쿼리가 프로시저의 몇번째 줄인지 매칭을 시도합니다.
         * @return 성공적으로 매칭된 경우 프로시저 소스 코드의 라인 넘버,
         *         매칭에 실패한 경우에는 음수를 반환합니다.
         */
        int matchForward(const std::string &statement, int fromIndex);
        
    private:
        const std::vector<std::string> &_procedureCodes;
    };
}

#endif //ULTRAVERSE_PROCMATCHER_HPP
