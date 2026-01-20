//
// Created by cheesekun on 8/10/22.
//

#ifndef ULTRAVERSE_DBHANDLE_HPP
#define ULTRAVERSE_DBHANDLE_HPP

#include <string>

namespace ultraverse::base {
    /**
     * @brief 여러 DB 소프트웨어를 지원하기 위한 DB 핸들(connection)의 추상화 클래스
     */
    class DBHandle {
    public:
        virtual ~DBHandle() = default;

        /**
         * @brief DB 서버에 접속한다.
         */
        virtual void connect(const std::string &host, int port, const std::string &user, const std::string &password) = 0;
        /**
         * @brief DB 서버로부터 접속을 해제한다.
         */
        virtual void disconnect() = 0;
        
        /**
         * @brief DB 서버에서 쿼리를 실행한다.
         * @param query 쿼리문
         * @return 결과 코드
         * @note 결과 코드는 각 DB 소프트웨어마다 다르다.
         */
        virtual int executeQuery(const std::string &query) = 0;
    };
}

#endif //ULTRAVERSE_DBHANDLE_HPP
