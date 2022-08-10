#ifndef DB_STATE_API_HDR_INCLUDED
#define DB_STATE_API_HDR_INCLUDED

#include "mysql.h"

/*
  - return 된 Group ID 로 쿼리를 시작함을 알림
  - Query Group Commit 으로 Group ID 에 해당하 는 쿼리들을 서버에 반영
  - Query Group Abort 로 Group ID 에 해당하는 쿼리들을 삭제
 */
int Stitch_SQL_Start(MYSQL *mysql);

/*
  - Group ID 로 쿼리를 실행
 */
int Stitch_SQL_Command(MYSQL *mysql, int id, const char *query);

/*
  - Query Group Command API로 실행한 쿼리들을 취소
 */
int Stitch_SQL_Abort(MYSQL *mysql, int id);

#endif /* DB_STATE_API_HDR_INCLUDED */
