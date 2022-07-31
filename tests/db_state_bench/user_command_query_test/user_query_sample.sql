# 최초 한번 DB 명시
 USE tpcc; 

# 모든 라인은 ADD 또는 DEL 로 시작하고 ; 로 종료

 # 쿼리 추가
 # ADD, DATETIME, QUERY
ADD, 2021-04-11 09:30:12.012345, UPDATE DISTRICT SET D_YTD = D_YTD + 4027.219970703125 WHERE D_ID > 1 AND D_ID < 10;

 # 쿼리 삭제
 # DEL, DATETIME
DEL, 2021-04-11 09:31:15.678901;

 # multiple line 가능
ADD , 2021-04-11 09:30:17 , UPDATE DISTRICT 
                            SET D_NEXT_O_ID = D_NEXT_O_ID + 1
                          WHERE D_W_ID = 1 AND D_ID = 6;
