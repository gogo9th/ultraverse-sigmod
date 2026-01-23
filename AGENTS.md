<section id="project-info">
# Ultraverse Technical Documentation for Implementation Agents

## 1. System Overview
Ultraverse is a retroactive operation framework designed to recover from attacks (e.g., SQL injection) by changing, adding, or removing past committed queries without fully rolling back the database. It consists of two main components:
1.  **Database System:** Supports efficient retroactive operations on SQL queries.
2.  **Web Application Framework:** Ensures web application semantics are preserved during retroactive updates.

---

## 2. Database System Architecture
The agent must implement the following components as described in the architecture:

### 2.1 Query Analyzer
* Runs in the background with an unmodified database system.
* **Functions:**
    * Records column-wise read/write dependencies ($R/W$ sets).
    * Performs row-wise dependency analysis (Query Clustering).
    * Computes and logs table state hashes (Hash-Jumper).

### 2.2 Replay Scheduler
* Manages the rollback and replay phases.
* Executes multiple non-dependent queries in parallel during replay to maximize speed.

---

## 3. Dependency Analysis Logic
The core of Ultraverse is determining which queries depend on the retroactive target query ($Q_\tau$).

### 3.1 Column-wise Read/Write Sets ($R/W$)
The agent must parse SQL to extract $R$ and $W$ sets based on **Table A**:

| Query Type | Read Set ($R$) | Write Set ($W$) |
| :--- | :--- | :--- |
| **SELECT** | Columns in `SELECT`/`WHERE` + FKs to external tables. | $\emptyset$ |
| **INSERT** | Inner sub-query $R$ sets + FK columns. | All columns of the target table. |
| **UPDATE/DELETE** | Inner sub-query $R$ + Target columns read + FKs + `WHERE` columns. | Updated/Deleted columns + Referencing FKs in external tables. |
| **CREATE/ALTER** | FK columns of external tables. | All columns of the created/altered table. |
| **TRIGGER** | Union of $R$ sets of all inner queries. | Union of $W$ sets of all inner queries. |

### 3.2 Dependency Rules (Column-wise)
Based on **Table 1**, a dependency $Q_n \rightarrow Q_m$ exists if:
1.  **Direct:** $Q_n$ reads/writes a column that $Q_m$ wrote previously ($m < n$).
2.  **Transitive:** If $Q_n \rightarrow Q_m$ and $Q_m \rightarrow Q_l$, then $Q_n \rightarrow Q_l$.
3.  **Trigger:** If a query $Q_n$ depends on the target $Q_\tau$ and triggers $T_x$, then $T_x$ also depends on $Q_\tau$.

---

## 4. Optimization Techniques

### 4.1 Row-wise Query Clustering
Refines dependencies by checking if queries operate on overlapping table rows.
* **Cluster Key:** A column (e.g., `uid`) used to partition queries.
* **Logic:** If $K_c(Q_n) \cap K_c(Q_m) = \emptyset$, queries are independent and replay can be skipped for $Q_n$.
* **Key Selection:** Choose the column that minimizes the variance of cluster sizes (Choice Rule).

### 4.2 Hash-Jumper
Terminates replay early if the database state becomes identical to the pre-rollback state.
* **Implementation:** Compute an incremental hash of the table (add/subtract row hashes modulo $p$) upon every commit.
* **Check:** During replay, compare the current table hash with the logged hash. If they match, skip further replay for that table.

---

## 5. Retroactive Operation Workflow
The agent should implement the following state machine:

1.  **Rollback Phase:**
    * Identify dependent queries using the Dependency Graph.
    * Rollback **Mutated Tables** (written by dependents) and **Consulted Tables** (read by dependents) to a temporary database.
2.  **Replay Phase:**
    * Execute the retroactive change (add/remove/update $Q_\tau$).
    * Replay dependent queries from the temporary DB, running non-conflicting queries in parallel.
3.  **Update Phase:**
    * Lock the original database.
    * Reflect changes from the temporary DB to the original DB.
    * Unlock the database.
</section>
<section id="implementation-info">
# Implementation Info (코드 기준)

## 1. 실행 바이너리 / 엔트리포인트
- `src/Application.*`: getopt 기반 CLI 베이스. 각 앱이 optString()/main()을 구현하고 `exec()`로 실행.
- `src/statelogd.cpp`: binlog.index를 순차 읽어 `.ultstatelog` + `.ultchkpoint` 생성. 필수 옵션 `-b`(binlog.index), `-o`(로그 이름), `-k`(키 컬럼). `BinaryLogSequentialReader`(MySQLBinaryLogReaderV2) 사용, `-p`로 프로시저 로그 추가, `-n`로 EOF 후 종료, `-r`로 체크포인트 복원, `-d`로 이전 로그 폐기, `-c` 스레드 수, `-G/-Q` 디버그 출력, `-v/-V` 로그 레벨.
- `src/db_state_change.cpp`: 상태 변경 CLI. 환경변수 `DB_HOST/DB_PORT/DB_USER/DB_PASS` 필수. 액션은 `action1:action2:...` 형식이며 `make_cluster`, `rollback=gid[,gid...]`, `auto-rollback=ratio`, `prepend=gid,sqlfile`, `full-replay`, `replay` 지원. 옵션 `-i`(state log), `-d`(DB), `-b`(backup), `-k`(키 컬럼 그룹), `-a`(alias), `-C`(스레드), `-S`(skip gid), `-r`(report), `-N`(intermediate DB 유지), `-w`(write state log), `-D`(dry-run), `-s/-e`(gid 범위). `RANGE_COMP_METHOD`(intersect/eqonly)로 범위 비교 방식 지정.
- `src/state_log_viewer.cpp`: `.ultstatelog` 뷰어. `-i` 필수, `-s/-e` 범위, `-v/-V` 상세 출력.

## 2. 실행 흐름 (CLI 기준)
1) `statelogd`로 binlog → `.ultstatelog`/`.ultchkpoint` 생성.
2) `db_state_change ... make_cluster`로 `.ultcluster`, `.ultcolumns`, `.ulttables`, `.ultindex` 생성.
3) `db_state_change ... rollback=...` 또는 `prepend=...` 실행 → **prepare 단계**가 `.ultreplayplan` + 리포트(JSON)를 생성.
4) `db_state_change ... replay` 실행 → `.ultreplayplan` 기반 병렬 재실행.
5) (선택) `db_state_change ... full-replay` 실행 → rollback GID 제외 전체 트랜잭션 순차 재실행.
- 액션은 `:`로 연결되며 `rollback=-`는 stdin에서 GID 리스트를 읽는다.

## 3. 코드/디렉터리 지도 (파일 위치 → 역할)
- `src/base/*`: 공통 인터페이스/기반 로직. `base/DBEvent.*`에서 SQL 파싱·R/W set 생성, `base/DBHandlePool.*`/`TaskExecutor.*`로 실행·동시성 관리.
- `src/mariadb/binlog/*`: binlog 읽기. `BinaryLogSequentialReader`가 binlog.index를 순차 탐색하고 `MySQLBinaryLogReaderV2`가 libbinlogevents 기반 이벤트 디코딩.
- `src/mariadb/DBEvent.*`: MySQL 이벤트 래퍼(Query/Row/TableMap/IntVar/Rand/UserVar) 정의.
- `src/mariadb/DBHandle.*`: MySQL 연결/쿼리 실행, 테스트용 Mock 포함.
- `src/mariadb/state/new/*`: 현행 상태 로그/리플레이 파이프라인.
  - `StateLogWriter/Reader`, `GIDIndexReader/Writer`: `.ultstatelog`/`.ultindex` I/O.
  - `StateChanger.*`, `StateChangePlan/Report/ReplayPlan`: prepare/replay/full-replay 오케스트레이션.
  - `analysis/TaintAnalyzer.*`: 컬럼 taint 전파/필터링.
  - `cluster/StateCluster.*`, `cluster/StateRelationshipResolver.*`: row-level 클러스터·FK/alias 해석.
  - `graph/RowGraph.*`: 병렬 재실행 그래프/워커 스케줄링.
  - `StateIO.*`: I/O 추상화(파일/Mock/백업 로더).
  - `ProcLogReader/ProcMatcher`: 프로시저 힌트 복원/추적.
- `src/mariadb/state/*`: legacy StateItem/StateHash 등 공통 데이터 구조.
- `include/state_log_hdr.h`: state log 헤더/시간 구조체 정의.
- `parserlib/*`: TiDB parser fork + C API(`capi.go`) + protobuf 정의(`ultparser_query.proto`).
- `dependencies/sql-parser/*`: Hyrise 토크나이저.
- `mysql-server/`: MySQL 소스 트리(외부 제공, `.gitignore`로 제외). libbinlogevents 빌드에 필요.
- `scripts/esperanza/*`: BenchBase 자동 실행/결과 비교 스크립트.
- `tests/*`: Catch2 기반 단위 테스트(taint/rowgraph/statecluster/sqlparser 등).
- `cmake/*`, `src/CMakeLists.txt`: 빌드/의존성 구성.

## 4. 데이터/파일 포맷 (실제 코드 기준)
- `.ultstatelog`: `StateLogWriter`가 `TransactionHeader`(packed) + cereal-serialized `Transaction`을 연속 기록. `TransactionHeader.nextPos`로 skip 가능. (`StateLogReader::skipTransaction`)
- `.ultindex`: GID → 로그 오프셋 인덱스. `GIDIndexWriter`가 append 방식으로 기록, `GIDIndexReader`가 mmap으로 조회.
- `.ultcluster`: `StateCluster`(row-level 클러스터) cereal 바이너리.
- `.ulttables`, `.ultcolumns`: `TableDependencyGraph`/`ColumnDependencyGraph` cereal 바이너리 (make_cluster에서 갱신됨; prepare는 그래프 대신 컬럼 집합 taint로 column-wise 필터 수행).
- `.ultreplayplan`: `StateChangeReplayPlan` cereal 바이너리 (`gids`, `userQueries`).
- `.ultchkpoint`: statelogd 체크포인트 파일. 현재는 serialize 코드가 주석 처리되어 있어 실사용이 제한적.

## 5. 핵심 모듈 맵
- SQL 파서:
  - `parserlib/capi.go` + `parserlib/parser/*`: TiDB parser(fork) 기반, instance-based C API (`ult_sql_parser_create/ult_sql_parse_new/ult_query_hash_new/ult_parse_jsonify_new`)로 protobuf(`ultparser_query.proto`) 생성.
  - 지원 범위: GROUP BY/HAVING/aggregate/subquery/SET/SELECT INTO/DECIMAL.
  - C++ `base/DBEvent.cpp::QueryEventBase::parse()`가 `libultparser`로 parse 결과를 받아 DML/DDL 처리.
  - 토크나이저는 `dependencies/sql-parser`(Hyrise) 사용.
- Binlog 파싱: `src/mariadb/binlog/*` + `src/mariadb/DBEvent.*` (MySQL binlog 이벤트를 DBEvent로 변환).
- 상태 로그 I/O: `StateLogWriter/Reader`, `GIDIndexReader/Writer`, `StateClusterWriter`.
- 상태 변경 오케스트레이션: `StateChanger` + `StateChangePlan/Context/Report`.
- Row-level 클러스터: `StateCluster`(v2), `RowCluster`(legacy).
- 병렬 재실행 그래프: `RowGraph` (read/write range 기반 의존성 그래프).

## 6. 내부 처리 단계 (StateChanger 기준)
1) **make_cluster**: `.ultstatelog` 스캔 → `StateCluster` + `ColumnDependencyGraph`/`TableDependencyGraph` 생성, `.ultcluster/.ultcolumns/.ulttables/.ultindex` 저장.
2) **prepare (rollback/prepend)**: `TaintAnalyzer`(column-wise) → `StateCluster`(row-wise)로 replay GID 축소, `.ultreplayplan` 기록 + `replaceQuery` 생성.
3) **replay**: `.ultreplayplan` 읽어 `RowGraph`로 병렬 재실행 (statement context 적용).
4) **full-replay**: rollback GID 제외 전체 트랜잭션을 intermediate DB에 순차 재실행하고 “DB rename” 안내만 출력.

## 7. 의존성/클러스터 로직 요약
- `QueryEventBase::buildRWSet()`이 DML 타입별 read/write `StateItem`을 구성.
- `StateCluster`는 key column별 `StateRange` → GID 집합을 유지하고, rollback/prepend 대상이 읽은 범위와 교차하는 write를 replay 대상으로 판단.
- `RelationshipResolver`가 column alias/FK 체인을 해석하여 실제 key column 매칭. `CachedRelationshipResolver`가 캐시 레이어 제공.
- `RowGraph`는 key range별 마지막 read/write 노드를 추적해 간선 생성 (W→R, R→W, W→W) 후 worker가 entrypoint를 병렬 실행. FK/alias 체인을 해석해 key column으로 정규화하며, key column 미탐지 시 테이블별/전역 wildcard로 보수적으로 직렬화한다.
- `-k`는 `,`로 그룹을 구분하고 `+`로 복합 키 그룹을 표현한다. (예: `users.id,orders.user_id+orders.item_id`) **동일 테이블 복합키는 AND**, **서로 다른 테이블을 포함한 그룹은 키 타입별 교집합(OR) 기준**으로 처리되며, 없는 컬럼은 wildcard로 확장되지 않는다.
- `Query`는 column-wise read/write 컬럼 집합을 별도로 보관하며(`.ultstatelog`에 직렬화됨), prepare 단계에서 column-wise 의존성(taint)으로 1차 필터링 후 row-wise로 축소한다. DDL 쿼리는 query 단위로 skip.
- `QueryEventBase`는 subquery/aggregate/GROUP BY/HAVING/DECIMAL/함수 표현식을 read column에 반영하고, WHERE 없는 쿼리는 관련 테이블 wildcard를 조건부로 추가한다.
- prepare 단계는 `analysis::TaintAnalyzer`로 column taint 전파 후 row-wise로 축소하며, 키 컬럼 미탐지 트랜잭션은 즉시 replay 대상으로 처리한다.
- `StateCluster::generateReplaceQuery()`는 테이블별 key column **projection**을 사용한다. 동일 테이블 복합키는 AND, 멀티테이블 그룹은 OR로 WHERE를 생성하며, 테이블에 없는 컬럼은 조건에서 제외된다.
- `RowGraph`는 컬럼/복합그룹별 worker 큐와 hold 노드, wildcard holder로 병렬 replay 안정화 및 순서 제어를 강화했다.
- `RelationshipResolver`/`RowCluster`는 alias coercion, FK/alias 체인 무한루프 가드, 소문자 정규화, `_id` 접미 컬럼의 implicit table 추정 지원을 포함한다.
- `StateChanger.sqlload`는 `DBEvent::columnRWSet()`으로 column-wise read/write set을 저장하며, `Query`는 statement context(last_insert_id/insert_id/rand_seed/user vars)를 보관한다.
- `ProcMatcher::trace()`는 IF/WHILE 조건식에서 readSet을 추출하지 않고, 블록 내부 statement만 union 처리한다.

## 8. Hash-Jumper 관련
- `StateHash`/`HashWatcher`는 binlog row 이벤트로 해시를 증분 갱신해 동일 상태 조기 종료를 지원하도록 설계.
- 현재 `StateChanger`에는 멤버만 있고 실제 실행 경로에서 사용되지 않음.
- `TableDependencyGraph`는 write-only 쿼리도 테이블 의존성으로 반영하며, `HashWatcher`는 `BinaryLogSequentialReader`를 사용한다.

## 9. 프로시저 처리
- `statelogd`는 `__ULTRAVERSE_PROCEDURE_HINT` row 이벤트의 `callid/procname/args/vars`를 읽어 `ProcCall`로 복원하며, `args/vars`는 JSON object로 파싱된다.
- `ProcMatcher`는 `procdef/<proc>.sql`을 파싱해 프로시저 메타/statement를 보관하고, `trace()`로 R/W set을 추정한다.
- 프로시저 콜 트랜잭션은 `ProcMatcher::trace()` 결과에 더해, 동일 트랜잭션의 RowEvent에서 수집된 read/write set 및 column set을 procCall Query에 병합해 row/column 분석에 반영한다.
- 프로시저 내부 statement 해시 매칭(matchForward) 기반 복원은 제거됨: 현재는 **CALL 쿼리 + trace 결과만 기록/분석**한다.
- `statelogd.procedureLogPath`로 `ProcLogReader`를 열 수 있지만 실제 매칭/적용 경로는 대부분 미연결.

## 10. 현재 코드의 제약/주의
- DDL 지원 미완: `processDDL()` 부분적이며 make_cluster/prepare에서 DDL 쿼리는 경고 후 skip.
- `prepend` 입력 SQL은 DML만 지원 (DDL은 에러/스킵).
- `statelogd`는 `binlog_row_metadata=FULL` 필요, `PARTIAL_UPDATE_ROWS_EVENT` 미지원.
- 프로시저 힌트는 `callid/procname/args/vars` 형식만 지원하며 legacy `callinfo` 배열 포맷은 미지원.
- `statelogd`의 `.ultchkpoint` 직렬화가 주석 처리되어 있어 `-r` 복원은 제한적.
- `db_state_change`는 `stateLogPath`를 `.`로 고정(FIXME)하고, `BINLOG_PATH`는 계획에만 저장되어 현재 경로에서는 미사용.
- `statelogd`는 JSON `keyColumns`를 그룹으로 파싱한 뒤 RW set 계산용으로 flatten하여 `+` 복합키를 반영한다. `db_state_change`와 동일한 그룹 파싱을 사용한다.
- `make_cluster`에서 column alias(`-a`) 사용 시 순차 처리로 전환됨.
- `HashWatcher`는 설계만 존재하고 실행 경로에 미연결.
- Esperanza 벤치 스크립트는 rollback GID 목록을 action에 직접 전달하고 `full-replay`를 사용하며, `+` 복합 키 그룹을 반영한다.
- `MySQLBackupLoader`는 mysql 클라이언트 경로를 `MYSQL_BIN_PATH`/`MYSQL_BIN`/`MYSQL_PATH`에서 우선 조회하고, 디렉터리 값이면 `/mysql`를 붙이며 없으면 `/usr/bin/mysql`를 사용한다.
- `StateData` 역직렬화 시 문자열 메모리는 `malloc/free` 규약을 따라야 하며(`new/free` 혼용 시 크래시). 
- `ProcMatcher::trace()`는 프로시저 파라미터/`DECLARE` 로컬 변수와 `@var`를 심볼로 해석한다. `SELECT ... INTO`는 기본적으로 UNKNOWN 처리하지만, 이미 KNOWN 값(힌트/초기 변수)이 있으면 유지한다. 함수/복잡식은 UNKNOWN 처리되며, 해당 값이 key column에 쓰이면 wildcard로 남는다.

## 11. 테스트/IO 추상화
- DB 핸들 추상화: `mariadb::DBHandle`(interface) + `MySQLDBHandle` + `MockedDBHandle`, `DBResult`, `DBHandlePoolAdapter` 도입.
- 상태 로그/클러스터/백업 I/O 추상화: `IStateLogReader`/`MockedStateLogReader`, `IStateClusterStore`/`FileStateClusterStore`/`MockedStateClusterStore`, `IBackupLoader`/`MySQLBackupLoader`, `StateIO`로 묶어 주입.
- `StateLogReader::seekGid()`는 `GIDIndexReader` 기반으로 동작.
- 테스트는 Catch2 v3 + C++20 기준이며 `sqlparser-test`는 새 parser API를 사용한다.
- CMake 최소 버전은 3.28이며 fmt/spdlog는 FetchContent로 관리한다.

## 12. DECIMAL 처리
- `parserlib/ultparser_query.proto`의 `DMLQueryExpr.ValueType`에 `DECIMAL` 추가, `decimal` 문자열 필드로 전달.
- C++ `StateData`는 `en_column_data_decimal`로 정규화된 문자열을 저장하며 비교/해시/직렬화에서 DECIMAL을 지원.
- `StateItem`의 범위 교차/AND 처리와 `FUNCTION_WILDCARD` 처리가 개선되었고, `state_log_time.sec_part`는 `uint32_t`로 변경되었다.

## 13. MySQL libbinlogevents 통합 (Phase 1, 2026-01-21)
- CMake 옵션: `ULTRAVERSE_MYSQLD_SRC_PATH` (기본 `./mysql-server`), `libs/mysql/binlog/event` 존재 여부를 검사해 없으면 `FATAL_ERROR`.
- `mysql-server` 소스 디렉터리는 레포에서 ignore됨(`.gitignore`의 `/mysql-server`).
- 빌드 모듈: `cmake/mysql_binlogevents.cmake`에서 `mysql_binlog_event_standalone` 정적 라이브러리 정의.
  - GLOB 수집: `binlog/event`, `codecs`, `compression`, `serialization`, `gtid`, `containers`.
  - include 경로: `${ULTRAVERSE_MYSQLD_SRC_PATH}/libs`, `${ULTRAVERSE_MYSQLD_SRC_PATH}/include`, `${ULTRAVERSE_MYSQLD_SRC_PATH}`.
  - compile defs: `STANDALONE_BINLOG`, `BINLOG_EVENT_COMPRESSION_USE_ZSTD_system`.
  - link: `ZLIB::ZLIB`, `PkgConfig::ZSTD`.
- `src/CMakeLists.txt`에서 위 모듈을 include하고 `ultraverse` 의존성에 `mysql_binlog_event_standalone`을 연결.
- 빌드 성공 사례(ultraverse 타겟 한정):
  - `cmake -S . -B build`
  - `GOCACHE=/tmp/go-build-cache GOMODCACHE=/tmp/go-mod-cache GOPATH=/tmp/go cmake --build build --target ultraverse`
  - Go 캐시 권한 문제를 피하기 위해 `/tmp` 캐시 경로 사용.
  - `my_config.h`는 `${ULTRAVERSE_MYSQLD_SRC_PATH}/include/my_config.h`에 사용자가 제공해야 함.

## 14. MySQL binlog Reader/Adapter 통합 (Phase 2, 2026-01-21)
- `MySQLBinaryLogReaderV2` 추가: libbinlogevents 기반으로 FDE/체크섬/TRANSACTION_PAYLOAD_EVENT 처리 및 이벤트 매핑 수행.
- `BinaryLogSequentialReader`는 V2 리더를 사용하도록 전환됨.
- `Table_map_event`의 optional metadata(COLUMN_NAME)로 컬럼명 매핑하며, `binlog_row_metadata=FULL` 전제. 누락 시 경고 후 스킵.
- Row 이벤트는 column bitmap 기반으로 파싱하며, `PARTIAL_UPDATE_ROWS_EVENT`는 미지원(경고 후 스킵).
- `HashWatcher`는 MySQL binlog 시퀀셜 리더 경로로 전환됨.

## 15. MySQL binlog 전환 완료 (Phase 3, 2026-01-21)
- 자체 구현 binlog 리더 제거: `MySQLBinaryLogReader`(v1), `MariaDBBinaryLogReader` 삭제.
- `BinaryLogSequentialReader`를 MySQL 전용으로 단순화하고 V2 리더를 고정 사용.
- `statelogd`/`HashWatcher`는 `BinaryLogSequentialReader`를 사용하도록 정리.

## 16. MySQL binlog 추가 이벤트 지원 (Phase 4, 2026-01-21)
- `INTVAR_EVENT`/`RAND_EVENT`/`USER_VAR_EVENT`를 `DBEvent`로 디코딩하고 `statelogd`에서 다음 statement에 컨텍스트로 연결.
- `state::v2::Query`에 statement 컨텍스트(`last_insert_id`/`insert_id`/`rand_seed`/user var)를 직렬화하여 보존.
- replay 시 statement 실행 전에 `SET`을 적용해 동일 세션 상태를 재현.
- USER_VAR는 `SET @v := ...` 형태로 적용하며 charset/collation 매핑은 TODO로 남김.

</section>
<section id="agent-rules">
# AGENT RULES

## 1. Interaction & Language
- 작업을 진행할 때 확실하지 않거나 궁금한 점이 있으면, 되도록 **추측하지 말고 사용자에게 질문**해서 명확히 하는 것을 우선해 주세요.
- 사용자는 영어가 모어가 아닐 수도 있습니다. **모든 대화와 Plan 작성은 `$LANG` 환경 변수에 지정된 언어로 진행해 주세요.**
- 프로젝트에 대한 중요한 정보나 커다란 변경 사항이 있을 때는, `AGENTS.md`를 수정하여 프로젝트에 대한 최신 정보를 반영해 주세요.
- 작업 중 새롭게 확인한 사실(코드/동작/제약)은 `AGENTS.md`에 기록해 최신 상태를 유지해 주세요.
- **권한이 부족하여 작업을 수행할 수 없는 경우, 반드시 사용자에게 elevation 요청을 해야 합니다.** (If a command fails due to insufficient permissions, you must elevate the command to the user for approval.)

## 2. Workflow Protocol (중요)
Codex는 기본적으로 자율적(Autonomous)으로 행동하지만, 아래의 **[Explicit Plan Mode]** 조건에 해당할 경우 행동 방식을 변경해야 합니다.

### [Explicit Plan Mode] 트리거 조건
1. 사용자가 명시적으로 **'Plan 모드'**, **'계획 모드'**, 또는 **'설계 먼저'**라고 요청한 경우.
2. 작업이 **3개 이상의 파일**에 구조적 변경을 일으키거나, **Core Logic**을 건드리는 위험한 변경일 경우.

### [Explicit Plan Mode] 행동 수칙
위 조건이 발동되면 **즉시 코드 구현을 멈추고** 다음 절차를 따르세요:
1. **Stop:** 코드를 작성하거나 수정하지 마십시오. (파일 읽기는 가능)
2. **Plan:** `update_plan` 도구를 사용하여 **한국어**로 상세 구현 계획, 영향 범위, 예상 리스크를 작성하십시오.
3. **Ask:** 사용자에게 계획을 제시하고 **"이대로 진행할까요?"**라고 승인을 요청하십시오.
4. **Action:** 사용자의 명시적 승인(예: "ㅇㅇ", "진행해")이 떨어진 후에만 코드를 수정하십시오.

*(위 조건에 해당하지 않는 단순 수정이나 버그 픽스는 기존대로 승인 없이 즉시 처리하고 결과를 보고하십시오.)*

## COMMIT CONVENTIONS

- 만약 git commit을 작성할 때는 기존 커밋 컨벤션을 따르는 것을 우선하고, 당신 자신을 Co-author로 추가하지 말아주세요.
- 커밋 컨벤션은 다음과 같습니다.

```
[scope]: [subject]
```

- [scope]: 변경 사항의 범위를 나타내는 짧은 단어 (예: core, ui, docs 등)
- [subject]: 변경 사항을 간결하게 설명하는 문장 (명령문 형태)

### EXAMPLES
- `transport/quic: QUIC 연결 재시도 로직 추가`
- `msgdef/v1/channels: 채널 메시지 정의 업데이트`
- `docs(README): README 파일에 설치 가이드 추가`
- `test(transport/quic): QUIC 전송 테스트 케이스 작성`

## 3. 참고 문서
- **상세 알고리즘/이론 정보가 필요한 경우**: `ultraverse.md` 파일을 읽어서 참조하세요.
  - Retroactive Operation의 수학적 정의 및 워크플로우
  - R/W Set 생성 정책 (Table A)
  - Query Dependency Graph 생성 규칙
  - Query Clustering / Hash-Jumper 알고리즘 상세

## 4. AGENTS.md 업데이트 규칙
작업 중 다음과 같은 새로운 사실을 알게 되면 **반드시 AGENTS.md에 기록**해 주세요:
- 기존 문서에 없는 새로운 모듈/파일의 역할
- 코드 동작 방식에 대한 중요한 발견 (예: 숨겨진 제약, 버그 workaround)
- 빌드/테스트 관련 새로운 명령어나 주의사항
- CLI 옵션/환경변수/설정의 undocumented 동작
- 성능 관련 발견 (병목 지점, 최적화 팁)

기록 위치(권장):
- 파일 위치/역할 → `<section id="file-location-map">`
- 빌드/테스트 → `<section id="build-commands">`
- 구현 세부사항/실행 플로우 → `<section id="implementation-info">`
- 제약/주의사항 → `<section id="implementation-info">`의 `## 10. 현재 코드의 제약/주의`

## 5. 테스트 작성 규칙 (중요)
테스트를 작성할 때는 **반드시** 아래 사항을 준수해 주세요:

1. **`ultraverse.md`의 의도에 부합하는가?**
   - 테스트는 `ultraverse.md`에 정의된 Retroactive Operation의 목적과 동작 방식을 검증해야 합니다.
   - R/W Set 정책, Query Dependency 규칙, Clustering 로직 등 문서에 명시된 스펙을 기준으로 테스트를 설계하세요.

2. **항상 진실하게 작성하세요 — "통과하는 테스트"가 아니라 "올바른 테스트"를 만드세요.**
   - 당장 테스트가 실패(fail)하더라도, `ultraverse.md`의 의도에 맞는 테스트를 작성하세요.
   - 현재 구현이 스펙과 다르다면, 테스트는 스펙을 기준으로 작성하고 구현을 수정해야 합니다.
   - **목표는 테스트를 통과시키는 것이 아니라, 프로젝트의 완성도를 끌어올리는 것입니다.**
   - 기존 코드에 맞춰 테스트를 "조작"하거나, 항상 성공하도록 assertion을 약화시키지 마세요.

</section>

<section id="build-commands">
# Build & Test Commands

## Build
```bash
# Configure (필수: MySQL 소스 트리(=libbinlogevents)가 ./mysql-server 에 있어야 함)
cmake -S . -B build

# Build all targets (Go cache 권한 문제를 피하려면 /tmp 캐시 사용)
GOCACHE=/tmp/go-build-cache GOMODCACHE=/tmp/go-mod-cache GOPATH=/tmp/go cmake --build build

# Build specific target (ultraverse 라이브러리만)
GOCACHE=/tmp/go-build-cache GOMODCACHE=/tmp/go-mod-cache GOPATH=/tmp/go cmake --build build --target ultraverse

# Build specific executables
cmake --build build --target statelogd
cmake --build build --target db_state_change
cmake --build build --target state_log_viewer
```

## Test
```bash
# Run all tests
cd build && ctest

# Run specific test
cd build && ctest -R statecluster-test
cd build && ctest -R sqlparser-test

# Run test executable directly (더 자세한 출력)
./build/tests/statecluster-test
./build/tests/sqlparser-test
./build/tests/rowgraph-test
```

## Available Test Targets
- `statecluster-test`: StateCluster row-level 클러스터링 테스트
- `rowgraph-test`: RowGraph 병렬 실행 그래프 테스트
- `relationship-resolver-test`: column alias/FK 체인 해석 테스트
- `statehash-test`: 테이블 상태 해시 테스트
- `stateitem-test`: StateItem 범위 연산 테스트
- `query-transaction-serialization-test`: Query/Transaction cereal round-trip 테스트
- `sqlparser-test`: libultparser SQL 파싱 테스트
- `tabledependencygraph-test`: 테이블 의존성 그래프 테스트
- `naminghistory-test`: 테이블 이름 변경 이력 테스트
- `rowcluster-test`: RowCluster 테스트
- `taintanalyzer-test`: column taint 전파 테스트
- `queryeventbase-rwset-test`: Query R/W set 생성 테스트
- `procmatcher-trace-test`: 프로시저 트레이싱 테스트
- `statechanger-test`: StateChanger 통합 테스트

## Esperanza Mini Integration (Minishop)
```bash
# Minishop 통합 테스트 (MySQL 실행 → 시나리오 실행 → rollback/replay 검증)
python scripts/esperanza/minishop.py
```
- `ULTRAVERSE_HOME`은 `statelogd`/`db_state_change`/`state_log_viewer` 바이너리가 있는 경로를 가리켜야 합니다.
- 시나리오 SQL은 `scripts/esperanza/minishop/*.sql`, 프로시저 정의는 `scripts/esperanza/procdefs/minishop/*.sql`를 사용합니다.
- 프로시저 생성 시 `procpatcher`를 통해 힌트 삽입 패치를 적용하며, 세션 경로에 `procpatcher/__ultraverse__helper.sql`이 생성됩니다.

## Unit Test Notes (Esperanza)
- Esperanza 관련 유닛 테스트/스크립트 실행 시 `scripts/esperanza`로 이동한 뒤 실행합니다.
- 기본 타임아웃은 120분으로 설정합니다.
- 실행 시 권한 상승(elevated)으로 수행합니다.
- `python` 대신 `python3`를 사용합니다.

## Prerequisites
- CMake 3.28+
- C++20 지원 컴파일러
- Go (parserlib 빌드용)
- MySQL 소스 (libbinlogevents용, 기본 `./mysql-server`)
- Dependencies: Boost, OpenSSL, protobuf, graphviz(libgvc), zstd, tbb, libmysqlclient(또는 libmariadb)

</section>

<section id="file-location-map">
# File Location Map (파일 위치 → 역할)

## Executables (src/*.cpp)
| 파일 | 역할 |
|------|------|
| `src/statelogd.cpp` | binlog.index → `.ultstatelog`/`.ultchkpoint` 변환 데몬 |
| `src/db_state_change.cpp` | 상태 변경 CLI (make_cluster, rollback, prepend, replay, full-replay) |
| `src/state_log_viewer.cpp` | `.ultstatelog` 내용 확인용 뷰어 |
| `src/Application.cpp`, `src/Application.hpp` | getopt 기반 CLI 베이스 클래스 |

## Core Libraries
| 디렉터리/파일 | 역할 |
|---------------|------|
| `src/base/DBEvent.cpp`, `src/base/DBEvent.hpp` | SQL 파싱 결과 기반 R/W set 생성 및 공통 이벤트 로직 |
| `src/mariadb/DBEvent.cpp`, `src/mariadb/DBEvent.hpp` | MySQL binlog 이벤트 래퍼/디코딩 결과를 Query로 변환 |
| `src/mariadb/DBHandle.cpp`, `src/mariadb/DBHandle.hpp` | DB 연결/쿼리 실행 + 테스트용 Mock |
| `src/mariadb/state/WhereClauseBuilder.*` | statelogd/ProcMatcher 공용 WHERE 절 파싱 및 StateItem 생성 헬퍼 |
| `src/mariadb/binlog/BinaryLogSequentialReader.*` | binlog.index 기반 순차 읽기 |
| `src/mariadb/binlog/MySQLBinaryLogReaderV2.*` | libbinlogevents 기반 MySQL binlog 파싱/매핑 |

## State Management (src/mariadb/state/new/)
| 파일 | 역할 |
|------|------|
| `src/mariadb/state/new/Transaction.*`, `src/mariadb/state/new/Query.*` | 트랜잭션/쿼리 데이터 구조 + cereal 직렬화 |
| `src/mariadb/state/new/StateLogWriter.*`, `src/mariadb/state/new/StateLogReader.*` | `.ultstatelog` 파일 I/O |
| `src/mariadb/state/new/GIDIndexWriter.*`, `src/mariadb/state/new/GIDIndexReader.*` | `.ultindex` GID→오프셋 인덱스 |
| `src/mariadb/state/new/StateChanger.*` | 상태 변경 오케스트레이션(prepare/replay/full-replay) |
| `src/mariadb/state/new/StateChanger.prepare.cpp` | prepare 단계: column taint → row-wise 필터링 |
| `src/mariadb/state/new/StateChanger.replay.cpp` | replay 단계: 병렬 재실행 |
| `src/mariadb/state/new/StateChanger.sqlload.cpp` | SQL 로드 및 R/W set 계산 |
| `src/mariadb/state/new/StateChangePlan.*` | 상태 변경 계획(action, GID 범위, skip 목록 등) |
| `src/mariadb/state/new/StateChangeContext.hpp` | 실행 컨텍스트(replay 대상 GID, replace query 등) |
| `src/mariadb/state/new/StateChangeReport.*` | 리포트(JSON) 생성 |
| `src/mariadb/state/new/StateChangeReplayPlan.hpp` | `.ultreplayplan` 직렬화 스키마 |
| `src/mariadb/state/new/StateIO.*` | 상태 I/O 추상화(파일/Mock/백업 로더) |

## Clustering & Analysis (src/mariadb/state/new/)
| 파일 | 역할 |
|------|------|
| `src/mariadb/state/new/cluster/StateCluster.*` | row-level 클러스터 (key column별 StateRange→GID 매핑) |
| `src/mariadb/state/new/cluster/StateRelationshipResolver.*` | column alias/FK 체인 해석 |
| `src/mariadb/state/new/cluster/NamingHistory.*` | 테이블 이름 변경 이력 추적 |
| `src/mariadb/state/new/cluster/RowCluster.*` | legacy 클러스터 구현 |
| `src/mariadb/state/new/analysis/TaintAnalyzer.*` | column-wise taint 전파 분석 |
| `src/mariadb/state/new/graph/RowGraph.*` | 병렬 replay용 의존성 그래프/워커 스케줄링 |
| `src/mariadb/state/new/TableDependencyGraph.*` | 테이블 레벨 의존성 그래프 |
| `src/mariadb/state/new/ColumnDependencyGraph.*` | 컬럼 레벨 의존성 그래프 |
| `src/mariadb/state/new/HashWatcher.*` | Hash-Jumper용 상태 해시 감시(현재 실행 경로에는 미연결) |

## Procedure Handling (src/mariadb/state/new/)
| 파일 | 역할 |
|------|------|
| `src/mariadb/state/new/ProcCall.*` | 프로시저 호출 데이터 구조 |
| `src/mariadb/state/new/ProcMatcher.*` | 프로시저 정의에서 내부 쿼리 재구성/trace |
| `src/mariadb/state/new/ProcLogReader.*` | 프로시저 로그 읽기 |

## SQL Parser (parserlib/)
| 파일 | 역할 |
|------|------|
| `parserlib/capi.go` | C API 엔트리포인트(ult_sql_parser_create, ult_sql_parse_new 등) |
| `parserlib/parser/*` | TiDB parser fork 기반 파서 구현 |
| `parserlib/ultparser_query.proto` | 파싱 결과 protobuf 스키마 |

## Procpatcher (procpatcher/)
| 파일 | 역할 |
|------|------|
| `procpatcher/main.go` | stored procedure 패치 CLI |
| `procpatcher/delimiter/preprocessor.go` | DELIMITER-aware lexer: statement 분할 + 원문 byte offset 추적 |
| `procpatcher/patcher/text_patch.go` | OriginTextPosition 기반 원문 패치(들여쓰기 보존) |

## Esperanza (scripts/esperanza/)
| 파일 | 역할 |
|------|------|
| `scripts/esperanza/minishop.py` | Minishop 통합 테스트(시나리오 실행 후 rollback/replay 검증) |
| `scripts/esperanza/minishop/*.sql` | Minishop 스키마/프로시저/시나리오/검증 SQL |
| `scripts/esperanza/procdefs/minishop/*.sql` | statelogd ProcMatcher용 프로시저 정의 |

## Tests (tests/)
테스트 파일은 테스트 대상 모듈명 + `-test.cpp` 형식. Catch2 v3 사용.

</section>
