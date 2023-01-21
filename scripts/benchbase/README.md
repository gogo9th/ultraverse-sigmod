# NAME
benchbase-bootstrap - benchbase를 사용한 테스트 환경을 bootstrap 합니다.

# SYNOPSIS
```shell
export ULTRAVERSE_HOME=$HOME/ultraverse/build/release
export BENCHBASE_HOME=$HOME/benchbase

# epinions / 1m 환경을 부트스트래핑 합니다
./bootstrap.pl epinions 1m

# 준비된 디렉토리로 이동합니다
cd runs/1234-epinions-1m

# ULTRAVERSE_HOME 등 환경 변수를 적절히 설정합니다.
vi envvars

# 키 컬럼 등 db_state_change로의 옵션을 설정합니다.
# 상세한 사항은 db_state_change -h 결과물을 참조해 주십시오.
vi 01-create-cluster.sh
vi 02-testcase-main.sh


# 분리된 MariaDB 도커 인스턴스를 실행합니다.
docker compose up -d

# 상태 전환을 위한 로그를 만듭니다.
./prepare-create-statelog.sh
# 클러스터 맵 / 인덱스 등을 생성합니다.
./01-create-cluster.sh
# 메인 테스트 케이스를 생성합니다.
./02-testcase-main.sh

# 테스트가 끝나면, MariaDB 도커 인스턴스를 종료합니다.
docker compose stop
```

# REQUIREMENTS

- Docker
- Perl 5.32 이상 (Perl 6으로 불리는 Raku / Rakudo는 호환되지 않습니다)

# NOTE

- MariaDB 도커 인스턴스는 호스트 PC의 3306 포트를 점유합니다.

  만약 MariaDB / MySQL을 호스트에서 이미 실행하고 있는 경우 정지해 주십시오.

- Perl 5.32에서 기본 제공되는 코어 모듈만으로 작성하였으나, 일부 환경에 따라 모듈이 누락되어 별도 설치가 필요할 수도 있습니다.

  (예를 들어 Ubuntu의 경우 `IPC::Open2`라는 모듈은 `apt install libipc-open2-perl` 같은 명령어로 설치가 가능합니다)
