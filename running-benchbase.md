# RUNNING BENCHBASE

## 1. 의존성 설치

```shell
$ sudo apt install build-essential cmake mariadb libmariadb-dev libmariadb-dev-compat pkg-config bison flex libboost-all-dev libfmt-dev libspdlog-dev libgvc6 graphviz-dev
$ sudo apt install perl
```

## 2. Docker 설치

```shell
$ sudo mkdir -p /etc/apt/keyrings
$ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

$ echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

$ sudo apt update
$ sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

## 3. ultraverse 빌드 

```shell
$ git clone $ULTRAVERSE_GIT
$ cd ultraverse
$ git submodule init
$ git submodule update
$ mkdir build && cd build 
$ cmake ..
$ make -j8 
```

## 4. 벤치마크 부트스트래핑

```shell
export ULTRAVERSE_HOME=$HOME/ultraverse/build/src
export BENCHBASE_HOME=$HOME/benchbase

cd ultraverse/scripts/benchbase

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