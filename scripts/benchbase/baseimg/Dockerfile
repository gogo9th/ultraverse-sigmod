FROM mariadb:10.9.4

RUN apt -y update && apt -y upgrade

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul

RUN apt -y install wget build-essential cmake git libboost-all-dev libncurses-dev libgnutls28-dev bison

RUN mkdir /ultplugin-build
WORKDIR /ultplugin-build
RUN git clone https://github.com/MariaDB/server.git --branch 10.9 --single-branch --recursive --depth 1 mariadb-server

RUN mkdir mariadb-server/ultbuild
COPY ultraverse_procassist/ /ultplugin-build/mariadb-server/plugin/ultraverse_procassist
COPY cereal/ /ultplugin-build/mariadb-server/plugin/ultraverse_procassist/cereal

WORKDIR /ultplugin-build/mariadb-server/ultbuild
RUN cmake ..
RUN make -j8 ultraverse_procassist

RUN cp plugin/ultraverse_procassist/ultraverse_procassist.so /usr/lib/mysql/plugin

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

EXPOSE 3306
CMD ["mariadbd"]