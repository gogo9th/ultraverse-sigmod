FROM mysql:8.0.32-debian

RUN apt -y update && apt -y upgrade

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul

RUN apt -y install wget build-essential cmake git libncurses-dev libgnutls28-dev bison

RUN mkdir /ultplugin-build
WORKDIR /ultplugin-build
RUN git clone https://github.com/mysql/mysql-server.git --branch mysql-8.0.32 --single-branch --recursive --depth 1 mysql-server

RUN mkdir mysql-server/ultbuild
COPY ultraverse_procassist/ /ultplugin-build/mysql-server/plugin/ultraverse_procassist
COPY cereal/ /ultplugin-build/mysql-server/plugin/ultraverse_procassist/cereal

WORKDIR /ultplugin-build/mysql-server/ultbuild
RUN apt -y install pkg-config libssl-dev
RUN mkdir -p /usr/local/src/boost_1_77_0
RUN cmake -DDOWNLOAD_BOOST=1 -DWITH_BOOST=/usr/local/src/boost_1_77_0 ..
RUN make -j8 ultraverse_procassist

RUN cp ./plugin_output_directory/ultraverse_procassist.so /usr/lib/mysql/plugin

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

EXPOSE 3306
CMD ["mysqld"]