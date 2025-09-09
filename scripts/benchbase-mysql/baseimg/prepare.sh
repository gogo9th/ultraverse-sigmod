#!/bin/sh
ULTRAVERSE_SRC_PATH=../../../

rm -rf ultraverse_procassist

cp -rv $ULTRAVERSE_SRC_PATH/mysql/ultraverse_procassist_mysql ./ultraverse_procassist
cp -rv $ULTRAVERSE_SRC_PATH/dependencies/cereal .
