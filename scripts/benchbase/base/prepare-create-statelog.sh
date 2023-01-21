#!/bin/bash
source envvars
export PATH=$PATH:$ULTRAVERSE_HOME

statelogd -b server-binlog.index -o benchbase | tee statelogd.out

