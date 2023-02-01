#!/bin/bash
source envvars
export PATH=$PATH:$ULTRAVERSE_HOME

statelogd -M -b server-binlog.index -o benchbase | tee statelogd.out

