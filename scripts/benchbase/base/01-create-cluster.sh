#!/bin/bash
source envvars
export PATH=$PATH:$ULTRAVERSE_HOME

db_state_change \
        -i benchbase \
        -d benchbase \
        -k "EDITME:KEYCOLUMNS" \
        make_cluster | tee 'cluster.out'


