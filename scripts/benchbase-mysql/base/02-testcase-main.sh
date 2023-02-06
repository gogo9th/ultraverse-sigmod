#!/bin/bash
source envvars
export PATH=$PATH:$ULTRAVERSE_HOME

yes Y | time db_state_change \
        -b dbdump.sql \
        -i benchbase \
        -d benchbase \
        -k "EDITME:KEYCOLUMNS" \
        rollback=2 | tee 'testcase.out'


