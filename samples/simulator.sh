#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: simulator.sh INPUT_PATH"
    exit 0
fi

FILE_PATH=$1 

function getEntryTime {
    ## For Mac: echo $1 | perl -nle 'print $& if m{"gcs_timestamp_ms":[0-9]+}' | awk -F ":" '{print $2}'
    echo $1 | grep -Po "\"gcs_timestamp_ms\":[0-9]+" | awk -F ":" '{print $2}'  
}

ZERO_TIME=$(getEntryTime $(head -n 1 $1))

while [ 1 ]; do
    PREV_TIME=$ZERO_TIME
    while read json ; do
	CUR_TIME=$(getEntryTime $json)
	T=$(echo "($CUR_TIME - $PREV_TIME)/1000.0" | bc -l | cut -c1-7)
	SIGN=$(echo $T | cut -c1-1)
	if [ $SIGN != "-" ]; then
	    sleep $T;
	    PREV_TIME=$CUR_TIME
	fi;
	echo $json
    done < $1
done | nc -l -k 7891
