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

while [ 1 ]; do
    ZERO_TIME=$(getEntryTime $(head -n 1 $1))
    while read json ; do
	CUR_TIME=$(getEntryTime $json)
	T=$(echo "($CUR_TIME - $ZERO_TIME)/1000.0" | bc -l)
	sleep $T
	echo $json
    done < $1
done | nc -l -k 7891
