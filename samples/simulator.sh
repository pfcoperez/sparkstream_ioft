#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: consolidate.sh INPUT_PATH"
    exit 0
fi

FILE_PATH=$1 

function getEntryTime {
    echo $1 | grep -Po "\"gcs_timestamp_ms\":[0-9]+" | awk -F ":" '{print $2}'  
}

ZERO_TIME=$(getEntryTime $(head -n 1 $1))

while read json ; do
    CUR_TIME=$(getEntryTime $json)
    T=$(echo "($CUR_TIME - $ZERO_TIME)/1000.0" | bc -l)
    sleep $T
    echo $json
done < $1 | nc -l -k 7891
