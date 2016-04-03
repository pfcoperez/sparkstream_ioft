#!/bin/bash

# This script uses a set of independent JSON entries separated by `\n` within
# the same file to build a list of JSON objects containing all of them.

if [ $# -ne 1 ]; then
    echo "Usage: consolidate.sh INPUT_PATH"
    exit 0
fi

awk '
BEGIN{print "["}
{
  print (NR==1?"":",")$0
}
END{print "]"}
' $1  # | xclip -selection "clipboard"
