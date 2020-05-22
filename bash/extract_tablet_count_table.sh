#!/bin/bash

ts_list=(`curl -s http://sp-dat-hdp02-mst02.kbin.io:8051/tablet-servers | grep "<td><a href=" | sed '/Dead/,+1d' | sed '/heartbeat/,+2d'  | sed '/Dead Tablet Servers/,+1d' | sed 's/<td><a href="http:\/\///g' | sed 's/:8050.*//g' | sed 's/ //g'`)
ts_list_sort=(`echo "${ts_list[@]}" | sed 's/ /\n/g' | sort`)

for i in ${ts_list_sort[@]}; do
    count=$(curl -s http://$i:8050/tablets | grep -w "$1" -A4 | grep 'RUNNING' -A1 -B1 | grep '[0-9][M,G,T]</td>' | sed 's/G<\/td>//g' | sed 's/<td>//g' | sed 's/ //g' | wc -l)
    echo "$i: $count"
done
