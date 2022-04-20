#!/bin/bash

ts_list=(`curl -s http://<kudu master hostname>:8051/tablet-servers | grep "<td><a href=" | sed '/Dead/,+1d' | sed '/heartbeat/,+2d'  | sed '/Dead Tablet Servers/,+1d' | sed 's/<td><a href="http:\/\///g' | sed 's/:8050.*//g' | sed 's/ //g'`)
ts_list_sort=(`echo "${ts_list[@]}" | sed 's/ /\n/g' | sort`)

for i in ${ts_list_sort[@]}; do
    count=$(curl -s http://$i:8050/tablets | grep -w 'RUNNING' | head -n 1 | sed 's/[<td>|<tr>|/]/ /g' | awk '{print $2}')
    echo "$i: $count"
done
