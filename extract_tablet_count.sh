#!/bin/bash

ts_list=("sp-dat-hdp03-slv01.kbin.io" "sp-dat-hdp03-slv02.kbin.io" "sp-dat-hdp03-slv03.kbin.io" "sp-dat-hdp03-slv04.kbin.io" "sp-dat-hdp03-slv05.kbin.io" "sp-dat-hdp03-slv06.kbin.io" "sp-dat-hdp03-slv07.kbin.io" "sp-dat-hdp03-slv08.kbin.io" "sp-dat-hdp03-slv09.kbin.io" "sp-dat-hdp03-slv10.kbin.io" "sp-dat-hdp03-slv11.kbin.io" "sp-dat-hdp03-slv12.kbin.io" "sp-dat-hdp03-slv13.kbin.io" "sp-dat-hdp03-slv14.kbin.io" "sp-dat-hdp03-slv15.kbin.io" "sp-dat-hdp03-slv16.kbin.io" "sp-dat-hdp03-slv17.kbin.io" "sp-dat-hdp03-slv18.kbin.io" "sp-dat-hdp03-slv19.kbin.io" "sp-dat-hdp03-slv20.kbin.io" "sp-dat-hdp03-slv21.kbin.io" "sp-dat-hdp03-slv22.kbin.io" "sp-dat-hdp03-slv23.kbin.io" "sp-dat-hdp03-slv24.kbin.io" "sp-dat-hdp03-slv25.kbin.io")
for i in ${ts_list[@]}; do
    count=$(curl -s http://$i:8050/tablets | grep -w "$1" -A4 | grep "PARTITION &quot;$2&quot;" -A2 | grep '[0-9]G</td>' | sed 's/G<\/td>//g' | sed 's/<td>//g' | sed 's/ //g' | wc -l)
    echo "$i: $count"
done
