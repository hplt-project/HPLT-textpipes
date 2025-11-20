#!/bin/bash
ps -fa | grep arefev| awk '{sum+=$4; n+=1} END {print n" "sum"%"}'
for x in '^python.*glotlid' '^python.*openlid' '^python.*xml2md' ^zstd '^python.*muxdemux' ^t2sz ^rclone ^perl; do
    PIDS=$(pgrep -f $x)
    if [[ -z "$PIDS" ]]; then
        continue
    fi
    printf "%20s" "$x "; ps -up $PIDS | grep -vE 'PID' | awk '{cpu+=$3; mem+=$4; vsz+=$5; n+=1} END {print n"\t"cpu"%\t"mem"%\t"vsz/1024/1024}' | column -t -s' '

done
