#!/bin/bash

declare -a memc_hosts=(
    "127.0.0.1:33013"
    "127.0.0.1:33014"
    "127.0.0.1:33015"
    "127.0.0.1:33016"
)

if [ "$1" = "stop" ]; then
    echo "stopping memcached"
    pkill -f memcached
else
    for host in "${memc_hosts[@]}"; do
        port=(${host//:/ })
        port=${port[1]}
        
        echo "Starting memcach on $host"
        memcached -d -p $port
    done
fi
