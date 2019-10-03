#!/bin/bash

declare -a memc_hosts=(
    "127.0.0.1:33013"
    "127.0.0.1:33014"
    "127.0.0.1:33015"
    "127.0.0.1:33016"
)

start() {
    for host in "${memc_hosts[@]}"; do
        port=(${host//:/ })
        port=${port[1]}
        
        echo "Starting memcach on $host"
        memcached -d -p $port
    done
}

stop () {
    echo "stopping memcached"
    pkill -f memcached
}
case $1 in
    "stop")
        stop
        ;;
    "reload")
        stop
        start
        ;;
    *)
        start
        ;;
esac
