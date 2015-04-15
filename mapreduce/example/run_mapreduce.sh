#!/bin/sh

ETCDBIN=$GOPATH/etcd-v2.0.5-darwin-amd64

# clear etcd
$ETCDBIN/etcdctl rm --recursive /


./ex -job="mapreduce test" -type=c -azureAccountKey=$azureAccountKey &

# need to wait for controller to setup
sleep 1
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey &


trap "kill 0" SIGINT SIGTERM EXIT