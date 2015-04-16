#!/bin/sh

ETCDBIN=$GOPATH/etcd-v2.0.5-darwin-amd64

# clear etcd
$ETCDBIN/etcdctl rm --recursive mapreduce+test/


./ex -job="mapreduce test" -type=c -azureAccountKey=$azureAccountKey > www.txt&

# need to wait for controller to setup
sleep 2
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www1.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www2.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www3.txt &


