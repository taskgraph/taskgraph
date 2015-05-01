#!/bin/sh

ETCDBIN=$GOPATH/etcd-v2.0.5-darwin-amd64

# clear etcd
$ETCDBIN/etcdctl rm --recursive mapreduce+test/

echo $azureAccountKey
./ex -job="mapreduce test" -type=c -azureAccountKey=$azureAccountKey > www.txt&

# need to wait for controller to setup
sleep 2
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www1.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www2.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www3.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www4.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www5.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www6.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www7.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www8.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www9.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www10.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www11.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www12.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www13.txt &
./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www14.txt &
 


