#!/bin/sh

ETCDBIN=$GOPATH/etcd-v2.0.0-darwin-amd64

$ETCDBIN/etcdctl rm --recursive /

./regression -job="haha" -type=c
