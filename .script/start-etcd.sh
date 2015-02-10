#!/bin/sh

ETCDTESTDIR=".tmp/etcd-testdir"

if [ ! -d ".tmp" ]; then
    mkdir .tmp
fi

if [ -d "$ETCDTESTDIR" ]; then
  rm -r $ETCDTESTDIR
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
    .script/etcd --data-dir $ETCDTESTDIR &
else
    .script/etcd-v2.0.0-linux-amd64/etcd --data-dir $ETCDTESTDIR &
fi

# etcd boot-up
sleep 3
