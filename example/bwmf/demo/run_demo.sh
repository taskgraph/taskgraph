#!/bin/env bash

# saving current dir
pushd $(pwd)

# goto script dir and stack it, we'll be back later
pushd $(dirname $0)

# goto project root, we start etcd there
pushd ../../..

# This is exactly the same as .script/test.

ETCDTESTDIR=".tmp/etcd-testdir"

if [ ! -d ".tmp" ]; then
    mkdir .tmp
fi

cleanup() {
  kill $(jobs -p) 2>/dev/null
  rm -rf $ETCDTESTDIR
}
# cleanup on any kind of exit
trap "cleanup" SIGINT SIGTERM EXIT

is_etcd_up_on_4001() {
  if curl -fs "http://localhost:4001/v2/machines" 2>/dev/null; then
      return 0
  fi
  return 1
}

if [ ! is_etcd_up_on_4001 ] ; then

  if [ -d "$ETCDTESTDIR" ]; then
    rm -rf $ETCDTESTDIR
  fi

  if [[ "$OSTYPE" == "darwin"* ]]; then
    .script/bin/etcd-v2.0.8-darwin --data-dir $ETCDTESTDIR > /dev/null 2>&1 &
  else
    .script/bin/etcd-v2.0.8-linux --data-dir $ETCDTESTDIR > /dev/null 2>&1 &
  fi

  for i in $(seq 10); do
    sleep 1
    if is_etcd_up_on_4001; then
      break
    fi
  done
fi

if is_etcd_up_on_4001 ; then
  echo "etcd is running on localhost:4001"
else
  echo "etcd failed to run on localhost:4001"
  exit 1
fi

# go back to script dir
popd

go build

./demo -job="bwmf_test" -type=c

# need to wait for controller to setup
sleep 1

./demo -job="bwmf_test" -type=t -row_file=dummy -column_file=dummy \
    -latent_dim=2 -block_id=0 -num_iters=10 -alpha=0.5 -beta=0.1 -sigma=0.01 \
    -tolerance=0.0001 -etcd_urls="http://localhost:4001/" \
    -num_tasks=2 &

./demo -job="bwmf_test" -type=t  -row_file=dummy -column_file=dummy \
    -latent_dim=2 -block_id=1 -num_iters=10 -alpha=0.5 -beta=0.1 -sigma=0.01 \
    -tolerance=0.0001 -etcd_urls="http://localhost:4001/" \
    -num_tasks=2 &

echo "Finished."

# go back to original dir and leave
popd 

trap "kill 0" SIGINT SIGTERM EXIT
