#!/bin/env bash

ETCDBIN=$GOPATH/etcd-v2.0.0-darwin-amd64
RESULT_FILE="result.txt"

# clear etcd
$ETCDBIN/etcdctl rm --recursive /
rm $RESULT_FILE

./regression -job="bwmf test" -type=c


# need to wait for controller to setup
sleep 1
./regression -job="bwmf test" -type=t -row_file=dummy -column_file=dummy \
    -latent_dim=2 -block_id=0 -num_iters=10 -alpha=0.5 -beta=0.1 -sigma=0.01 \
    -tolerance=0.0001 -etcd_urls="http://localhost:4001/v2/" \
    -num_tasks=2 &

./regression -job="bwmf test" -type=t  -row_file=dummy -column_file=dummy \
    -latent_dim=2 -block_id=1 -num_iters=10 -alpha=0.5 -beta=0.1 -sigma=0.01 \
    -tolerance=0.0001 -etcd_urls="http://localhost:4001/v2/" \
    -num_tasks=2 &

#while [ ! -e $RESULT_FILE ]; do
#    sleep 1
#done

echo "Finished."

trap "kill 0" SIGINT SIGTERM EXIT
