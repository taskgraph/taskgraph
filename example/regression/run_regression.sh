#!/bin/sh

ETCDBIN=$GOPATH/etcd-v2.0.0-darwin-amd64
RESULT_FILE="result.txt"

# clear etcd
$ETCDBIN/etcdctl rm --recursive /
rm $RESULT_FILE

./regression -job="regression test" -type=c &

# need to wait for controller to setup
sleep 1
./regression -job="regression test" -type=t &
./regression -job="regression test" -type=t &

while [ ! -e $RESULT_FILE ]; do
    sleep 1
done

echo "Result has been written to file 'result.txt'. Reading the file:"
echo "===== RESULT ====="
cat $RESULT_FILE

trap "kill 0" SIGINT SIGTERM EXIT