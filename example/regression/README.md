Regression Framework
======

How to run
------

### Build
```
go build
```
This should generate a binary call `regression`.

### Start etcd server

Download an etcd binary from:
https://github.com/coreos/etcd/releases

Unzip, start it by doing
```
./etcd
```

The example assumes the default port 4001

### Start controller

Modify `run_controller.sh` file with the correct `ETCDBIN` setting. Then run

```
./run_controller.sh
```

### Clean up
A binary: `regression`.
An output file: `result.txt`.
