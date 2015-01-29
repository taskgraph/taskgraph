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

```
./run_controller.sh
```

### Start task

```
./run_regression.sh
```
Do it in two terminals. Because by default there are two tasks.

### Clean up
No clean up needed.