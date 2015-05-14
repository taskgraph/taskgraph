#!/bin/bash

# For Go plugin update, please see:
#   https://github.com/grpc/grpc-common/tree/master/go

go get -u github.com/coreos/go-etcd/etcd
go get -u github.com/colinmarc/hdfs
go get -u golang.org/x/net/context
go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/proto
go get -u golang.org/x/tools/cmd/vet
go get -u github.com/golang/lint/golint
go get -u github.com/Azure/azure-sdk-for-go/storage


