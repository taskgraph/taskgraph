package etcdutil

import (
	"strings"

	"github.com/coreos/go-etcd/etcd"
)

func IsKeyNotFound(err error) bool {
	if strings.Contains(err.Error(), "Key not found") {
		return true
	}
	return false
}

func ListKeys(nodes []*etcd.Node) []string {
	res := make([]string, len(nodes))
	for i, n := range nodes {
		res[i] = n.Key
	}
	return res
}
