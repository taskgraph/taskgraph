package etcdutil

import "github.com/coreos/go-etcd/etcd"

func ListKeys(nodes []*etcd.Node) []string {
	res := make([]string, len(nodes))
	for i, n := range nodes {
		res[i] = n.Key
	}
	return res
}
