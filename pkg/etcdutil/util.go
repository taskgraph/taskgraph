package etcdutil

import (
	"log"

	"github.com/coreos/go-etcd/etcd"
)

func ListKeys(nodes []*etcd.Node) []string {
	res := make([]string, len(nodes))
	for i, n := range nodes {
		res[i] = n.Key
	}
	return res
}

func MustCreate(c *etcd.Client, logger *log.Logger, key, value string, ttl uint64) {
	if _, err := c.Create(key, value, ttl); err != nil {
		logger.Panicf("controller create failed. Key: %s, err: %v", key, err)
	}
}
