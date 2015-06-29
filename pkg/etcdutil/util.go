package etcdutil

import (
	"log"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
)

func ListKeys(nodes []*etcd.Node) []string {
	res := make([]string, len(nodes))
	for i, n := range nodes {
		res[i] = n.Key
	}
	return res
}

func MustCreate(c *etcd.Client, logger *log.Logger, key, value string, ttl uint64) *etcd.Response {
	resp, err := c.Create(key, value, ttl)
	if err != nil {
		logger.Panicf("Create failed. Key: %s, err: %v", key, err)
	}
	return resp
}

func AtomicInc(c *etcd.Client, key string) error {
	resp, err := c.Get(key, false, false)
	if err != nil {
		return err
	}

	valstr := resp.Node.Value
	val, err := strconv.Atoi(valstr)
	if err != nil {
		return err
	}

	for {
		resp, err = c.CompareAndSwap(key, strconv.Itoa(val+1), 0, valstr, 0)
		if err == nil {
			break
		}
		resp, err = c.Get(key, false, false)
		if err != nil {
			return err
		}
		valstr = resp.Node.Value
		val, err = strconv.Atoi(valstr)
		if err != nil {
			return err
		}
	}

	return nil
}
