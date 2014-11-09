package meritop

import (
	"path"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
)

const (
	keyPrefix = "/meritop"
)

type controller struct {
	etcdclient *etcd.Client
}

func (c *controller) initTopo(size, replica int) error {
	_, err := c.etcdclient.Create(path.Join(keyPrefix, "size"), strconv.Itoa(size), 0)
	if err != nil {
		return err
	}
	for i := 0; i < size; i++ {
		n := strconv.Itoa(i)
		_, err := c.etcdclient.Create(path.Join(keyPrefix, "dataserver", n), "empty", 0)
		if err != nil {
			return err
		}
	}
	_, err = c.etcdclient.Create(path.Join(keyPrefix, "nextslot"), strconv.Itoa(size-1), 0)

	rsize := replica * size
	for i := 0; i < rsize; i++ {
		n := strconv.Itoa(i)
		_, err := c.etcdclient.Create(path.Join(keyPrefix, "replica", n), "empty", 0)
		if err != nil {
			return err
		}
	}

	_, err = c.etcdclient.Create(path.Join(keyPrefix, "rnextslot"), strconv.Itoa(rsize-1), 0)
	return err
}
