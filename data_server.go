package meritop

import (
	"errors"
	"log"
	"path"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
)

var (
	errFullDataServer = errors.New("meritop: data server is full")
)

type dataServer struct {
	id         int
	status     string
	context    string
	etcdclient *etcd.Client
}

func (ds *dataServer) establishPrimary() error {
	err := ds.establish("nextslot", "dataserver")
	if err != nil {
		return err
	}
	ds.status = "primary"
	return nil
}

func (ds *dataServer) establishStandby() error {
	err := ds.establish("rnextslot", "replica")
	if err != nil {
		return err
	}
	ds.status = "standby"
	return nil
}

func (ds *dataServer) establish(nextkey, location string) error {
	// grab slot -> decrese count by compareandswap
	// publish data to the slot
	nextslotkey := path.Join(keyPrefix, nextkey)
	var next int
	for {
		resp, err := ds.etcdclient.Get(nextslotkey, false, false)
		if err != nil {
			return err
		}
		next, err = strconv.Atoi(resp.Node.Value)
		if err != nil {
			return err
		}
		if next != -1 {
			log.Printf("datasever: try to establish %d at %s", next, location)
			slot := path.Join(keyPrefix, location, resp.Node.Value)
			_, err = ds.etcdclient.CompareAndSwap(slot, ds.context, 0, "empty", 0)
			if err != nil {
				continue
			}
			log.Printf("dataserver: established %s at %s [%s]", slot, location, ds.context)
			break
		} else {
			return errFullDataServer
		}
	}
	ds.id = next
	ds.etcdclient.CompareAndSwap(nextslotkey, strconv.Itoa(next-1), 0, strconv.Itoa(next), 0)
	return nil
}
