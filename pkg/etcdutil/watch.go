package etcdutil

import (
	"log"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
)

func WatchEpoch(client *etcd.Client, appname string, stop chan bool) <-chan uint64 {
	epochC := make(chan uint64)
	receiver := make(chan *etcd.Response, 1)
	go client.Watch(MakeJobEpochPath(appname), 1, false, receiver, stop)

	go func() {
		for resp := range receiver {
			if resp.Action != "compareAndSwap" && resp.Action != "set" {
				continue
			}
			epoch, err := strconv.ParseUint(resp.Node.Value, 10, 64)
			if err != nil {
				log.Fatal("etcdutil: can't parse epoch from etcd")
			}
			epochC <- epoch
		}
	}()
	return epochC
}
