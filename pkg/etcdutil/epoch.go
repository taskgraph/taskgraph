package etcdutil

import (
	"log"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
)

func WatchEpoch(client *etcd.Client, appname string, stop chan bool) <-chan uint64 {
	epochC := make(chan uint64)
	receiver := make(chan *etcd.Response, 1)
	go client.Watch(EpochPath(appname), 1, false, receiver, stop)

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

func CASEpoch(client *etcd.Client, appname string, prevEpoch, epoch uint64) error {
	prevEpochStr := strconv.FormatUint(epoch, 10)
	epochStr := strconv.FormatUint(epoch+1, 10)
	_, err := client.CompareAndSwap(EpochPath(appname), epochStr, 0, prevEpochStr, 0)
	return err
}

func GetEpoch(client *etcd.Client, appname string) (uint64, error) {
	resp, err := client.Get(EpochPath(appname), false, false)
	if err != nil {
		log.Fatal("etcdutil: can not get epoch from etcd")
	}
	return strconv.ParseUint(resp.Node.Value, 10, 64)
}
