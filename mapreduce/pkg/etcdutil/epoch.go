package etcdutil

import (
	"log"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
)

func GetAndWatchEpoch(client *etcd.Client, appname string, epochC chan uint64, stop chan bool) (uint64, error) {
	resp, err := client.Get(EpochPath(appname), false, false)
	if err != nil {
		log.Fatal("etcdutil: can not get epoch from etcd")
	}
	ep, err := strconv.ParseUint(resp.Node.Value, 10, 64)
	if err != nil {
		return 0, err
	}
	receiver := make(chan *etcd.Response, 1)
	go client.Watch(EpochPath(appname), resp.EtcdIndex+1, false, receiver, stop)
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

	return ep, nil
}

func CASEpoch(client *etcd.Client, appname string, prevEpoch, epoch uint64) error {
	prevEpochStr := strconv.FormatUint(prevEpoch, 10)
	epochStr := strconv.FormatUint(epoch, 10)
	_, err := client.CompareAndSwap(EpochPath(appname), epochStr, 0, prevEpochStr, 0)
	return err
}
