package meritop

import (
	"fmt"
	"testing"

	"github.com/coreos/go-etcd/etcd"
)

// start an etcd server at localhost:4001 before start this test.
func TestBootstrap(t *testing.T) {
	t.Skip("")
	c := etcd.NewClient([]string{"http://localhost:4001"})
	ctl := controller{etcdclient: c}
	err := ctl.initTopo(20, 2)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan string, 150)
	statusmap := make(map[string]int)

	for i := 0; i < 150; i++ {
		go func(i int) {
			d, _ := establish(i)
			done <- d.status
		}(i)
	}

	for i := 0; i < 150; i++ {
		s := <-done
		statusmap[s] = statusmap[s] + 1
	}
	if statusmap["standby"] != 40 {
		t.Errorf("standb = %d, want 40", statusmap["standby"])
	}
	if statusmap["primary"] != 20 {
		t.Errorf("primary = %d, want 20", statusmap["primary"])
	}
}

func establish(i int) (dataServer, error) {
	c := etcd.NewClient([]string{"http://localhost:4001"})
	ds := dataServer{id: -1, etcdclient: c, context: fmt.Sprintf("10.10.0.%d", i)}
	err := ds.establishPrimary()
	if err != nil {
		return ds, ds.establishStandby()
	}
	return ds, nil
}
