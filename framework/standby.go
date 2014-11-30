package framework

import "github.com/go-distributed/meritop/pkg/etcdutil"

func (f *framework) standby() error {
	for {
		failedTask := etcdutil.WaitFailure(f.etcdClient, f.name)
		ok := etcdutil.TryOccupyTask(f.etcdClient, f.name, failedTask, f.ln.Addr().String())
		if ok {
			f.taskID = failedTask
			return nil
		}
	}
}
