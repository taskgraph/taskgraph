package framework

import "github.com/go-distributed/meritop/pkg/etcdutil"

func (f *framework) standby() error {
	for {
		failedTask, err := etcdutil.WaitFailure(f.etcdClient, f.name, f.log)
		if err != nil {
			return err
		}
		f.log.Printf("standby got failure at task %d", failedTask)
		ok := etcdutil.TryOccupyTask(f.etcdClient, f.name, failedTask, f.ln.Addr().String())
		if ok {
			f.taskID = failedTask
			return nil
		}
		f.log.Printf("standby tried task %d failed. Wait failure again.", failedTask)
	}
}
