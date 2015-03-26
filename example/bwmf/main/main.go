package main

import (
	"flag"
	"log"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/controller"
	"github.com/taskgraph/taskgraph/example/bwmf"
	"github.com/taskgraph/taskgraph/example/topo"
	"github.com/taskgraph/taskgraph/framework"
)

func main() {
	programType := flag.String("type", "", "(c) controller or (t) task")
	job := flag.String("job", "", "job name")
	rowShardPath := flag.String("row_file", "", "HDFS path to the row shard matrix.")
	columnShardPath := flag.String("column_file", "", "HDFS path to the column shard matrix.")
	blockId := flag.Int("block_id", 0, "ID of the sharded data block. 0 ~ num_tasks-1")
	numIters := flag.Int("num_iters", 10, "Num of iterations")
	alpha := flag.Float64("alpha", 0.5, "Parameter alpha in rojected gradient method.")
	beta := flag.Float64("beta", 0.5, "Parameter beta in rojected gradient method.")
	sigma := flag.Float64("sigma", 0.5, "Parameter sigma in rojected gradient method.")
	tolerance := flag.Float64("tolerance", 0.5, "Parameter tolerance in rojected gradient method.")
	etcdURLs := strings.Split(*flag.String("etcd_urls", "", "List of etcd instances, sep by ','."), ",")
	ntask := flag.Int("num_tasks", 1, "Num of task nodes.")

	flag.Parse()

	switch *programType {
	case "c":
		log.Printf("controller")
		controller := controller.New(*job, etcd.NewClient(etcdURLs), ntask)
		controller.Start()
		controller.WaitForJobDone()
	case "t":
		log.Printf("task")
		bootstrap := framework.NewBootStrap(*job, etcdURLs, createListener(), nil)
		taskBuilder := &bwmf.BWMFTaskBuilder{
			NumOfIters:      10,
			PGMsigma:        *sigma,
			PGMalpha:        *alpha,
			PGMbeta:         *beta,
			PGMtol:          *tolerance,
			blockId:         *blockId,
			rowShardPath:    *rowShardPath,
			columnShardPath: *columnShardPath,
		}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.SetTopology(topo.NewFullTopology(ntask))
		bootstrap.Start()
	default:
		log.Fatal("Please choose a type: (c) controller or (t) task.")
	}
}

func createListener() net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
