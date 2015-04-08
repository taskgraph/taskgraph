package main

import (
	"flag"
	"log"
	"net"
	// "strings"

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
	// XXX(baigang): We will be dealing with local fs first.
	latentDim := flag.Int("latent_dim", 1, "Latent dimension.")
	ntask := flag.Int("num_tasks", 1, "Num of task nodes.")
	blockId := flag.Int("block_id", 0, "ID of the sharded data block. 0 ~ num_tasks-1")
	numIters := flag.Int("num_iters", 10, "Num of iterations")
	alpha := flag.Float64("alpha", 0.5, "Parameter alpha in rojected gradient method.")
	beta := flag.Float64("beta", 0.5, "Parameter beta in rojected gradient method.")
	sigma := flag.Float64("sigma", 0.5, "Parameter sigma in rojected gradient method.")
	tolerance := flag.Float64("tolerance", 0.5, "Parameter tolerance in rojected gradient method.")
	//etcdURLs := strings.Split(*flag.String("etcd_urls", "", "List of etcd instances, sep by ','."), ",")
	{
		tmp := *flag.String("etcd_urls", "", "List of etcd instances, sep by ','.")
		tmp = tmp
	}
	etcdURLs := []string{"http://localhost:4001"}

	flag.Parse()
	numTasks := uint64(*ntask)

	var rowShardBuf, columnShardBuf []byte

	switch *programType {
	case "c":
		log.Printf("controller")
		// controller := controller.New(*job, etcd.NewClient(etcdURLs), numTasks, []string{"Parents", "Children"})
		controller := controller.New(*job, etcd.NewClient(etcdURLs), numTasks, []string{"Neighbors", "Master"})
		controller.Start()
		controller.WaitForJobDone()
	case "t":
		log.Printf("task")

		// load sharded matrix data here
		if false {
			shardLoader := bwmf.NewLocalBufLoader()
			lslErr := shardLoader.Init()
			if lslErr != nil {
				log.Panicf("Initializing shardLoader failed: %s", lslErr)
			}
			rShardBuf, rbErr := shardLoader.ReadAll(*rowShardPath)
			if rbErr != nil {
				log.Panicf("Failed reading rowShardBuf: %s", rbErr)
			}
			cShardBuf, cbErr := shardLoader.ReadAll(*columnShardPath)
			if cbErr != nil {
				log.Panicf("Failed reading columnShardBuf: %s", cbErr)
			}
			rowShardBuf = rShardBuf
			columnShardBuf = cShardBuf
		} else {
			// NOTE(baigang): We use fake data for local test/demo.
			rowShardBuf0, columnShardBuf0, rowShardBuf1, columnShardBuf1, bufErr := getBufs()
			if bufErr != nil {
				log.Fatalf("Failed getting fake bufs")
			}
			if *blockId == 0 {
				rowShardBuf = rowShardBuf0
				columnShardBuf = columnShardBuf0
			} else {
				rowShardBuf = rowShardBuf1
				columnShardBuf = columnShardBuf1
			}
		}

		bootstrap := framework.NewBootStrap(*job, etcdURLs, createListener(), nil)
		taskBuilder := &bwmf.BWMFTaskBuilder{
			NumOfIters:     uint64(*numIters),
			NumOfTasks:     numTasks,
			PgmSigma:       float32(*sigma),
			PgmAlpha:       float32(*alpha),
			PgmBeta:        float32(*beta),
			PgmTol:         float32(*tolerance),
			BlockId:        uint32(*blockId),
			K:              *latentDim,
			RowShardBuf:    rowShardBuf,
			ColumnShardBuf: columnShardBuf,
			WorkPath:       "./",
		}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.SetTopology(topo.NewFullTopology(numTasks))
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
