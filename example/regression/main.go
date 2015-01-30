package main

import (
	"flag"
	"log"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop/controller"
	"github.com/go-distributed/meritop/example"
	"github.com/go-distributed/meritop/framework"
)

func main() {
	programType := flag.String("type", "", "(c) controller or (t) task")
	job := flag.String("job", "", "job name")
	etcdURLs := []string{"http://localhost:4001"}
	flag.Parse()

	if *job == "" {
		log.Fatalf("Please specify a job name")
	}

	ntask := uint64(2)
	switch *programType {
	case "c":
		log.Printf("controller")
		controller := controller.New(*job, etcd.NewClient(etcdURLs), ntask)
		controller.Start()
		pause := make(chan struct{})
		<-pause
		//currently only manually ctrl+c is supported
	case "t":
		log.Printf("task")
		bootstrap := framework.NewBootStrap(*job, etcdURLs, createListener(), nil)
		taskBuilder := &framework.SimpleTaskBuilder{
			GDataChan:          make(chan int32, 11),
			FinishChan:         make(chan struct{}),
			NumberOfIterations: 10,
		}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.SetTopology(example.NewTreeTopology(2, ntask))
		bootstrap.Start()
	default:
		log.Fatal("Please choose a type: (c) controller, (t) task")
	}
}

func createListener() net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
