package main 

import (
	"flag"
	"log"
	"net"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
	"../../controller"
	"../mapreduce"
	"../../framework"
)

func main() {
	programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) reducer")
	job := flag.String("job", "", "job name")
	mapperNum := strconv.Atoi(flag.String("mapperNum", "0"))
	shuffleNum := strconv.Atoi(flag.String("mapperNum", "0"))
	reducerNum := strconv.Atoi(flag.String("reducerNum", "0"))
	azureAccountName := flag.String("azureAccountName", "")
	azureAccountKey := flag.String("azureAccountKey", "")
	outputContainerName := flag.String("outputContainerName", "")
	outputBlob

	etcdURLs := []string{"http://localhost:4001"} 
	mapper
	flag.Parse()
	if *job = "" {
		log.Fatalf("Please specify a job name")
	}
	switch *programType {
	case "c":
		log.Printf("controller")
		controller := controller.New(*job, etcd.NewClient(etcdURLs), ntask)
		controller.Start()
		controller.WaitForJobDone()
	case "t":
		log.Printf("task")
		bootstrap := framework.NewBootStrap(*job, etcdURLs, createListener(), nil)
		taskBuilder := &mapreduce.MapreduceTaskBuilder{
			MasterConfig:       map[string][]string{"writefile": {"result.txt"}}
		}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.SetTopology(mapreduce.NewMapReduceTopology(mapperNum, shuffleNum, reducerNum))
		bootstrap.Start()
	default:
		log.Fatal("Please choose a type: (c) controller, (t) task")
	} 
}
