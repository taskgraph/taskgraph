package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/plutoshe/taskgraph/controller"
	"github.com/plutoshe/taskgraph/topo"
	"github.com/taskgraph/taskgraph/example/bwmf"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph/framework"
)

func main() {

	etcdUrlList := flag.String("etcd_urls", "", "ETCD server lists, sep by a comma.")
	jobName := flag.String("job_name", "bwmf", "Job name in etcd path.")
	jobType := flag.String("job_type", "c", "Job type, either 'c' for controller or 't' for task.")
	numTasks := flag.Int("num_tasks", 1, "Num of tasks.")
	taskConfigFile := flag.String("task_config", "", "Path to task config json file.")

	flag.Parse()

	if *jobName == "" {
		log.Fatal("Job name is required.")
	}

	crd, oErr := filesystem.NewLocalFSClient().OpenReadCloser(*taskConfigFile)
	if oErr != nil {
		log.Fatalf("Failed opening task config file. %s", oErr)
	}
	confData, rdErr := ioutil.ReadAll(crd)
	if rdErr != nil {
		log.Fatalf("Failed reading task config. %s", rdErr)
	}
	log.Printf("conf data: %s", confData)

	if *etcdUrlList == "" {
		log.Fatal("Please specify the etcd server urls.")
	}
	etcdUrls := strings.Split(*etcdUrlList, ",")
	log.Println("etcd urls: ", etcdUrls)

	topoParent := topo.NewFullTopologyOfMaster(uint64(*numTasks))
	topoChildren := topo.NewFullTopologyOfNeighbor(uint64(*numTasks))

	switch *jobType {
	case "t":
		bootstrap := framework.NewBootStrap(*jobName, etcdUrls, createListener(), nil)
		taskBuilder := &bwmf.BWMFTaskBuilder{
			NumOfTasks: uint64(*numTasks),
			ConfBytes:  confData,
		}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.AddLinkage("Parents", topoParent)
		bootstrap.AddLinkage("Children", topoChildren)
		log.Println("Starting task..")
		bootstrap.Start()
	case "c":
		controller := controller.New(*jobName, etcd.NewClient(etcdUrls), uint64(*numTasks), topo.GetLinkTypes())
		controller.Start()
		log.Println("Controller started.")
		controller.WaitForJobDone()
		controller.Stop()
	default:
		log.Fatal("Please choose a type via '-jobtype': (c) controller, (t) task")
	}
}

func createListener() net.Listener {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("os.Hostname(\"tcp4\", \"\") failed: %v", err)
	}
	l, err := net.Listen("tcp4", hostname+":0")
	if err != nil {
		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	log.Printf("Service listens to %s.", l.Addr().String())
	return l
}
