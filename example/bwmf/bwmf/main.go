package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/controller"
	"github.com/taskgraph/taskgraph/example/bwmf"
	"github.com/taskgraph/taskgraph/example/topo"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph/framework"
)

func main() {

	etcdUrlList := flag.String("etcd_urls", "", "ETCD server lists, sep by a comma.")
	jobName := flag.String("job_name", "bwmf", "Job name in etcd path.")
	jobType := flag.String("job_type", "c", "Job type, either 'c', 't' or 'd'")
	pbResPath := flag.String("pb_shard_path", "./", "Path to output pb buf. For 'd' job only.")
	numTasks := flag.Int("num_tasks", 1, "Num of tasks.")
	numIters := flag.Int("num_iters", 10, "Num of iters for matrix factorization.")
	latentDim := flag.Int("latent_dim", 100, "Dimensions of latent factors.")
	taskConfigFile := flag.String("task_config", "", "Path to task config json file.")

	flag.Parse()

	if *jobType != "c" && *jobType != "t" && *jobType != "d" {
		log.Fatalf("Invalid job type: %s. Please set it as one of c/t/d.", *jobType)
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
	if *jobType == "d" {
		log.Println("Trying to convert the mat to protobuf and save it to ", *pbResPath)
		ConvertMatrixTxtToPb(confData, *pbResPath)
		return
	}

	if *jobName == "" {
		log.Fatal("Job name is required.")
	}

	if *etcdUrlList == "" {
		log.Fatal("Please specify the etcd server urls.")
	}
	etcdUrls := strings.Split(*etcdUrlList, ",")
	log.Println("etcd urls: ", etcdUrls)

	topo := topo.NewFullTopology(uint64(*numTasks))

	switch *jobType {
	case "t":
		bootstrap := framework.NewBootStrap(*jobName, etcdUrls, createListener(), nil)
		taskBuilder := &bwmf.BWMFTaskBuilder{
			NumOfTasks: uint64(*numTasks),
			NumIters:   uint64(*numIters),
			ConfBytes:  confData,
			LatentDim:  *latentDim,
		}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.SetTopology(topo)
		log.Println("Starting task..")
		bootstrap.Start()
	case "c":
		controller := controller.New(*jobName, etcd.NewClient(etcdUrls), uint64(*numTasks), topo.GetLinkTypes())
		controller.Start()
		log.Println("Controller started.")
		controller.WaitForJobDone()
		controller.Stop()
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
