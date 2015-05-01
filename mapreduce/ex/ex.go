package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/controller"

	"../../../taskgraph"
	"../../filesystem"
	"../../framework"
	"../../mapreduce"
	"../pkg/etcdutil"
)

func mapperFunc(mp taskgraph.MapreduceTask, text string) {
	textReader := bufio.NewReader(strings.NewReader(text))
	var err error
	var str string
	for err != io.EOF {
		str, err = textReader.ReadString(' ')
		if err != io.EOF && err != nil {
			return
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		mp.Emit(str, "1")
	}
}

func reducerFunc(mp taskgraph.MapreduceTask, key string, val []string) {
	lenVal := len(val)
	var sum int
	for v := 0; v < lenVal; v++ {
		a, err := strconv.Atoi(val[v])
		if err != nil {
			continue
		}
		sum = sum + a
	}
	mp.Collect(key, strconv.Itoa(sum))
}

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// Input files defined in "input($mapperTaskID).txt"
func main() {
	programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) reducer")
	job := flag.String("job", "", "job name")
	mapperNum := flag.Int("mapperNum", 1, "mapperNum")
	shuffleNum := flag.Int("shuffleNum", 1, "shuffleNum")
	reducerNum := flag.Int("reducerNum", 1, "reducerNum")
	azureAccountName := flag.String("azureAccountName", "spluto", "azureAccountName")
	azureAccountKey := flag.String("azureAccountKey", "", "azureAccountKey")
	outputDir := flag.String("outputDir", "defaultoutputpathformapreduce003", "outputDir")

	flag.Parse()
	if *job == "" {
		log.Fatalf("Please specify a job name")
	}
	if *azureAccountKey == "" {
		log.Fatalf("Please specify azureAccountKey")
	}

	azureClient, err := filesystem.NewAzureClient(
		*azureAccountName,
		*azureAccountKey,
		"core.chinacloudapi.cn",
		"2014-02-14",
		true,
	)
	if err != nil {
		log.Fatalf("%v", err)
	}

	works := make([]taskgraph.Work, 0)
	for inputM := 1; inputM <= 5; inputM++ {
		inputFile := "testforcomplexmapreduceframework/textForExamination" + strconv.Itoa(inputM)
		newWork := taskgraph.Work{
			Config: map[string]string{"inputFile": inputFile},
		}
		works = append(works, newWork)
	}
	var ll *log.Logger
	ll = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	etcdURLs := []string{"http://localhost:4001"}
	mapreduceConfig := taskgraph.MapreduceConfig{
		MapperNum:        uint64(*mapperNum),
		ShuffleNum:       uint64(*shuffleNum),
		ReducerNum:       uint64(*reducerNum),
		MapperFunc:       mapperFunc,
		ReducerFunc:      reducerFunc,
		UserDefined:      true,
		AppName:          *job,
		InterDir:         "mapreducerprocesstemporaryresult",
		OutputDir:        *outputDir,
		EtcdURLs:         etcdURLs,
		FilesystemClient: azureClient,
		WorkDir:          map[string][]taskgraph.Work{"mapper": works},
	}
	ntask := max(mapreduceConfig.MapperNum+mapreduceConfig.ShuffleNum, mapreduceConfig.ShuffleNum+mapreduceConfig.ReducerNum) + 1
	switch *programType {
	case "c":
		log.Printf("controller")

		etcdClient := etcd.NewClient(mapreduceConfig.EtcdURLs)
		for i := 0; i < len(mapreduceConfig.WorkDir["mapper"]); i++ {
			etcdutil.MustCreate(etcdClient, ll, etcdutil.FreeWorkPathForType(mapreduceConfig.AppName, "mapper", strconv.Itoa(i)), "", 0)

		}
		for i := uint64(0); i < mapreduceConfig.ReducerNum; i++ {
			etcdutil.MustCreate(etcdClient, ll, etcdutil.FreeWorkPathForType(mapreduceConfig.AppName, "shuffle", strconv.FormatUint(i, 10)), "", 0)
		}
		etcdClient.CreateDir(etcdutil.FreeWorkDirForType(mapreduceConfig.AppName, "reducer"), 0)
		controller := controller.New(mapreduceConfig.AppName, etcd.NewClient(mapreduceConfig.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
		controller.Start()
		controller.WaitForJobDone()

	case "t":
		log.Printf("task")
		bootstrap := framework.NewMapreduceBootStrap(mapreduceConfig.AppName, mapreduceConfig.EtcdURLs, createListener(), ll)
		taskBuilder := &mapreduce.MapreduceTaskBuilder{}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.SetTopology(mapreduce.NewMapReduceTopology(mapreduceConfig.MapperNum, mapreduceConfig.ShuffleNum, mapreduceConfig.ReducerNum))
		bootstrap.InitWithMapreduceConfig(mapreduceConfig)
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
