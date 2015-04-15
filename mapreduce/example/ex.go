package main 

import (
	"flag"
	"log"
	"strconv"
	"strings"
	"io"
	"os"
	"bufio"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"../../controller"
	"../../mapreduce"
	"../../framework"
	"../../../taskgraph"
)

func mapperFunc(framework taskgraph.Framework, text string) {
	textReader := bufio.NewReader(strings.NewReader(text))
	var err error
	var str string
	for err != io.EOF {
		str, err = textReader.ReadString(' ')
		if err != nil {
			return
		}
		framework.Emit(str, "1")
	}
}

func reducerFunc(framework taskgraph.Framework, key string, val []string) {
	lenVal := len(val)
	sum := 0
	for v := 0; v < lenVal; v++ {
		a, err := strconv.Atoi(val[v])
		if err != nil {
			continue
		}
		sum = sum + a
	}
	framework.Collect(key, string(sum))
}

var mpFiles []string

func main() {
	programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) reducer")
	job := flag.String("job", "", "job name")
	mapperNum := flag.Int("mapperNum", 1, "mapperNum")
	shuffleNum := flag.Int("shuffleNum", 1, "shuffleNum")
	reducerNum := flag.Int("reducerNum", 1, "reducerNum")
	azureAccountName := flag.String("azureAccountName", "spluto", "azureAccountName")
	azureAccountKey := flag.String("azureAccountKey", "", "azureAccountKey")
	outputContainerName := flag.String("outputContainerName", "defaultoutputpathformapreducepro", "outputContainerName")
	outputBlobName := flag.String("outputBlobName", "result.txt", "outputBlobName")
	inputFileSource := flag.String("inputFileName", "input1.txt", "mapperInputFileName")
	tmpFileReader, err := os.Open(*inputFileSource)
	if err != nil {
		log.Fatalf("open file error ", err)
	}
	fileRead := bufio.NewReader(tmpFileReader)
	err = nil
	var str string
	for err != io.EOF {
		str, err = fileRead.ReadString('\n')
		if err != io.EOF && err != nil {
			log.Fatal("read error ", err)
		}
		mpFiles = append(mpFiles, str)
	}
	
	etcdURLs := []string{"http://localhost:4001"} 
	
	flag.Parse()
	if *job == "" {
		log.Fatalf("Please specify a job name")
	}
	if *azureAccountKey == "" {
		log.Fatalf("Please specify azureAccountKey")
	}
	switch *programType {
	case "c":
		log.Printf("controller")
		ntask := *mapperNum + *shuffleNum + *reducerNum
		controller := controller.New(*job, etcd.NewClient(etcdURLs), uint64(ntask), []string{"Prefix", "Suffix"})
		controller.Start()
		controller.WaitForJobDone()
	case "t":
		log.Printf("task")
		bootstrap := framework.NewBootStrap(*job, etcdURLs, createListener(), nil)
		taskBuilder := &mapreduce.MapreduceTaskBuilder{
			MapperConfig:       map[string][]string{ "files" : mpFiles},
			MapperNum : uint64(*mapperNum),
			ShuffleNum : uint64(*shuffleNum),
			ReducerNum : uint64(*reducerNum),
		}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.SetTopology(mapreduce.NewMapReduceTopology(uint64(*mapperNum), uint64(*shuffleNum), uint64(*reducerNum)))
		bootstrap.InitWithMapreduceConfig(
			uint64(*mapperNum), 
			uint64(*shuffleNum), 
			uint64(*reducerNum), 
			*azureAccountName,
			*azureAccountKey,
			*outputContainerName,
			*outputBlobName,
			mapperFunc,
			reducerFunc,
		)
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

