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
	// "fmt"

	"github.com/coreos/go-etcd/etcd"
	"../../controller"
	"../../mapreduce"
	"../../framework"
	"../../../taskgraph"
	"../../filesystem"
)
// azureAccountName string, 
// 		azureAccountKey string, 
// 		outputContainerName string, 
// 		outputBlobName string,

func mapperFunc(framework taskgraph.Framework, text string) {
	textReader := bufio.NewReader(strings.NewReader(text))
	var err error
	var str string
	for err != io.EOF {
		str, err = textReader.ReadString(' ')
		if err != io.EOF && err != nil {
			return
		}
		if err != io.EOF {
			str = str[:len(str) - 1]
		}
		framework.Emit(str, "1")
	}
}

func reducerFunc(framework taskgraph.Framework, key string, val []string) {
	lenVal := len(val)
	var sum int
	for v := 0; v < lenVal; v++ {
		a, err := strconv.Atoi(val[v])
		if err != nil {
			continue
		}
		sum = sum + a
	}
	framework.Collect(key, strconv.Itoa(sum))
}

var mpFiles []string

func main() {
	programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) reducer")
	job := flag.String("job", "", "job name")
	mapperNum := flag.Int("mapperNum", 3, "mapperNum")
	shuffleNum := flag.Int("shuffleNum", 5, "shuffleNum")
	reducerNum := flag.Int("reducerNum", 2, "reducerNum")
	azureAccountName := flag.String("azureAccountName", "spluto", "azureAccountName")
	azureAccountKey := flag.String("azureAccountKey", "aaa", "azureAccountKey")
	outputContainerName := flag.String("outputContainerName", "defaultoutputpathformapreducep12", "outputContainerName")
	outputBlobName := flag.String("outputBlobName", "result1.txt", "outputBlobName")
	// inputFileSource := flag.String("inputFileName", "input1.txt", "mapperInputFileName")
	var q []map[string][]string
	// /q = make(map[string][]string)
	for inputM := 1; inputM <= *mapperNum; inputM++ {
		inputFileSource := "input" + strconv.Itoa(inputM) + ".txt"
		tmpFileReader, err := os.Open(inputFileSource)
		mpFiles = mpFiles[:0]
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
			if err != io.EOF {
				str = str[:len(str) - 1]
			}
			mpFiles = append(mpFiles, str)
		}
		q = append(q, map[string][]string{ "files" : mpFiles})
	}
	var ll *log.Logger
	ll = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	etcdURLs := []string{"http://localhost:4001"} 
	// fmt.Println(*azureAccountKey)
	// fmt.Println(mpFiles[1])
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
	if (err != nil) {
		log.Fatalf("%v", err)
	}
	
	
	switch *programType {
	case "c":
		log.Printf("controller")
		ntask := *mapperNum + *shuffleNum + *reducerNum + 1
		controller := controller.New(*job, etcd.NewClient(etcdURLs), uint64(ntask), []string{"Prefix", "Suffix"})
		controller.Start()
		controller.WaitForJobDone()
	case "t":
		log.Printf("task")
		bootstrap := framework.NewBootStrap(*job, etcdURLs, createListener(), ll)
		taskBuilder := &mapreduce.MapreduceTaskBuilder{
			MapperConfig: q, 
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
			azureClient,
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

