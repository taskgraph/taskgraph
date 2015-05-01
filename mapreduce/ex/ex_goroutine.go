package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"../../../taskgraph"
	"../../filesystem"
	"../../mapreduce"
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

// Input files defined in "input($mapperTaskID).txt"
func main() {
	// programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) reducer")
	job := flag.String("job", "mapredectest", "job name")
	mapperNum := flag.Int("mapperNum", 3, "mapperNum")
	shuffleNum := flag.Int("shuffleNum", 2, "shuffleNum")
	reducerNum := flag.Int("reducerNum", 5, "reducerNum")
	azureAccountName := flag.String("azureAccountName", "spluto", "azureAccountName")
	azureAccountKey := flag.String("azureAccountKey", "+aGFki7uhMxio0m0vdTaMb2D26Qgfj0E/hXxnuVseyD6jFXoqx3Pre4akPL02I15lHpMi7o3tXVWHuzMcdyqIQ==", "azureAccountKey")
	outputDir := flag.String("outputContainerName", "defaultoutputpathformapreduce003", "outputContainerName")

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
	fmt.Printf("%s %s %v", *azureAccountName, *azureAccountKey, err)
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

	etcdURLs := []string{"http://localhost:4001"}
	mapreduceConfig := taskgraph.MapreduceConfig{
		MapperNum:        uint64(*mapperNum),
		ShuffleNum:       uint64(*shuffleNum),
		ReducerNum:       uint64(*reducerNum),
		MapperFunc:       mapperFunc,
		ReducerFunc:      reducerFunc,
		UserDefined:      true,
		AppName:          *job,
		InterDir:         "",
		OutputDir:        *outputDir,
		EtcdURLs:         etcdURLs,
		FilesystemClient: azureClient,
		WorkDir:          map[string][]taskgraph.Work{"mapper": works},
	}

	c := mapreduce.NewMapreduceController()
	c.Start(mapreduceConfig)
}

// func createListener() net.Listener {
// 	l, err := net.Listen("tcp4", "127.0.0.1:0")
// 	if err != nil {
// 		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
// 	}
// 	return l
// }
