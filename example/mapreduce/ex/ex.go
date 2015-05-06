package main

// an example to start a mapreduce framework

import (
	"bufio"
	"flag"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/filesystem"
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
	job := flag.String("job", "", "job name")
	mapperNum := flag.Int("mapperNum", 3, "mapperNum")
	shuffleNum := flag.Int("shuffleNum", 2, "shuffleNum")
	reducerNum := flag.Int("reducerNum", 5, "reducerNum")
	azureAccountName := flag.String("azureAccountName", "spluto", "azureAccountName")
	azureAccountKey := flag.String("azureAccountKey", "", "azureAccountKey")
	outputDir := flag.String("outputDir", "0newmapreducepathformapreduce000", "outputDir")
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

	iconfig := map[string]interface{}{
		"MapperNum":        uint64(*mapperNum),
		"ShuffleNum":       uint64(*shuffleNum),
		"ReducerNum":       uint64(*reducerNum),
		"MapperFunc":       mapperFunc,
		"ReducerFunc":      reducerFunc,
		"UserDefined":      true,
		"AppName":          *job,
		"InterDir":         "mapreducerprocesstemporaryresult",
		"OutputDir":        *outputDir,
		"EtcdURLs":         etcdURLs,
		"FilesystemClient": azureClient,
		"WorkDir":          map[string][]taskgraph.Work{"mapper": works},
		"ReaderBufferSize": 4096,
		"WriterBufferSize": 4096,
	}
	boot := mapreduce.NewMapreduceBootstrapController()
	err = boot.Start(iconfig)
	if err != nil {
		log.Fatalf("%v", err)
	}
}
