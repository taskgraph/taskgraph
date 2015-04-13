package main 

import (
	"flag"
	"log"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/plutoshe/taskgraph/controller"
	"github.com/plutoshe/taskgraph/mapreduce"
	"github.com/plutoshe/taskgraph/example/topo"
	"github.com/plutoshe/taskgraph/framework"
)

func main() {
	programType := flag.String("type", "", "(c) controller, (m) mapper, (s) shuffle, or (r) reducer")
	job := flag.String("job", "", "job name")
	mapNum := flag.String("mapNum", "")
	etcdURLs := []string{"http://localhost:4001"} 
	flag.Parse()
	if *job = "" {
		log.Fatalf("Please specify a job name")
	}

	switch 
}
