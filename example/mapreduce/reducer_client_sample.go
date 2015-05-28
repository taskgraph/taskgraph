package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	pb "./proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	address = "localhost"
)

var (
	port = flag.Int("port", 10000, "The server port")
	c    pb.ReducerClient
)

func testCollect(key string, value []string, stop bool) {
	r, err := c.GetCollectResult(context.Background(), &pb.ReducerRequest{Key: key, Value: value})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	if !stop {
		for {
			feature, err := r.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("%v.ListFeatures(_) = _, %v", c, err)
				return
			}
			fmt.Println(feature)
		}
	}
}

func main() {
	// Set up a connection to the server.
	flag.Parse()
	conn, err := grpc.Dial(address + fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c = pb.NewReducerClient(conn)

	// Contact the server and print out its response.
	fmt.Println("link server ", (address + fmt.Sprintf(":%d", *port)))

	testCollect("key1", []string{"1", "10", "100", "19"}, false)
	testCollect("key2", []string{"11", "30", "9000", "19"}, false)
	testCollect("key3", []string{"100"}, false)
	testCollect("Stop", []string{}, true)

}
