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
	address     = "localhost"
	defaultName = "world"
)

var (
	port = flag.Int("port", 10000, "The server port")
	c    pb.MapperClient
)

func testEmit(key string, value string, stop bool) {
	stream, err := c.GetEmitResult(context.Background(), &pb.MapperRequest{Key: key, Value: value})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	if !stop {
		fmt.Println("in Emit steps")
		for {
			feature, err := stream.Recv()
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
	c = pb.NewMapperClient(conn)

	// Contact the server and print out its response.
	fmt.Println("link server ", (address + fmt.Sprintf(":%d", *port)))

	testEmit("a b c d e f g", "", false)
	testEmit("a a c dsdf e f gww", "", false)
	testEmit("Stop", "Stop", true)

}
