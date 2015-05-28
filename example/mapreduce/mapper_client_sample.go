package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	pb "./mapper_proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	address     = "localhost"
	defaultName = "world"
)

var (
	port = flag.Int("port", 10000, "The server port")
)

func main() {
	// Set up a connection to the server.
	flag.Parse()
	conn, err := grpc.Dial(address + fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMapperClient(conn)

	// Contact the server and print out its response.
	fmt.Println("link server ", (address + fmt.Sprintf(":%d", *port)))
	stream, err := c.GetEmitResult(context.Background(), &pb.MapperRequest{Key: "a b c d e f g", Value: ""})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
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

	_, err = c.GetEmitResult(context.Background(), &pb.MapperRequest{Key: "", Value: "Stop"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

}
