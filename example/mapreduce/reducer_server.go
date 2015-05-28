package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"

	pb "./reducer_proto"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 10000, "The server port")
	s    *grpc.Server
)

type server struct{}

func (*server) GetCollectResult(KvPair *pb.ReducerRequest, stream pb.Reducer_GetCollectResultServer) error {
	fmt.Println("===in Collect Function")
	if KvPair.Key == "Stop" && len(KvPair.Value) == 0 {
		// server.Stop()
		s.Stop()
		fmt.Println("Stop")
		return nil
	}
	count := 0
	for i := range KvPair.Value {
		v, err := strconv.Atoi(KvPair.Value[i])
		if err == nil {
			count += v
		}
	}
	stream.Send(&pb.ReducerResponse{Key: KvPair.Key, Value: strconv.Itoa(count)})

	return nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Println("Listening...")
	s = grpc.NewServer()
	pb.RegisterReducerServer(s, &server{})
	s.Serve(lis)
}
