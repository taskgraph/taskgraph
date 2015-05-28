package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	pb "./mapper_proto"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 10000, "The server port")
)

var s *grpc.Server

type server struct{}

func (*server) GetEmitResult(KvPair *pb.MapperRequest, stream pb.Mapper_GetEmitResultServer) error {
	fmt.Println("in Emit Function")
	if KvPair.Value == "Stop" {
		// server.Stop()
		s.Stop()
		fmt.Println("Stop")
		return nil
	}
	chop := strings.Split(KvPair.Key, " ")
	for i := range chop {
		res := &pb.MapperResponse{
			Key:   chop[i],
			Value: "1",
		}
		fmt.Println(chop[i])
		if err := stream.Send(res); err != nil {
			return err
		}
	}
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
	pb.RegisterMapperServer(s, &server{})
	s.Serve(lis)
}
