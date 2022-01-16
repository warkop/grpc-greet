package main

import (
	"context"
	"fmt"
	"grpc-greet/greet/greet_client/grpc_client"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	fmt.Println("Hello I'm a client")
	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}

	defer cc.Close()

	c := grpc_client.NewGreetServiceClient(cc)

	// doUnary(c)
	// doServerStreaming(c)
	// doClientStreaming(c)
	// doBiDiStreaming(c)
	doUnaryWithDeadline(c, 5*time.Second)
	doUnaryWithDeadline(c, 1*time.Second)
}

func doUnary(c grpc_client.GreetServiceClient) {
	fmt.Println("Starting to do a Unary RPC...")
	req := &grpc_client.GreetRequest{
		Greeting: &grpc_client.Greeting{
			FirstName: "Stephane",
			LastName:  "Maarek",
		},
	}

	res, err := c.Greet(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Greet RPC: %v", err)
	}
	log.Printf("Response from Greet: %v", res.Result)
}

func doServerStreaming(c grpc_client.GreetServiceClient) {
	fmt.Println("Starting to do server streaming RPC...")
	req := &grpc_client.GreetManyTimesRequest{
		Greeting: &grpc_client.Greeting{
			FirstName: "Stephane",
			LastName:  "Maarek",
		},
	}
	resStream, err := c.GreetManyTimes(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling GreetManyTimes RPC: %v", err)
	}
	for {
		msg, err := resStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}

		if err != nil {
			log.Fatalf("error while reading stream: %v", err)
		}

		log.Printf("response from GreetManyTimes: %v", msg.Result)
	}
}

func doClientStreaming(c grpc_client.GreetServiceClient) {
	fmt.Println("Starting to do client streaming RPC...")

	requests := []*grpc_client.LongGreetRequest{
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "Roger",
			},
		},
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "John",
			},
		},
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "Dimitri",
			},
		},
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "Siska",
			},
		},
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "Yuri",
			},
		},
	}

	stream, err := c.LongGreet(context.Background())
	if err != nil {
		log.Fatalf("error while calling LongGreet: %v", err)
	}

	for _, req := range requests {
		fmt.Printf("Sending req: %v \n", req)
		stream.Send(req)
		time.Sleep(1000 * time.Millisecond)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error while receiving response from LongGreet: %v", err)
	}

	fmt.Printf("LongGreet Response: %v", res)
}

func doBiDiStreaming(c grpc_client.GreetServiceClient) {
	fmt.Println("Starting to do BiDi streaming RPC...")

	// we create a stream by invoking the client
	stream, err := c.GreetEveryone(context.Background())
	if err != nil {
		log.Fatalf("Error while creating stream: %v", err)
		return
	}

	requests := []*grpc_client.GreetEveryoneRequest{
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "Roger",
			},
		},
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "John",
			},
		},
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "Dimitri",
			},
		},
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "Siska",
			},
		},
		{
			Greeting: &grpc_client.Greeting{
				FirstName: "Yuri",
			},
		},
	}

	waitc := make(chan struct{})
	//we send a bunch of messages from the client (go routine)
	go func() {
		for _, req := range requests {
			fmt.Printf("Sending message: %v \n", req)
			stream.Send(req)
			time.Sleep(1000 * time.Millisecond)
		}

		stream.CloseSend()
	}()

	// we receive a bunc of message from the server (go routine)
	go func() {

		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				log.Fatalf("Error while receiving: %v \n", err)
				break
			}
			fmt.Printf("Received: %v \n", res.GetResult())
		}
		close(waitc)
	}()

	// block until everything is done
	<-waitc
}

func doUnaryWithDeadline(c grpc_client.GreetServiceClient, timeout time.Duration) {
	fmt.Println("Starting to do a UnaryWithDeadline RPC...")
	req := &grpc_client.GreetWithDeadlineRequest{
		Greeting: &grpc_client.Greeting{
			FirstName: "Stephane",
			LastName:  "Maarek",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := c.GreetWithDeadline(ctx, req)
	if err != nil {
		statusErr, ok := status.FromError(err)
		if ok {
			if statusErr.Code() == codes.DeadlineExceeded {
				fmt.Println("Timeout was hit! Deadline was exceeded")
			} else {
				fmt.Printf("unexpected error: %v", statusErr)
			}
		} else {
			log.Fatalf("error while calling GreetWithDeadline RPC: %v", err)
		}
		return
	}
	log.Printf("Response from GreetWithDeadline: %v", res.Result)
}
