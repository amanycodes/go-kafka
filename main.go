package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	port = ":9092"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		startServer(ctx)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	fmt.Println("signal received: ", sig)

	cancel()

	wg.Wait()
}

func startServer(ctx context.Context) {
	l, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("error listening: ", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	var connWg sync.WaitGroup

	go func() {
		<-ctx.Done()
		fmt.Println("shutting down server")
		l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				fmt.Println("server shutdown complete")
				return
			default:
				fmt.Println("error accepting: ", err.Error())
				continue
			}
		}

		connWg.Add(1)
		go func() {
			defer connWg.Done()

			handleConnection(ctx, conn)
		}()
	}
}

func handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	fmt.Println("client connected: ", conn.RemoteAddr())

	buffer := make([]byte, 1024)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("connnection closed due to shutdown")
			return
		default:
			conn.SetReadDeadline(time.Now().Add(1 * time.Second))

			n, err := conn.Read(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				fmt.Println("error reading: ", err.Error())
				return
			}
			fmt.Printf("received: %s\n", string(buffer[:n]))

			_, err = conn.Write([]byte("message received"))
			if err != nil {
				fmt.Println("error writing: ", err.Error())
				return
			}
		}
	}
}
