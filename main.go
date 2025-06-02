package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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

const (
	ProduceReqKey  int16 = 0
	FetchReqKey    int16 = 1
	MetadataReqKey int16 = 3
)

type RequestHeader struct {
	Size          int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
}

type ResponseHeader struct {
	CorrelationId int32
}

func readInt32(r io.Reader) (int32, error) {
	var val int32
	err := binary.Read(r, binary.BigEndian, &val)
	return val, err
}

func readInt16(r io.Reader) (int16, error) {
	var val int16
	err := binary.Read(r, binary.BigEndian, &val)
	return val, err
}

func parseRequestHeader(r io.Reader) (*RequestHeader, error) {
	var header RequestHeader

	val16, err := readInt16(r)
	if err != nil {
		return nil, fmt.Errorf("error reading api key: %w", err)
	}
	header.ApiKey = val16

	val16, err = readInt16(r)
	if err != nil {
		return nil, fmt.Errorf("error reading api version: %w", err)
	}
	header.ApiVersion = val16

	val32, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("error reading correlation id: %w", err)
	}
	header.CorrelationId = val32

	return &header, nil
}

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
	fmt.Println("broker stopped gracefully.")
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
				if opErr, ok := err.(*net.OpError); ok && opErr.Op == "accept" {
					fmt.Println("listener closed, stopping accept loop")
					return
				}
				fmt.Println("error accepting: ", err.Error())
				continue
			}
		}

		connWg.Add(1)
		go func() {
			defer connWg.Done()

			handleConnection(conn)
		}()
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("client connected: ", conn.RemoteAddr())

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	var size int32
	err := binary.Read(conn, binary.BigEndian, &size)
	if err != nil {
		if err == io.EOF {
			fmt.Println("client disconnected before sending size.", conn.RemoteAddr())
		} else {
			fmt.Println("error reading message size: ", err, conn.RemoteAddr())
		}
	}

	fmt.Printf("Received: %s: Received message size: %d bytes \n", conn.RemoteAddr(), size)

	lr := io.LimitedReader{R: conn, N: int64(size)}

	header, err := parseRequestHeader(&lr)
	if err != nil {
		if err == io.EOF && lr.N > 0 {
			fmt.Println("client disconnected while reading header", conn.RemoteAddr())
		} else if err == io.EOF {
			fmt.Println("Client sent data matching header size, but potentially incomplete or just a header.", conn.RemoteAddr())
		} else {
			fmt.Println("error parsing request header: ", err, conn.RemoteAddr())
		}
		return
	}
	header.Size = size

	fmt.Printf("client: %s parsed header: %+v\n", conn.RemoteAddr(), header)

	fmt.Printf("Client %s: Remaining bytes for payload: %d\n", conn.RemoteAddr(), lr.N)

	if lr.N > 0 {
		remaining := make([]byte, lr.N)
		_, err = io.ReadFull(&lr, remaining)
		if err != nil {
			fmt.Printf("client: %s error reading remaining payload: %v\n", conn.RemoteAddr(), err)
			return
		}
		fmt.Printf("Client %s: Consumed %d bytes of payload.\n", conn.RemoteAddr(), len(remaining))
	}
	fmt.Printf("Client %s: Request processing finished (no response sent yet).\n", conn.RemoteAddr())
}
