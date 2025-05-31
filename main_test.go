package main

import (
	"net"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	time.Sleep(1 * time.Second)

	conn, err := net.Dial("tcp", port)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	message := "test message"
	_, err = conn.Write([]byte(message))
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	buffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	res := string(buffer[:n])
	if res != "message received" {
		t.Fatalf("unexpected response: %s", res)
	}
}
