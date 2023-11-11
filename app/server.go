package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	// "os"
)

const (
	PING        = "*1\r\n$4\r\nping\r\n"
	ECHO_PREFIX = "*2\r\n$4\r\nECHO\r\n$"
	SEP         = "\r\n"
	PONG        = "+PONG\r\n"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		var buf bytes.Buffer
		tmp := make([]byte, 1024)
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
				return
			}
		}
		buf.Write(tmp[:n])

		respond(buf, conn)
	}

}

func respond(buf bytes.Buffer, conn net.Conn) {
	toks := strings.Split(buf.String(), SEP)
	if isPing(toks) {
		conn.Write([]byte(PONG))
	} else if isEcho(toks) {
		fmt.Printf("in echo, echoing back %s", toks[4])
		_, err := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(toks[4]), toks[4])
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		// fmt.Printf("unsupported command: %s", buf.String())
	}
}

func isEcho(toks []string) bool {
	return len(toks) >= 5 && strings.Compare(toks[2], "echo") == 0
}

func isPing(toks []string) bool {
	return len(toks) >= 3 && strings.Compare(toks[2], "ping") == 0
}
