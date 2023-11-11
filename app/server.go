package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/timwee/redis/resp"
	// "os"
)

const (
	PING        = "*1\r\n$4\r\nping\r\n"
	ECHO_PREFIX = "*2\r\n$4\r\nECHO\r\n$"
	SEP         = "\r\n"
	PONG        = "+PONG\r\n"
)

type Server struct {
	store map[string]*resp.Value
	mu    sync.RWMutex
}

// NewServer returns a new Server.
func NewServer() *Server {
	return &Server{
		store: make(map[string]*resp.Value),
	}
}

func (s *Server) Set(k string, v *resp.Value) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[k] = v
}

func (s *Server) Get(k string) *resp.Value {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if v, ok := s.store[k]; ok {
		return v
	}
	return nil
}

// Conn represents a RESP network connection.
type Conn struct {
	*resp.Reader
	*resp.Writer
	base       net.Conn
	RemoteAddr string
}

// NewConn returns a Conn.
func NewConn(conn net.Conn) *Conn {
	return &Conn{
		Reader:     resp.NewReader(conn),
		Writer:     resp.NewWriter(conn),
		base:       conn,
		RemoteAddr: conn.RemoteAddr().String(),
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	s := NewServer()
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
		go s.HandleConnection(NewConn(conn))
	}
}

func (s *Server) HandleConnection(conn *Conn) {
	defer conn.base.Close()
	for {
		// var buf bytes.Buffer
		// tmp := make([]byte, 1024)
		// n, err := conn.Read(tmp)
		// if err != nil {
		// 	if err != io.EOF {
		// 		fmt.Println(err)
		// 		return
		// 	}
		// }
		// buf.Write(tmp[:n])

		// respond(buf, conn)

		v, _, _, err := conn.ReadMultiBulk()
		if err != nil {
			fmt.Println(err)
			return
		}
		values := v.Array()
		if len(values) == 0 {
			continue
		}
		lccommandName := values[0].String()
		commandName := strings.ToUpper(lccommandName)
		fmt.Println(commandName)
		switch commandName {
		case "QUIT":
			conn.WriteSimpleString("OK")
			return
		case "PING":
			if err := conn.WriteSimpleString("PONG"); err != nil {
				fmt.Println(err)
			}
			continue
		case "ECHO":
			if err := conn.WriteString(values[1].String()); err != nil {
				fmt.Println(err)
			}
			continue
		case "SET":
			if len(values) != 3 {
				fmt.Println(fmt.Errorf("expecting three params for SET, instead got %v", v.String()))
				continue
			}
			fmt.Printf("writing into map for key %s: %s\n", values[1].String(), values[2].String())
			s.Set(values[1].String(), &values[2])
			conn.WriteSimpleString("OK")
			continue
		case "GET":
			if len(values) != 2 {
				fmt.Println(fmt.Errorf("expecting 2 params for GET, instead got %v", v.String()))
				continue
			}
			fmt.Printf("retrieving from map for key %s\n", values[1].String())
			if v, ok := s.store[values[1].String()]; ok {
				fmt.Printf("Found in map for key %s, value: %s\n", values[1].String(), v.String())
				conn.WriteBytes(v.Bytes())
			}
			continue
		}
	}

}

// func respond(buf bytes.Buffer, conn net.Conn) {
// 	toks := strings.Split(buf.String(), SEP)
// 	if isPing(toks) {
// 		conn.Write([]byte(PONG))
// 	} else if isEcho(toks) {
// 		fmt.Printf("in echo, echoing back %s", toks[4])
// 		_, err := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(toks[4]), toks[4])
// 		if err != nil {
// 			fmt.Println(err)
// 			return
// 		}
// 	} else {
// 		// fmt.Printf("unsupported command: %s", buf.String())
// 	}
// }

// func isEcho(toks []string) bool {
// 	return len(toks) >= 5 && strings.Compare(toks[2], "echo") == 0
// }

// func isPing(toks []string) bool {
// 	return len(toks) >= 3 && strings.Compare(toks[2], "ping") == 0
// }
