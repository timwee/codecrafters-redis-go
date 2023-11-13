package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/timwee/redis/resp"
	// "os"
)

const (
	PING         = "*1\r\n$4\r\nping\r\n"
	ECHO_PREFIX  = "*2\r\n$4\r\nECHO\r\n$"
	SEP          = "\r\n"
	PONG         = "+PONG\r\n"
	MAX_DURATION = 1<<63 - 1
)

type ExpirableEntry struct {
	Ttl       time.Duration
	AddedTime time.Time
	Val       *resp.Value
}

type Server struct {
	expirableStore map[string]*ExpirableEntry
	emu            sync.RWMutex
}

// NewServer returns a new Server.
func NewServer() *Server {
	return &Server{
		expirableStore: make(map[string]*ExpirableEntry),
	}
}

func (s *Server) ESet(k string, v *resp.Value, ttl time.Duration) {
	s.emu.Lock()
	defer s.emu.Unlock()

	s.expirableStore[k] = &ExpirableEntry{
		Ttl:       ttl,
		Val:       v,
		AddedTime: time.Now(),
	}
}

func (s *Server) EGet(k string) *resp.Value {
	s.emu.RLock()
	defer s.emu.RUnlock()

	if ev, ok := s.expirableStore[k]; ok {
		past := time.Since(ev.AddedTime)
		if past < ev.Ttl {
			return ev.Val
		}
		delete(s.expirableStore, k)
		return nil
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
			if len(values) != 3 && len(values) != 5 {
				fmt.Println(fmt.Errorf("expecting three or 5 params for SET, instead got %v", v.String()))
				continue
			}
			k := values[1].String()
			val := &values[2]
			fmt.Printf("writing into map for key %s: %s\n", k, val.String())
			var ttl time.Duration = MAX_DURATION
			if len(values) == 5 && strings.ToUpper(values[3].String()) == "PX" {
				ttl = time.Duration(values[4].Integer()) * time.Millisecond
			}
			s.ESet(values[1].String(), &values[2], ttl)
			conn.WriteSimpleString("OK")
			continue
		case "GET":
			if len(values) != 2 {
				fmt.Println(fmt.Errorf("expecting 2 params for GET, instead got %v", v.String()))
				continue
			}
			fmt.Printf("retrieving from map for key %s\n", values[1].String())
			if v := s.EGet(values[1].String()); v != nil {
				fmt.Printf("Found in map for key %s, value: %s\n", values[1].String(), v.String())
				conn.WriteBytes(v.Bytes())
			} else {
				conn.WriteNull()
			}
			continue
		}
	}

}
