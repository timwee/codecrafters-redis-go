package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/timwee/redis/rdb"
	"github.com/timwee/redis/resp"
)

var (
	dir    = flag.String("dir", "", "the dirname")
	dbfile = flag.String("dbfilename", "", "the rdb file")
)

const (
	PING         = "*1\r\n$4\r\nping\r\n"
	ECHO_PREFIX  = "*2\r\n$4\r\nECHO\r\n$"
	SEP          = "\r\n"
	PONG         = "+PONG\r\n"
	MAX_DURATION = 1<<63 - 1
)

type ExpirableEntry struct {
	ExpiryTime time.Time
	Val        *resp.Value
}

type Server struct {
	expirableStore map[string]*ExpirableEntry
	emu            sync.RWMutex
	dir            string
	dbFileName     string
}

// NewServer returns a new Server.
func NewServer(dir, dbfile string) *Server {
	serverStore := make(map[string]*ExpirableEntry)

	// if we're given an RDB file, read it into serverStore
	if dir != "" && dbfile != "" {
		f, err := rdb.NewRDBFileFromPath(path.Join(dir, dbfile))
		if err != nil {
			fmt.Printf("server: load-rdb: exited with error: %s, skipped", err)
		}
		if err := f.Parse(); err != nil {
			fmt.Printf("server: parse-rdb: exited with error: %s, skipped", err)
		} else {
			for k, vRDB := range f.GetDB() {
				expiryTime := vRDB.Expires
				if !vRDB.HasExpiry {
					expiryTime = time.Now().Add(MAX_DURATION)
				}
				respVal := resp.StringValue(vRDB.Value)
				expirableVal := ExpirableEntry{
					Val:        &respVal,
					ExpiryTime: expiryTime,
				}
				// fmt.Printf("Adding %v: %v\n", k, vRDB.Value)
				serverStore[k] = &expirableVal
			}
		}
	}

	return &Server{
		expirableStore: serverStore,
		dir:            dir,
		dbFileName:     dbfile,
	}
}

func (s *Server) ESet(k string, v *resp.Value, ttl time.Duration) {
	s.emu.Lock()
	defer s.emu.Unlock()

	s.expirableStore[k] = &ExpirableEntry{
		ExpiryTime: time.Now().Add(ttl),
		Val:        v,
	}
}

func (s *Server) EGet(k string) *resp.Value {
	s.emu.RLock()
	defer s.emu.RUnlock()

	if ev, ok := s.expirableStore[k]; ok {
		if ev.ExpiryTime.After(time.Now()) {
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
	flag.Parse()
	s := NewServer(*dir, *dbfile)
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
		case "KEYS":
			if len(values) != 2 || strings.ToUpper(values[1].String()) != "*" {
				fmt.Println(fmt.Errorf("expecting 2 params for KEYS, but got %v", v.String()))
				continue
			}
			if len(s.expirableStore) < 1 {
				conn.WriteNull()
				continue
			}
			var firstKey string
			for k := range s.expirableStore {
				firstKey = k
				break
			}
			conn.WriteArray([]resp.Value{
				resp.StringValue(firstKey),
			})
			continue
		case "CONFIG":
			if len(values) != 3 || strings.ToUpper(values[1].String()) != "GET" {
				fmt.Println(fmt.Errorf("expecting 3 params for CONFIG, but got %v", v.String()))
				continue
			}
			lConfigKey := values[2].String()
			configKey := strings.ToLower(lConfigKey)
			switch configKey {
			case "dir":
				conn.WriteArray([]resp.Value{
					resp.StringValue(configKey),
					resp.StringValue(s.dir),
				})
				continue
			case "dbfilename":
				conn.WriteArray([]resp.Value{
					resp.StringValue(configKey),
					resp.StringValue(s.dbFileName),
				})
				continue
			default:
				fmt.Println(fmt.Errorf("unexpected command %v", lConfigKey))
				continue
			}

		}

	}

}
