package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/tarm/serial"
)

type Config struct {
	Role              string
	SerialDevice      string
	BaudRate          int
	ProxyListen       string
	TransparentListen string
	AllowLAN          bool
}

type MsgType byte

const (
	MsgTransparent  MsgType = 0x01
	MsgResponse     MsgType = 0x02
	MsgConnect      MsgType = 0x03
	MsgConnectOK    MsgType = 0x04
	MsgData         MsgType = 0x05
	MsgClose        MsgType = 0x06
	MsgError        MsgType = 0x07
)

const maxMessageSize = 10 * 1024 * 1024

type Message struct {
	Type     MsgType
	StreamID uint32
	Data     []byte
}

func compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompress(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}

func writeMessage(w io.Writer, msg Message) error {
	length := len(msg.Data)
	if length > maxMessageSize {
		return fmt.Errorf("message too large: %d", length)
	}
	header := make([]byte, 9)
	header[0] = byte(msg.Type)
	binary.BigEndian.PutUint32(header[1:5], msg.StreamID)
	binary.BigEndian.PutUint32(header[5:9], uint32(length))
	if _, err := w.Write(header); err != nil {
		return err
	}
	if length > 0 {
		if _, err := w.Write(msg.Data); err != nil {
			return err
		}
	}
	return nil
}

func readMessage(r io.Reader) (Message, error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(r, header); err != nil {
		return Message{}, err
	}
	msg := Message{
		Type:     MsgType(header[0]),
		StreamID: binary.BigEndian.Uint32(header[1:5]),
	}
	length := binary.BigEndian.Uint32(header[5:9])
	if length > maxMessageSize {
		return Message{}, fmt.Errorf("message too large: %d", length)
	}
	if length > 0 {
		msg.Data = make([]byte, length)
		if _, err := io.ReadFull(r, msg.Data); err != nil {
			return Message{}, err
		}
	}
	return msg, nil
}

type SerialMultiplexer struct {
	conn      io.ReadWriteCloser
	writeMu   sync.Mutex
	streams   map[uint32]chan Message
	streamsMu sync.RWMutex
	nextID    uint32
	isClient  bool
}

func NewSerialMultiplexer(conn io.ReadWriteCloser, isClient bool) *SerialMultiplexer {
	sm := &SerialMultiplexer{
		conn:     conn,
		streams:  make(map[uint32]chan Message),
		isClient: isClient,
	}
	if isClient {
		sm.nextID = 1
	}
	go sm.readLoop()
	return sm
}

func (sm *SerialMultiplexer) AllocStreamID() uint32 {
	return atomic.AddUint32(&sm.nextID, 2) - 2
}

func (sm *SerialMultiplexer) OpenStream(id uint32) chan Message {
	ch := make(chan Message, 256)
	sm.streamsMu.Lock()
	sm.streams[id] = ch
	sm.streamsMu.Unlock()
	return ch
}

func (sm *SerialMultiplexer) CloseStream(id uint32) {
	sm.streamsMu.Lock()
	delete(sm.streams, id)
	sm.streamsMu.Unlock()
}

func (sm *SerialMultiplexer) Send(msg Message) error {
	sm.writeMu.Lock()
	defer sm.writeMu.Unlock()
	return writeMessage(sm.conn, msg)
}

func (sm *SerialMultiplexer) readLoop() {
	for {
		msg, err := readMessage(sm.conn)
		if err != nil {
			if err == io.EOF {
				log.Println("Serial connection closed")
				return
			}
			log.Printf("Read message error: %v", err)
			continue
		}
		sm.dispatch(msg)
	}
}

func (sm *SerialMultiplexer) dispatch(msg Message) {
	sm.streamsMu.RLock()
	ch, ok := sm.streams[msg.StreamID]
	sm.streamsMu.RUnlock()

	if ok {
		select {
		case ch <- msg:
		default:
			log.Printf("Stream %d buffer full, dropping message type %d", msg.StreamID, msg.Type)
		}
		return
	}

	if !sm.isClient && (msg.Type == MsgTransparent || msg.Type == MsgConnect) {
		ch := sm.OpenStream(msg.StreamID)
		go sm.handleServerRequest(msg, ch)
	} else {
		log.Printf("Unexpected message: stream=%d type=%d", msg.StreamID, msg.Type)
	}
}

func (sm *SerialMultiplexer) handleServerRequest(msg Message, ch chan Message) {
	switch msg.Type {
	case MsgTransparent:
		sm.handleTransparent(msg, ch)
	case MsgConnect:
		sm.handleConnect(msg, ch)
	}
}

func (sm *SerialMultiplexer) handleTransparent(msg Message, ch chan Message) {
	defer sm.CloseStream(msg.StreamID)

	data, err := decompress(msg.Data)
	if err != nil {
		log.Printf("Decompress error: %v", err)
		sm.Send(Message{Type: MsgError, StreamID: msg.StreamID, Data: []byte("decompress failed")})
		return
	}

	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		log.Printf("Parse request error: %v", err)
		sm.Send(Message{Type: MsgError, StreamID: msg.StreamID, Data: []byte("parse request failed")})
		return
	}

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Proxy request error: %v", err)
		sm.Send(Message{Type: MsgError, StreamID: msg.StreamID, Data: []byte(err.Error())})
		return
	}
	defer resp.Body.Close()

	respData, err := httputil.DumpResponse(resp, true)
	if err != nil {
		log.Printf("Dump response error: %v", err)
		sm.Send(Message{Type: MsgError, StreamID: msg.StreamID, Data: []byte(err.Error())})
		return
	}

	compressed, err := compress(respData)
	if err != nil {
		log.Printf("Compress error: %v", err)
		sm.Send(Message{Type: MsgError, StreamID: msg.StreamID, Data: []byte("compress failed")})
		return
	}

	sm.Send(Message{Type: MsgResponse, StreamID: msg.StreamID, Data: compressed})
}

func (sm *SerialMultiplexer) handleConnect(msg Message, ch chan Message) {
	host := string(bytes.TrimSpace(msg.Data))
	if host == "" {
		sm.Send(Message{Type: MsgError, StreamID: msg.StreamID, Data: []byte("missing host")})
		sm.CloseStream(msg.StreamID)
		return
	}

	conn, err := net.Dial("tcp", host)
	if err != nil {
		sm.Send(Message{Type: MsgError, StreamID: msg.StreamID, Data: []byte(err.Error())})
		sm.CloseStream(msg.StreamID)
		return
	}

	if err := sm.Send(Message{Type: MsgConnectOK, StreamID: msg.StreamID, Data: []byte("OK")}); err != nil {
		conn.Close()
		sm.CloseStream(msg.StreamID)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	done := make(chan struct{})

	// remote -> serial
	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if sendErr := sm.Send(Message{Type: MsgData, StreamID: msg.StreamID, Data: buf[:n]}); sendErr != nil {
					close(done)
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("Connect remote read error: %v", err)
				}
				sm.Send(Message{Type: MsgClose, StreamID: msg.StreamID, Data: nil})
				close(done)
				return
			}
		}
	}()

	// serial -> remote
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-ch:
				switch msg.Type {
				case MsgData:
					if _, err := conn.Write(msg.Data); err != nil {
						conn.Close()
						return
					}
				case MsgClose:
					conn.Close()
					return
				}
			case <-done:
				conn.Close()
				return
			}
		}
	}()

	wg.Wait()
	sm.CloseStream(msg.StreamID)
	conn.Close()
}

type connectProxy struct {
	mux *SerialMultiplexer
}

func (p *connectProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodConnect {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	if host == "" {
		http.Error(w, "missing host", http.StatusBadRequest)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	streamID := p.mux.AllocStreamID()
	ch := p.mux.OpenStream(streamID)
	defer p.mux.CloseStream(streamID)

	if err := p.mux.Send(Message{Type: MsgConnect, StreamID: streamID, Data: []byte(host)}); err != nil {
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		return
	}

	msg := <-ch
	if msg.Type == MsgError {
		clientConn.Write([]byte(fmt.Sprintf("HTTP/1.1 502 Bad Gateway\r\n\r\n%s", msg.Data)))
		return
	}
	if msg.Type != MsgConnectOK {
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\nUnexpected response"))
		return
	}

	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	done := make(chan struct{})

	// browser -> serial
	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := clientConn.Read(buf)
			if n > 0 {
				if sendErr := p.mux.Send(Message{Type: MsgData, StreamID: streamID, Data: buf[:n]}); sendErr != nil {
					close(done)
					return
				}
			}
			if err != nil {
				p.mux.Send(Message{Type: MsgClose, StreamID: streamID, Data: nil})
				close(done)
				return
			}
		}
	}()

	// serial -> browser
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-ch:
				switch msg.Type {
				case MsgData:
					if _, err := clientConn.Write(msg.Data); err != nil {
						clientConn.Close()
						return
					}
				case MsgClose:
					clientConn.Close()
					return
				}
			case <-done:
				clientConn.Close()
				return
			}
		}
	}()

	wg.Wait()
}

type transparentProxy struct {
	mux *SerialMultiplexer
}

func (p *transparentProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = req.Host
	}

	dump, err := httputil.DumpRequest(req, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	streamID := p.mux.AllocStreamID()
	ch := p.mux.OpenStream(streamID)
	defer p.mux.CloseStream(streamID)

	compressed, err := compress(dump)
	if err != nil {
		http.Error(w, "compress failed", http.StatusInternalServerError)
		return
	}

	if err := p.mux.Send(Message{Type: MsgTransparent, StreamID: streamID, Data: compressed}); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	msg := <-ch
	if msg.Type == MsgError {
		http.Error(w, string(msg.Data), http.StatusBadGateway)
		return
	}

	data, err := decompress(msg.Data)
	if err != nil {
		http.Error(w, "decompress failed", http.StatusBadGateway)
		return
	}

	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(data)), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func parseFlags() *Config {
	cfg := &Config{}
	flag.StringVar(&cfg.Role, "role", "client", "Role: client or server")
	flag.StringVar(&cfg.SerialDevice, "serial", "/dev/ttyUSB0", "Serial device path")
	flag.IntVar(&cfg.BaudRate, "baud", 115200, "Baud rate")
	flag.StringVar(&cfg.ProxyListen, "proxy-listen", ":8080", "CONNECT proxy listen address")
	flag.StringVar(&cfg.TransparentListen, "transparent-listen", ":8081", "Transparent proxy listen address")
	flag.BoolVar(&cfg.AllowLAN, "allow-lan", false, "Allow LAN connections (listen on 0.0.0.0)")
	flag.Parse()
	return cfg
}

func openSerial(device string, baud int) (*serial.Port, error) {
	c := &serial.Config{Name: device, Baud: baud}
	return serial.OpenPort(c)
}

func main() {
	cfg := parseFlags()

	if cfg.Role != "client" && cfg.Role != "server" {
		log.Fatal("role must be 'client' or 'server'")
	}

	log.Printf("Starting as %s", cfg.Role)
	log.Printf("Serial: %s @ %d baud", cfg.SerialDevice, cfg.BaudRate)
	log.Printf("Proxy listen: %s, Transparent listen: %s, AllowLAN: %v", cfg.ProxyListen, cfg.TransparentListen, cfg.AllowLAN)

	serialConn, err := openSerial(cfg.SerialDevice, cfg.BaudRate)
	if err != nil {
		log.Fatalf("Failed to open serial: %v", err)
	}
	defer serialConn.Close()

	mux := NewSerialMultiplexer(serialConn, cfg.Role == "client")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		os.Exit(0)
	}()

	if cfg.Role == "client" {
		runClient(cfg, mux)
	} else {
		runServer(cfg, mux)
	}
}

func runClient(cfg *Config, mux *SerialMultiplexer) {
	var wg sync.WaitGroup

	addr := cfg.ProxyListen
	if !cfg.AllowLAN && addr != "" && addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	if addr != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("CONNECT proxy listening on %s", addr)
			if err := http.ListenAndServe(addr, &connectProxy{mux: mux}); err != nil {
				log.Fatalf("CONNECT proxy error: %v", err)
			}
		}()
	}

	addr = cfg.TransparentListen
	if !cfg.AllowLAN && addr != "" && addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	if addr != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("Transparent proxy listening on %s", addr)
			if err := http.ListenAndServe(addr, &transparentProxy{mux: mux}); err != nil {
				log.Fatalf("Transparent proxy error: %v", err)
			}
		}()
	}

	wg.Wait()
}

func runServer(cfg *Config, mux *SerialMultiplexer) {
	log.Println("Server running, waiting for requests...")
	select {}
}
