package main

// 修复汇总:
// 1. [CRIT] sync.Once 保护 done channel — 防止双 goroutine 重复 close panic
// 2. [CRIT] 帧同步机制 (Magic Number) — 串口半包读取后自动恢复
// 3. [CRIT] 网络操作超时控制 — 防止永久阻塞
// 4. [HIGH] OpenStream 返回 bool — 防止重复 ID 覆盖
// 5. [HIGH] 所有 sm.Send 错误路径检查返回值
// 6. [HIGH] 透明代理验证消息类型
// 7. [MED]  writeMessage 一次性组装 buffer
// 8. [MED]  信号处理改为优雅关闭
// 9. [MED]  serial->remote goroutine 所有返回路径关闭 done
// 10. [MED] 透明代理响应 Header 过滤自动管理头
// 11. [LOW]  maxMessageSize 统一为 uint32
// 12. [LOW]  转发阶段处理 MsgError
// 13. [LOW]  完整串口配置参数
// 14. [LOW]  所有 Send 调用检查错误
// 15. [LOW]  context 控制生命周期

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tarm/serial"
)

type Config struct {
	Role              string
	SerialDevice      string
	BaudRate          int
	ProxyListen       string
	TransparentListen string
}

type MsgType byte

const (
	MsgTransparent MsgType = 0x01
	MsgResponse    MsgType = 0x02
	MsgConnect     MsgType = 0x03
	MsgConnectOK   MsgType = 0x04
	MsgData        MsgType = 0x05
	MsgClose       MsgType = 0x06
	MsgError       MsgType = 0x07
)

// 协议帧格式: [2字节Magic][1字节Type][4字节StreamID][4字节Length][Data]
// 帧同步魔数 — 用于检测和恢复帧边界
const (
	protocolMagic    = uint16(0xAA55)
	maxMessageSize   = uint32(10 * 1024 * 1024)
	readBufferSize   = 4096
	streamChanSize   = 256
	connectTimeout   = 10 * time.Second
	proxyTimeout     = 30 * time.Second
	dialTimeout      = 10 * time.Second
)

// Go HTTP 自动管理响应头 — 不应从原始响应复制
var autoManagedHeaders = map[string]bool{
	"content-length":   true,
	"transfer-encoding": true,
	"connection":       true,
	"date":             true,
	"trailer":          true,
}

type Message struct {
	Type     MsgType
	StreamID uint32
	Data     []byte
}

// ---------------------------------------------------------------------------
// 压缩 / 解压
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 协议读写（含帧同步）
// ---------------------------------------------------------------------------

func writeMessage(w io.Writer, msg Message) error {
	length := uint32(len(msg.Data))
	if length > maxMessageSize {
		return fmt.Errorf("message too large: %d", length)
	}
	// 一次性组装完整帧后写入，避免串口驱动分帧 [MED#7]
	buf := make([]byte, 11+length)
	binary.BigEndian.PutUint16(buf[0:2], protocolMagic)
	buf[2] = byte(msg.Type)
	binary.BigEndian.PutUint32(buf[3:7], msg.StreamID)
	binary.BigEndian.PutUint32(buf[7:11], length)
	copy(buf[11:], msg.Data)
	_, err := w.Write(buf)
	return err
}

func readMessage(r io.Reader) (Message, error) {
	header := make([]byte, 11)
	if _, err := io.ReadFull(r, header); err != nil {
		return Message{}, err
	}
	magic := binary.BigEndian.Uint16(header[0:2])
	if magic != protocolMagic {
		return Message{}, fmt.Errorf("invalid magic: 0x%04X", magic)
	}
	msg := Message{
		Type:     MsgType(header[2]),
		StreamID: binary.BigEndian.Uint32(header[3:7]),
	}
	length := binary.BigEndian.Uint32(header[7:11])
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

// ---------------------------------------------------------------------------
// SerialMultiplexer — 串口多路复用器
// ---------------------------------------------------------------------------

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
		sm.nextID = 1 // Client 使用奇数 ID
	} else {
		sm.nextID = ^uint32(0) // Server 不应主动分配 ID，设为无效值 [LOW#11]
	}
	go sm.readLoop()
	return sm
}

func (sm *SerialMultiplexer) AllocStreamID() uint32 {
	if !sm.isClient {
		panic("server should not allocate stream IDs")
	}
	return atomic.AddUint32(&sm.nextID, 2) - 2
}

// OpenStream 创建流 channel。若 ID 已存在则返回 false，防止覆盖 [HIGH#4]
func (sm *SerialMultiplexer) OpenStream(id uint32) (chan Message, bool) {
	ch := make(chan Message, streamChanSize)
	sm.streamsMu.Lock()
	defer sm.streamsMu.Unlock()
	if _, exists := sm.streams[id]; exists {
		return nil, false
	}
	sm.streams[id] = ch
	return ch, true
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

// ---------------------------------------------------------------------------
// 读取循环 + 帧同步 [CRIT#2]
// ---------------------------------------------------------------------------

func (sm *SerialMultiplexer) readLoop() {
	for {
		msg, err := sm.readMessageSync()
		if err != nil {
			if err == io.EOF {
				log.Println("Serial connection closed")
				return
			}
			log.Printf("Read message error: %v", err)
			// 帧同步会处理 magic 不匹配的情况，其他致命错误继续尝试
			continue
		}
		sm.dispatch(msg)
	}
}

// readMessageSync 读取一条消息，若帧错位自动进行帧同步
func (sm *SerialMultiplexer) readMessageSync() (Message, error) {
	header := make([]byte, 11)
	if _, err := io.ReadFull(sm.conn, header); err != nil {
		return Message{}, err
	}
	magic := binary.BigEndian.Uint16(header[0:2])
	if magic != protocolMagic {
		return sm.resync(header)
	}
	return parseMessageBody(sm.conn, header)
}

// resync 在已读数据中寻找合法的帧头，恢复帧同步
func (sm *SerialMultiplexer) resync(partial []byte) (Message, error) {
	log.Println("Frame desync detected, entering resync mode...")

	window := make([]byte, 0, 22)
	for _, b := range partial {
		window = append(window, b)
		if m := checkWindow(window); m != nil {
			return *m, nil
		}
	}

	buf := make([]byte, 1)
	scanLimit := int(maxMessageSize) + 22
	for scanned := 0; scanned < scanLimit; scanned++ {
		_, err := io.ReadFull(sm.conn, buf)
		if err != nil {
			return Message{}, fmt.Errorf("resync failed: %w", err)
		}
		window = append(window, buf[0])
		if len(window) > 22 {
			window = window[1:]
		}
		if m := checkWindow(window); m != nil {
			log.Println("Frame sync recovered")
			return *m, nil
		}
	}
	return Message{}, fmt.Errorf("resync exceeded scan limit (%d bytes)", scanLimit)
}

// checkWindow 检查滑动窗口末尾是否存在合法帧头
func checkWindow(window []byte) *Message {
	if len(window) < 11 {
		return nil
	}
	candidate := window[len(window)-11:]
	if binary.BigEndian.Uint16(candidate[0:2]) != protocolMagic {
		return nil
	}
	length := binary.BigEndian.Uint32(candidate[7:11])
	if length > maxMessageSize {
		return nil
	}
	msg := Message{
		Type:     MsgType(candidate[2]),
		StreamID: binary.BigEndian.Uint32(candidate[3:7]),
	}
	return &msg
}

func parseMessageBody(r io.Reader, header []byte) (Message, error) {
	msg := Message{
		Type:     MsgType(header[2]),
		StreamID: binary.BigEndian.Uint32(header[3:7]),
	}
	length := binary.BigEndian.Uint32(header[7:11])
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

// ---------------------------------------------------------------------------
// 消息分发
// ---------------------------------------------------------------------------

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

	// Server 端为新请求创建流 [HIGH#4: 检查 OpenStream 返回值]
	if !sm.isClient && (msg.Type == MsgTransparent || msg.Type == MsgConnect) {
		ch, ok := sm.OpenStream(msg.StreamID)
		if !ok {
			log.Printf("Stream %d already exists, dropping duplicate request", msg.StreamID)
			return
		}
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

// ---------------------------------------------------------------------------
// Server: 透明代理处理 [CRIT#3, HIGH#5]
// ---------------------------------------------------------------------------

func (sm *SerialMultiplexer) handleTransparent(msg Message, ch chan Message) {
	defer sm.CloseStream(msg.StreamID)

	data, err := decompress(msg.Data)
	if err != nil {
		log.Printf("Decompress error: %v", err)
		sm.sendError(msg.StreamID, "decompress failed")
		return
	}

	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		log.Printf("Parse request error: %v", err)
		sm.sendError(msg.StreamID, "parse request failed")
		return
	}

	// [FIX] 清空 RequestURI，http.Client.Do 禁止发送带 RequestURI 的请求
	req.RequestURI = ""

	// 确保 URL 完整（某些请求可能是相对路径）
	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = req.Host
	}

	client := &http.Client{
		Timeout: proxyTimeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Proxy request error: %v", err)
		sm.sendError(msg.StreamID, err.Error())
		return
	}
	defer resp.Body.Close()

	respData, err := httputil.DumpResponse(resp, true)
	if err != nil {
		log.Printf("Dump response error: %v", err)
		sm.sendError(msg.StreamID, err.Error())
		return
	}

	compressed, err := compress(respData)
	if err != nil {
		log.Printf("Compress error: %v", err)
		sm.sendError(msg.StreamID, "compress failed")
		return
	}

	if err := sm.Send(Message{Type: MsgResponse, StreamID: msg.StreamID, Data: compressed}); err != nil {
		log.Printf("Send response error: %v", err)
	}
}

// sendError 封装错误发送并检查返回值 [HIGH#5]
func (sm *SerialMultiplexer) sendError(streamID uint32, text string) {
	if err := sm.Send(Message{Type: MsgError, StreamID: streamID, Data: []byte(text)}); err != nil {
		log.Printf("Send error message failed: %v", err)
	}
}


// ---------------------------------------------------------------------------
// Server: CONNECT 处理 [CRIT#1, CRIT#3, MED#9]
// ---------------------------------------------------------------------------

func (sm *SerialMultiplexer) handleConnect(msg Message, ch chan Message) {
	host := string(bytes.TrimSpace(msg.Data))
	if host == "" {
		sm.sendError(msg.StreamID, "missing host")
		sm.CloseStream(msg.StreamID)
		return
	}

	// 带超时的 TCP 连接 [CRIT#3]
	conn, err := net.DialTimeout("tcp", host, connectTimeout)
	if err != nil {
		sm.sendError(msg.StreamID, err.Error())
		sm.CloseStream(msg.StreamID)
		return
	}

	if err := sm.Send(Message{Type: MsgConnectOK, StreamID: msg.StreamID, Data: []byte("OK")}); err != nil {
		log.Printf("Send ConnectOK failed: %v", err)
		conn.Close()
		sm.CloseStream(msg.StreamID)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	done := make(chan struct{})
	var doneOnce sync.Once
	closeDone := func() { doneOnce.Do(func() { close(done) }) } // [CRIT#1]

	// remote -> serial
	go func() {
		defer wg.Done()
		buf := make([]byte, readBufferSize)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if sendErr := sm.Send(Message{Type: MsgData, StreamID: msg.StreamID, Data: buf[:n]}); sendErr != nil {
					log.Printf("Send data failed: %v", sendErr)
					closeDone()
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("Connect remote read error: %v", err)
				}
				if serr := sm.Send(Message{Type: MsgClose, StreamID: msg.StreamID, Data: nil}); serr != nil {
					log.Printf("Send close failed: %v", serr)
				}
				closeDone()
				return
			}
		}
	}()

	// serial -> remote [MED#9: 所有返回路径关闭 done]
	go func() {
		defer wg.Done()
		for {
			select {
			case m := <-ch:
				switch m.Type {
				case MsgData:
					if _, err := conn.Write(m.Data); err != nil {
						log.Printf("Write to remote failed: %v", err)
						closeDone()
						conn.Close()
						return
					}
				case MsgClose:
					closeDone()
					conn.Close()
					return
				case MsgError: // [LOW#12]
					log.Printf("Received error for stream %d: %s", msg.StreamID, string(m.Data))
					closeDone()
					conn.Close()
					return
				default:
					log.Printf("Unexpected message type %d in CONNECT stream %d", m.Type, msg.StreamID)
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

// ---------------------------------------------------------------------------
// Client: CONNECT 代理 [CRIT#1, HIGH#6]
// ---------------------------------------------------------------------------

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
	// 注意: 不在此处 defer close，因为转发 goroutine 会管理连接生命周期

	streamID := p.mux.AllocStreamID()
	ch, ok := p.mux.OpenStream(streamID)
	if !ok {
		log.Printf("Failed to open stream %d: already exists", streamID)
		clientConn.Close()
		return
	}
	defer p.mux.CloseStream(streamID)

	if err := p.mux.Send(Message{Type: MsgConnect, StreamID: streamID, Data: []byte(host)}); err != nil {
		log.Printf("Send connect request failed: %v", err)
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		clientConn.Close()
		return
	}

	// 等待 server 响应（带超时保护）
	var respMsg Message
	select {
	case respMsg = <-ch:
	case <-time.After(connectTimeout):
		log.Printf("Connect handshake timeout for stream %d", streamID)
		clientConn.Write([]byte("HTTP/1.1 504 Gateway Timeout\r\n\r\n"))
		clientConn.Close()
		return
	}

	if respMsg.Type == MsgError {
		clientConn.Write([]byte(fmt.Sprintf("HTTP/1.1 502 Bad Gateway\r\n\r\n%s", respMsg.Data)))
		clientConn.Close()
		return
	}
	if respMsg.Type != MsgConnectOK {
		clientConn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\nUnexpected response"))
		clientConn.Close()
		return
	}

	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		clientConn.Close()
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	done := make(chan struct{})
	var doneOnce sync.Once
	closeDone := func() { doneOnce.Do(func() { close(done) }) } // [CRIT#1]

	// browser -> serial
	go func() {
		defer wg.Done()
		buf := make([]byte, readBufferSize)
		for {
			n, err := clientConn.Read(buf)
			if n > 0 {
				if sendErr := p.mux.Send(Message{Type: MsgData, StreamID: streamID, Data: buf[:n]}); sendErr != nil {
					log.Printf("Send data failed: %v", sendErr)
					closeDone()
					return
				}
			}
			if err != nil {
				if serr := p.mux.Send(Message{Type: MsgClose, StreamID: streamID, Data: nil}); serr != nil {
					log.Printf("Send close failed: %v", serr)
				}
				closeDone()
				return
			}
		}
	}()

	// serial -> browser [LOW#12: 处理 MsgError]
	go func() {
		defer wg.Done()
		for {
			select {
			case m := <-ch:
				switch m.Type {
				case MsgData:
					if _, err := clientConn.Write(m.Data); err != nil {
						log.Printf("Write to browser failed: %v", err)
						closeDone()
						clientConn.Close()
						return
					}
				case MsgClose:
					closeDone()
					clientConn.Close()
					return
				case MsgError:
					log.Printf("Received error for stream %d: %s", streamID, string(m.Data))
					closeDone()
					clientConn.Close()
					return
				default:
					log.Printf("Unexpected message type %d in CONNECT stream %d", m.Type, streamID)
				}
			case <-done:
				clientConn.Close()
				return
			}
		}
	}()

	wg.Wait()
	clientConn.Close()
}

// ---------------------------------------------------------------------------
// Client: 透明代理 [HIGH#6, MED#10]
// ---------------------------------------------------------------------------

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
	ch, ok := p.mux.OpenStream(streamID)
	if !ok {
		http.Error(w, "stream allocation failed", http.StatusInternalServerError)
		return
	}
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

	// 等待响应（带超时）
	var msg Message
	select {
	case msg = <-ch:
	case <-time.After(proxyTimeout):
		http.Error(w, "proxy timeout", http.StatusGatewayTimeout)
		return
	}

	if msg.Type == MsgError {
		http.Error(w, string(msg.Data), http.StatusBadGateway)
		return
	}
	// [HIGH#6] 严格验证消息类型
	if msg.Type != MsgResponse {
		http.Error(w, fmt.Sprintf("unexpected message type: %d", msg.Type), http.StatusBadGateway)
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

	// [MED#10] 复制响应头时过滤 Go 自动管理的头
	for k, vv := range resp.Header {
		lowerK := strings.ToLower(k)
		if autoManagedHeaders[lowerK] {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// ---------------------------------------------------------------------------
// CLI / 生命周期
// ---------------------------------------------------------------------------

func parseFlags() *Config {
	cfg := &Config{}
	flag.StringVar(&cfg.Role, "role", "client", "Role: client or server")
	flag.StringVar(&cfg.SerialDevice, "serial", "/dev/ttyUSB0", "Serial device path")
	flag.IntVar(&cfg.BaudRate, "baud", 115200, "Baud rate")
	flag.StringVar(&cfg.ProxyListen, "proxy-listen", ":8080", "CONNECT proxy listen address")
	flag.StringVar(&cfg.TransparentListen, "transparent-listen", ":8081", "Transparent proxy listen address")
	flag.Parse()
	return cfg
}

// [LOW#13] 完整串口配置
func openSerial(device string, baud int) (*serial.Port, error) {
	c := &serial.Config{
		Name:     device,
		Baud:     baud,
		Size:     8,
		StopBits: serial.Stop1,
		Parity:   serial.ParityNone,
	}
	return serial.OpenPort(c)
}

// ---------------------------------------------------------------------------
// Main [MED#8: 优雅关闭]
// ---------------------------------------------------------------------------

func main() {
	cfg := parseFlags()

	if cfg.Role != "client" && cfg.Role != "server" {
		log.Fatal("role must be 'client' or 'server'")
	}

	log.Printf("Starting as %s", cfg.Role)
	log.Printf("Serial: %s @ %d baud", cfg.SerialDevice, cfg.BaudRate)
	log.Printf("Protocol: magic=0x%04X, header=11 bytes", protocolMagic)
	log.Printf("Proxy listen: %s, Transparent listen: %s", cfg.ProxyListen, cfg.TransparentListen)

	serialConn, err := openSerial(cfg.SerialDevice, cfg.BaudRate)
	if err != nil {
		log.Fatalf("Failed to open serial: %v", err)
	}
	defer serialConn.Close()

	mux := NewSerialMultiplexer(serialConn, cfg.Role == "client")

	// Context 控制优雅关闭 [MED#8]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down gracefully...")
		serialConn.Close() // 触发 readLoop EOF 退出
		cancel()
	}()

	if cfg.Role == "client" {
		runClient(ctx, cfg, mux)
	} else {
		runServer(ctx, cfg, mux)
	}

	log.Println("Shutdown complete")
}

func runClient(ctx context.Context, cfg *Config, mux *SerialMultiplexer) {
	var wg sync.WaitGroup
	serverDone := make(chan struct{})
	serverCount := 0

	if cfg.ProxyListen != "" {
		serverCount++
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("CONNECT proxy listening on %s", cfg.ProxyListen)
			srv := &http.Server{
				Addr:    cfg.ProxyListen,
				Handler: &connectProxy{mux: mux},
			}
			go func() {
				<-ctx.Done()
				srv.Close()
			}()
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("CONNECT proxy error: %v", err)
			}
		}()
	}

	if cfg.TransparentListen != "" {
		serverCount++
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("Transparent proxy listening on %s", cfg.TransparentListen)
			srv := &http.Server{
				Addr:    cfg.TransparentListen,
				Handler: &transparentProxy{mux: mux},
			}
			go func() {
				<-ctx.Done()
				srv.Close()
			}()
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("Transparent proxy error: %v", err)
			}
		}()
	}

	// 等待 context 取消或所有服务器退出
	go func() {
		wg.Wait()
		close(serverDone)
	}()

	select {
	case <-ctx.Done():
		// 等待服务器优雅关闭（最多5秒）
		select {
		case <-serverDone:
		case <-time.After(5 * time.Second):
			log.Println("Server shutdown timeout")
		}
	case <-serverDone:
	}
}

func runServer(ctx context.Context, cfg *Config, mux *SerialMultiplexer) {
	log.Println("Server running, waiting for requests...")
	<-ctx.Done()
	log.Println("Server shutting down...")
}
