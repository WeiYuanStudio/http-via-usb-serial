package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
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
	Role            string
	SerialDevice    string
	BaudRate        int
	ProxyListen     string
	ReverseUpstream string
	ReverseListen   string
}

type MsgType byte

const (
	MsgTransparent     MsgType = 0x01
	MsgResponse        MsgType = 0x02
	MsgConnect         MsgType = 0x03
	MsgConnectOK       MsgType = 0x04
	MsgData            MsgType = 0x05
	MsgClose           MsgType = 0x06
	MsgError           MsgType = 0x07
	MsgResponseHeaders MsgType = 0x08
	MsgRetransmit      MsgType = 0x09
)

// 帧格式: [2B Magic][1B Type][4B StreamID][4B Length][4B SeqNum][Data][4B CRC32]
// CRC32 covers Data only; header is protected by Magic + resync
const (
	protocolMagic      = uint16(0x39C5)
	maxMessageSize     = uint32(10 * 1024 * 1024)
	readBufferSize     = 4096
	streamChanSize     = 256
	connectTimeout     = 10 * time.Second
	proxyTimeout       = 30 * time.Second
	dialTimeout        = 10 * time.Second
	streamChunkSize        = 256
	streamChunkTimeout     = 300 * time.Second
	largeResponseThreshold = 4 * 1024 * 1024
	largeStreamChunkSize   = 4096
	maxRetries             = 5
	sendBufSize            = 2048
)

var errBadCRC = errors.New("bad CRC")

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

type sendBufEntry struct {
	seqNum   uint32
	streamID uint32
	rawFrame []byte
	retries  int
	used     bool
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
// 协议读写（含帧同步 / CRC32 / 重传）
// ---------------------------------------------------------------------------

func (sm *SerialMultiplexer) writeMessage(w io.Writer, msg Message) error {
	length := uint32(len(msg.Data))
	if length > maxMessageSize {
		return fmt.Errorf("message too large: %d", length)
	}

	seqNum := atomic.AddUint32(&sm.sendSeq, 1) - 1

	headerSize := 15
	crc := crc32.ChecksumIEEE(msg.Data)
	totalLen := headerSize + int(length) + 4
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint16(buf[0:2], protocolMagic)
	buf[2] = byte(msg.Type)
	binary.BigEndian.PutUint32(buf[3:7], msg.StreamID)
	binary.BigEndian.PutUint32(buf[7:11], length)
	binary.BigEndian.PutUint32(buf[11:15], seqNum)
	copy(buf[15:], msg.Data)
	binary.BigEndian.PutUint32(buf[15+length:], crc)

	sm.storeSendBuf(seqNum, msg.StreamID, buf)

	if err := writeFull(w, buf); err != nil {
		return err
	}
	return nil
}

// readMessage 读取完整消息（对外兼容，不处理重传逻辑）
func readMessage(r io.Reader) (Message, error) {
	header := make([]byte, 15)
	if _, err := io.ReadFull(r, header); err != nil {
		return Message{}, err
	}
	magic := binary.BigEndian.Uint16(header[0:2])
	if magic != protocolMagic {
		return Message{}, fmt.Errorf("invalid magic: 0x%04X", magic)
	}
	return parseMessageBody(r, header)
}

// ---------------------------------------------------------------------------
// SerialMultiplexer — 串口多路复用器
// ---------------------------------------------------------------------------

type SerialMultiplexer struct {
	conn       io.ReadWriteCloser
	scanBuf    []byte
	writeMu    sync.Mutex
	streams    map[uint32]chan Message
	streamsMu  sync.RWMutex
	nextID     uint32
	isClient   bool
	httpsHosts sync.Map

	sendSeq      uint32
	sendBuf      []sendBufEntry
	sendBufMu    sync.Mutex
	processedSeq map[uint32]bool
	processedMu  sync.Mutex
}

func writeFull(w io.Writer, buf []byte) error {
	for written := 0; written < len(buf); {
		n, err := w.Write(buf[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func NewSerialMultiplexer(conn io.ReadWriteCloser, isClient bool) *SerialMultiplexer {
	sm := &SerialMultiplexer{
		conn:         conn,
		streams:      make(map[uint32]chan Message),
		isClient:     isClient,
		sendBuf:      make([]sendBufEntry, sendBufSize),
		processedSeq: make(map[uint32]bool),
	}
	if isClient {
		sm.nextID = 1
	} else {
		sm.nextID = ^uint32(0)
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
	return sm.writeMessage(sm.conn, msg)
}

// ---------------------------------------------------------------------------
// 读取循环 + 帧同步 + CRC 校验 + 重传请求
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
			continue
		}
		sm.dispatch(msg)
	}
}

func (sm *SerialMultiplexer) readMessageSync() (Message, error) {
	searchPos := 0
	for {
		for searchPos+15 <= len(sm.scanBuf) {
			if binary.BigEndian.Uint16(sm.scanBuf[searchPos:searchPos+2]) != protocolMagic {
				searchPos++
				continue
			}
			msgType := sm.scanBuf[searchPos+2]
			if msgType < 1 || msgType > 9 {
				searchPos++
				continue
			}
			length := binary.BigEndian.Uint32(sm.scanBuf[searchPos+7 : searchPos+11])
			if length > maxMessageSize {
				searchPos++
				continue
			}

			totalNeeded := searchPos + 15 + int(length) + 4
			if totalNeeded > len(sm.scanBuf) {
				break
			}

			bodyStart := searchPos + 15
			bodyEnd := bodyStart + int(length)
			body := sm.scanBuf[bodyStart:bodyEnd]
			crcBytes := sm.scanBuf[bodyEnd:totalNeeded]

			if binary.BigEndian.Uint32(crcBytes) != crc32.ChecksumIEEE(body) {
				seqNum := binary.BigEndian.Uint32(sm.scanBuf[searchPos+11 : searchPos+15])
				streamID := binary.BigEndian.Uint32(sm.scanBuf[searchPos+3 : searchPos+7])
				if streamID != 0 {
					sm.requestRetransmit(seqNum)
				}
				log.Printf("Bad CRC: seq=%d stream=%d type=%d len=%d, requesting retransmit", seqNum, streamID, msgType, length)
				searchPos++
				continue
			}

			seqNum := binary.BigEndian.Uint32(sm.scanBuf[searchPos+11 : searchPos+15])
			msg := Message{
				Type:     MsgType(msgType),
				StreamID: binary.BigEndian.Uint32(sm.scanBuf[searchPos+3 : searchPos+7]),
				Data:     make([]byte, length),
			}
			if length > 0 {
				copy(msg.Data, body)
			}

			sm.scanBuf = sm.scanBuf[totalNeeded:]
			searchPos = 0

			if sm.isProcessed(seqNum) {
				continue
			}
			sm.markProcessed(seqNum)
			return msg, nil
		}

		if searchPos > 65536 {
			sm.scanBuf = sm.scanBuf[searchPos:]
			searchPos = 0
		}

		tmp := make([]byte, 65536)
		n, err := sm.conn.Read(tmp)
		if n > 0 {
			sm.scanBuf = append(sm.scanBuf, tmp[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				return Message{}, err
			}
			log.Printf("Serial read error in scan loop: %v", err)
			continue
		}
	}
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
	crcBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, crcBytes); err != nil {
		return Message{}, err
	}
	expectedCRC := binary.BigEndian.Uint32(crcBytes)
	actualCRC := crc32.ChecksumIEEE(msg.Data)
	if expectedCRC != actualCRC {
		return Message{}, errBadCRC
	}
	return msg, nil
}

// ---------------------------------------------------------------------------
// 发送缓冲区
// ---------------------------------------------------------------------------

func (sm *SerialMultiplexer) storeSendBuf(seqNum uint32, streamID uint32, frame []byte) {
	sm.sendBufMu.Lock()
	defer sm.sendBufMu.Unlock()
	idx := seqNum % sendBufSize
	sm.sendBuf[idx] = sendBufEntry{
		seqNum:   seqNum,
		streamID: streamID,
		rawFrame: frame,
		retries:  0,
		used:     true,
	}
}

func (sm *SerialMultiplexer) requestRetransmit(seqNum uint32) {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, seqNum)
	if err := sm.Send(Message{Type: MsgRetransmit, StreamID: 0, Data: data}); err != nil {
		log.Printf("Failed to send retransmit request for seq=%d: %v", seqNum, err)
	}
}

func (sm *SerialMultiplexer) isProcessed(seqNum uint32) bool {
	sm.processedMu.Lock()
	defer sm.processedMu.Unlock()
	return sm.processedSeq[seqNum]
}

func (sm *SerialMultiplexer) markProcessed(seqNum uint32) {
	sm.processedMu.Lock()
	defer sm.processedMu.Unlock()
	sm.processedSeq[seqNum] = true
	if len(sm.processedSeq) > sendBufSize*2 {
		threshold := seqNum - sendBufSize
		for k := range sm.processedSeq {
			if k < threshold {
				delete(sm.processedSeq, k)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 消息分发
// ---------------------------------------------------------------------------

func (sm *SerialMultiplexer) dispatch(msg Message) {
	if msg.Type == MsgRetransmit {
		sm.handleMsgRetransmit(msg)
		return
	}

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

func (sm *SerialMultiplexer) handleMsgRetransmit(msg Message) {
	if len(msg.Data) < 4 {
		return
	}
	seqNum := binary.BigEndian.Uint32(msg.Data)

	sm.sendBufMu.Lock()
	idx := seqNum % sendBufSize
	entry := &sm.sendBuf[idx]
	if !entry.used || entry.seqNum != seqNum {
		sm.sendBufMu.Unlock()
		return
	}

	if entry.retries >= maxRetries {
		streamID := entry.streamID
		entry.used = false
		sm.sendBufMu.Unlock()
		log.Printf("[RETRANSMIT] seq=%d stream=%d max retries exceeded, abandoning", seqNum, streamID)
		sm.sendError(streamID, "max retries exceeded")
		return
	}

	entry.retries++
	frameCopy := make([]byte, len(entry.rawFrame))
	copy(frameCopy, entry.rawFrame)
	sm.sendBufMu.Unlock()

	log.Printf("[RETRANSMIT] Resending seq=%d stream=%d retry=%d/%d", seqNum, entry.streamID, entry.retries, maxRetries)
	sm.writeMu.Lock()
	if err := writeFull(sm.conn, frameCopy); err != nil {
		log.Printf("[RETRANSMIT] seq=%d stream=%d write error: %v", seqNum, entry.streamID, err)
	}
	sm.writeMu.Unlock()
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
// Server: 透明代理处理
// ---------------------------------------------------------------------------

func (sm *SerialMultiplexer) handleTransparent(msg Message, ch chan Message) {
	defer sm.CloseStream(msg.StreamID)

	data, err := decompress(msg.Data)
	if err != nil {
		log.Printf("Decompress error: %v", err)
		sm.sendError(msg.StreamID, "decompress failed")
		return
	}
	log.Printf("[SERVER-TRANSPARENT] stream=%d received %d bytes\n%s", msg.StreamID, len(data), string(data))

	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		log.Printf("Parse request error: %v", err)
		sm.sendError(msg.StreamID, "parse request failed")
		return
	}

	req.RequestURI = ""

	log.Printf("[SERVER-TRANSPARENT] stream=%d parsed: method=%s url=%s scheme=%s host=%s requestURI=%s",
		msg.StreamID, req.Method, req.URL.String(), req.URL.Scheme, req.URL.Host, req.RequestURI)

	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = req.Host
	}

	hostKey := req.URL.Host
	if knownScheme, ok := sm.httpsHosts.Load(hostKey); ok && knownScheme.(string) == "https" {
		req.URL.Scheme = "https"
	}

	log.Printf("[SERVER-TRANSPARENT] stream=%d after fixup: scheme=%s url=%s -> making request",
		msg.StreamID, req.URL.Scheme, req.URL.String())

	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, _ = io.ReadAll(req.Body)
		req.Body.Close()
	}
	req.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	var cancelled atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamDone := make(chan struct{})
	go func() {
		for {
			select {
			case m, ok := <-ch:
				if !ok {
					return
				}
				if m.Type == MsgClose {
					log.Printf("[SERVER] stream=%d client disconnected, cancelling request", msg.StreamID)
					cancelled.Store(true)
					cancel()
					return
				}
			case <-streamDone:
				return
			}
		}
	}()
	defer close(streamDone)

	req = req.WithContext(ctx)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ResponseHeaderTimeout: proxyTimeout,
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

	if isHTTPToHTTPSRedirect(resp) {
		resp.Body.Close()
		sm.httpsHosts.Store(hostKey, "https")
		log.Printf("[SERVER-TRANSPARENT] stream=%d host=%s redirected to HTTPS, retrying", msg.StreamID, hostKey)

		retryReq := req.Clone(ctx)
		retryReq.URL.Scheme = "https"
		retryReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		resp2, err := client.Do(retryReq)
		if err != nil {
			log.Printf("Proxy HTTPS retry error: %v", err)
			sm.sendError(msg.StreamID, err.Error())
			return
		}
		resp = resp2
	}

	defer resp.Body.Close()
	log.Printf("[SERVER-TRANSPARENT] stream=%d response: status=%d", msg.StreamID, resp.StatusCode)

	contentLen := resp.ContentLength
	if isStreamResponse(resp) {
		sm.handleTransparentStream(msg.StreamID, resp, &cancelled, streamChunkSize)
	} else if contentLen > largeResponseThreshold || contentLen < 0 {
		sm.handleTransparentStream(msg.StreamID, resp, &cancelled, largeStreamChunkSize)
	} else {
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
}

func (sm *SerialMultiplexer) handleTransparentStream(streamID uint32, resp *http.Response, cancelled *atomic.Bool, chunkSize int) {
	headers := dumpResponseHeaders(resp)
	compressed, err := compress(headers)
	if err != nil {
		log.Printf("Compress headers error: %v", err)
		sm.sendError(streamID, "compress headers failed")
		return
	}
	if err := sm.Send(Message{Type: MsgResponseHeaders, StreamID: streamID, Data: compressed}); err != nil {
		log.Printf("Send response headers error: %v", err)
		return
	}

	buf := make([]byte, chunkSize)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if sendErr := sm.Send(Message{Type: MsgData, StreamID: streamID, Data: buf[:n]}); sendErr != nil {
				log.Printf("Send stream data error: %v", sendErr)
				return
			}
		}
		if err != nil {
			if cancelled.Load() {
				log.Printf("[SERVER] stream=%d cancelled by client", streamID)
				return
			}
			if err != io.EOF {
				log.Printf("Read stream body error: %v", err)
				sm.sendError(streamID, err.Error())
			}
			break
		}
	}
	sm.Send(Message{Type: MsgClose, StreamID: streamID, Data: nil})
}

func isStreamResponse(resp *http.Response) bool {
	return strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")
}

func isHTTPToHTTPSRedirect(resp *http.Response) bool {
	if resp.StatusCode < 300 || resp.StatusCode > 399 {
		return false
	}
	location := resp.Header.Get("Location")
	if location == "" {
		return false
	}
	if !strings.Contains(location, "https") {
		return false
	}
	u, err := url.Parse(location)
	if err != nil || u.Scheme != "https" {
		return false
	}
	return true
}

func dumpResponseHeaders(resp *http.Response) []byte {
	var buf bytes.Buffer
	buf.WriteString(resp.Proto)
	buf.WriteByte(' ')
	buf.WriteString(resp.Status)
	buf.WriteString("\r\n")
	for k, vv := range resp.Header {
		for _, v := range vv {
			buf.WriteString(k)
			buf.WriteString(": ")
			buf.WriteString(v)
			buf.WriteString("\r\n")
		}
	}
	buf.WriteString("\r\n")
	return buf.Bytes()
}

func (sm *SerialMultiplexer) sendError(streamID uint32, text string) {
	if err := sm.Send(Message{Type: MsgError, StreamID: streamID, Data: []byte(text)}); err != nil {
		log.Printf("Send error message failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Server: CONNECT 处理
// ---------------------------------------------------------------------------

func (sm *SerialMultiplexer) handleConnect(msg Message, ch chan Message) {
	host := string(bytes.TrimSpace(msg.Data))
	if host == "" {
		sm.sendError(msg.StreamID, "missing host")
		sm.CloseStream(msg.StreamID)
		return
	}

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
	closeDone := func() { doneOnce.Do(func() { close(done) }) }

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
				case MsgError:
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
// Client: CONNECT 代理
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
	closeDone := func() { doneOnce.Do(func() { close(done) }) }

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
// Client: 透明代理
// ---------------------------------------------------------------------------

type transparentProxy struct {
	mux *SerialMultiplexer
}

type reverseProxy struct {
	mux      *SerialMultiplexer
	upstream *url.URL
}

func (p *reverseProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Printf("[REVERSE] original: method=%s host=%s url=%s scheme=%s", req.Method, req.Host, req.URL.String(), req.URL.Scheme)
	req.URL.Scheme = p.upstream.Scheme
	req.URL.Host = p.upstream.Host
	req.Host = p.upstream.Host
	log.Printf("[REVERSE] rewritten: scheme=%s host=%s url=%s", req.URL.Scheme, req.URL.Host, req.URL.String())
	(&transparentProxy{mux: p.mux}).ServeHTTP(w, req)
}

type unifiedProxy struct {
	mux *SerialMultiplexer
}

func (p *unifiedProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodConnect {
		(&connectProxy{mux: p.mux}).ServeHTTP(w, req)
	} else {
		(&transparentProxy{mux: p.mux}).ServeHTTP(w, req)
	}
}

func (p *transparentProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = req.Host
	}

	dump, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	streamID := p.mux.AllocStreamID()
	log.Printf("[TRANSPARENT] stream=%d dumped request (%d bytes):\n%s", streamID, len(dump), string(dump))

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

	if msg.Type == MsgResponseHeaders {
		p.handleStreamResponse(w, req, streamID, ch, msg)
		return
	}

	if msg.Type != MsgResponse {
		http.Error(w, fmt.Sprintf("unexpected message type: %d", msg.Type), http.StatusBadGateway)
		return
	}

	data, err := decompress(msg.Data)
	if err != nil {
		http.Error(w, "decompress failed", http.StatusBadGateway)
		return
	}

	// log.Printf("[TRANSPARENT] stream=%d received response (%d bytes):\n%s", streamID, len(data), string(data))

	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(data)), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		lowerK := strings.ToLower(k)
		if autoManagedHeaders[lowerK] {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	log.Printf("[TRANSPARENT] stream=%d returning to client: status=%d", streamID, resp.StatusCode)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (p *transparentProxy) handleStreamResponse(w http.ResponseWriter, req *http.Request, streamID uint32, ch chan Message, headersMsg Message) {
	headersData, err := decompress(headersMsg.Data)
	if err != nil {
		http.Error(w, "decompress headers failed", http.StatusBadGateway)
		return
	}

	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(headersData)), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		lowerK := strings.ToLower(k)
		if autoManagedHeaders[lowerK] {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	log.Printf("[TRANSPARENT] stream=%d streaming response: status=%d", streamID, resp.StatusCode)
	w.WriteHeader(resp.StatusCode)

	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Printf("[TRANSPARENT] stream=%d Flusher not supported, streaming disabled", streamID)
		return
	}

	for {
		var m Message
		select {
		case m = <-ch:
		case <-time.After(streamChunkTimeout):
			log.Printf("[TRANSPARENT] stream=%d chunk timeout", streamID)
			return
		}

		switch m.Type {
		case MsgData:
			if _, err := w.Write(m.Data); err != nil {
				log.Printf("[TRANSPARENT] stream=%d write chunk error, sending close to server: %v", streamID, err)
				p.mux.Send(Message{Type: MsgClose, StreamID: streamID, Data: nil})
				return
			}
			flusher.Flush()
		case MsgClose:
			return
		case MsgError:
			log.Printf("[TRANSPARENT] stream=%d received error: %s", streamID, string(m.Data))
			return
		default:
			log.Printf("[TRANSPARENT] stream=%d unexpected message type %d in stream", streamID, m.Type)
		}
	}
}

// ---------------------------------------------------------------------------
// CLI / 生命周期
// ---------------------------------------------------------------------------

func parseFlags() *Config {
	cfg := &Config{}
	flag.StringVar(&cfg.Role, "role", "client", "Role: client or server")
	flag.StringVar(&cfg.SerialDevice, "serial", "/dev/ttyUSB0", "Serial device path")
	flag.IntVar(&cfg.BaudRate, "baud", 115200, "Baud rate")
	flag.StringVar(&cfg.ProxyListen, "proxy-listen", ":8080", "Proxy listen address (supports CONNECT + transparent)")
	flag.StringVar(&cfg.ReverseUpstream, "reverse-upstream", "https://api.deepseek.com", "Reverse proxy upstream URL")
	flag.StringVar(&cfg.ReverseListen, "reverse-listen", ":8081", "Reverse proxy listen address (empty to disable)")
	flag.Parse()
	return cfg
}

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
// Main
// ---------------------------------------------------------------------------

func main() {
	cfg := parseFlags()

	if cfg.Role != "client" && cfg.Role != "server" {
		log.Fatal("role must be 'client' or 'server'")
	}

	log.Printf("Starting as %s", cfg.Role)
	log.Printf("Serial: %s @ %d baud", cfg.SerialDevice, cfg.BaudRate)
	log.Printf("Protocol: magic=0x%04X, header=15 bytes, CRC32", protocolMagic)
	log.Printf("Retransmit: maxRetries=%d, sendBuf=%d entries", maxRetries, sendBufSize)
	log.Printf("Proxy listen: %s (CONNECT + transparent)", cfg.ProxyListen)

	serialConn, err := openSerial(cfg.SerialDevice, cfg.BaudRate)
	if err != nil {
		log.Fatalf("Failed to open serial: %v", err)
	}
	defer serialConn.Close()

	mux := NewSerialMultiplexer(serialConn, cfg.Role == "client")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down gracefully...")
		serialConn.Close()
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
	if cfg.ProxyListen == "" {
		log.Fatal("proxy-listen is required for client")
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Proxy listening on %s (CONNECT + transparent)", cfg.ProxyListen)
		srv := &http.Server{
			Addr:    cfg.ProxyListen,
			Handler: &unifiedProxy{mux: mux},
		}
		go func() {
			<-ctx.Done()
			srv.Close()
		}()
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Proxy error: %v", err)
		}
	}()

	if cfg.ReverseListen != "" && cfg.ReverseUpstream != "" {
		upstreamURL, err := url.Parse(cfg.ReverseUpstream)
		if err != nil {
			log.Fatalf("Invalid reverse-upstream URL: %v", err)
		}
		if upstreamURL.Host == "" || upstreamURL.Scheme == "" {
			log.Fatal("reverse-upstream must be a full URL (e.g. https://api.deepseek.com)")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("Reverse proxy listening on %s -> %s", cfg.ReverseListen, cfg.ReverseUpstream)
			srv := &http.Server{
				Addr:    cfg.ReverseListen,
				Handler: &reverseProxy{mux: mux, upstream: upstreamURL},
			}
			go func() {
				<-ctx.Done()
				srv.Close()
			}()
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("Reverse proxy error: %v", err)
			}
		}()
	}

	wg.Wait()
}

func runServer(ctx context.Context, cfg *Config, mux *SerialMultiplexer) {
	log.Println("Server running, waiting for requests...")
	<-ctx.Done()
	log.Println("Server shutting down...")
}
