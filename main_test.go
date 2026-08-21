package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

type bufferConn struct {
	bytes.Buffer
}

func (c *bufferConn) Close() error { return nil }

func TestStreamReorderBufFlushesGapInOrder(t *testing.T) {
	rb := newStreamReorderBuf()
	if got := rb.put(0, []byte("a")); len(got) != 1 || string(got[0]) != "a" {
		t.Fatalf("put chunk 0 = %q, want a", got)
	}
	if got := rb.put(2, []byte("c")); len(got) != 0 {
		t.Fatalf("put chunk 2 flushed %q before gap was filled", got)
	}
	got := rb.put(1, []byte("b"))
	if len(got) != 2 || string(got[0]) != "b" || string(got[1]) != "c" {
		t.Fatalf("fill gap flushed %q, want [b c]", got)
	}
}

func TestDrainAckAcceptsFirstChunkZero(t *testing.T) {
	ackCh := make(chan uint32, 1)
	ackCh <- 0
	var ack uint32
	acked := false
	drainAck(ackCh, &ack, &acked)
	if !acked || ack != 0 {
		t.Fatalf("drainAck = (%d, %t), want (0, true)", ack, acked)
	}
}

func TestRetransmitByChunkMatchesStream(t *testing.T) {
	conn := &bufferConn{}
	sm := NewSerialMultiplexer(noReadConn{Writer: conn}, false)

	frame1 := testDataFrame(11, 3, 7, "wrong")
	frame2 := testDataFrame(12, 5, 7, "right")
	sm.storeSendBuf(11, 3, frame1)
	sm.storeSendBuf(12, 5, frame2)

	sm.retransmitByChunk(5, 7)
	if !bytes.Equal(conn.Bytes(), frame2) {
		t.Fatalf("retransmitted frame for wrong stream: got %x want %x", conn.Bytes(), frame2)
	}
}

func testDataFrame(seqNum, streamID, chunkNum uint32, body string) []byte {
	data := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(data, chunkNum)
	copy(data[4:], body)
	frame := make([]byte, 15+len(data)+4)
	binary.BigEndian.PutUint16(frame[0:2], protocolMagic)
	frame[2] = byte(MsgData)
	binary.BigEndian.PutUint32(frame[3:7], streamID)
	binary.BigEndian.PutUint32(frame[7:11], uint32(len(data)))
	binary.BigEndian.PutUint32(frame[11:15], seqNum)
	copy(frame[15:], data)
	return frame
}

type noReadConn struct {
	io.Writer
}

func (c noReadConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c noReadConn) Close() error             { return nil }
