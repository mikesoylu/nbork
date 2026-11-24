// This entrypoint runs one SSH session using sockets supplied by a Worker.
package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall/js"
	"time"

	"golang.org/x/crypto/ssh"
)

type result struct {
	value js.Value
	err   error
}

// Worker operations complete asynchronously so the Go scheduler can continue
// processing the other direction of the SSH stream while a read is pending.
func call(obj js.Value, method string, args ...any) (js.Value, error) {
	resultCh := make(chan result, 1)
	cb := js.FuncOf(func(_ js.Value, values []js.Value) any {
		if !values[0].IsNull() {
			resultCh <- result{err: errors.New(values[0].String())}
		} else {
			resultCh <- result{value: values[1]}
		}
		return nil
	})
	defer cb.Release()
	obj.Call(method, append(args, cb)...)
	r := <-resultCh
	return r.value, r.err
}

type workerSocket struct {
	socket  js.Value
	readMu  sync.Mutex
	pending []byte
}
type workerAddr string

func (a workerAddr) Network() string         { return "tcp" }
func (a workerAddr) String() string          { return string(a) }
func (c *workerSocket) LocalAddr() net.Addr  { return workerAddr("gateway:22") }
func (c *workerSocket) RemoteAddr() net.Addr { return workerAddr("peer:22") }
func (c *workerSocket) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for len(c.pending) == 0 {
		chunk, err := call(c.socket, "read")
		if err != nil {
			return 0, err
		}
		if chunk.IsNull() {
			return 0, io.EOF
		}
		c.pending = make([]byte, chunk.Length())
		js.CopyBytesToGo(c.pending, chunk)
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}
func (c *workerSocket) Write(p []byte) (int, error) {
	b := js.Global().Get("Uint8Array").New(len(p))
	js.CopyBytesToJS(b, p)
	if _, err := call(c.socket, "write", b); err != nil {
		return 0, err
	}
	return len(p), nil
}
func (c *workerSocket) Close() error { c.socket.Call("close"); return nil }
func (c *workerSocket) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}
func (c *workerSocket) SetReadDeadline(t time.Time) error  { return c.setDeadline("read", t) }
func (c *workerSocket) SetWriteDeadline(t time.Time) error { return c.setDeadline("write", t) }
func (c *workerSocket) setDeadline(direction string, t time.Time) error {
	timeout := -1
	if !t.IsZero() {
		timeout = max(0, int(time.Until(t).Milliseconds()))
	}
	c.socket.Call("deadline", direction, timeout)
	return nil
}

func main() {
	session := js.Global().Get("nborkSessions").Call("get", os.Args[1])
	signer, err := ssh.ParsePrivateKey([]byte(session.Get("key").String()))
	if err != nil {
		panic("invalid gateway host key")
	}
	g := gateway{signer: signer, dial: func(ctx context.Context, name string) (net.Conn, error) {
		deadline, _ := ctx.Deadline()
		sock, err := call(session, "dial", name, int(time.Until(deadline).Milliseconds()))
		if err != nil {
			return nil, err
		}
		return &workerSocket{socket: sock}, nil
	}, pin: func(name string, key ssh.PublicKey) error {
		ok, err := call(session, "pin", name, ssh.FingerprintSHA256(key))
		if err != nil {
			return err
		}
		if !ok.Bool() {
			return errors.New("channel host key changed")
		}
		return nil
	}}
	g.serve(&workerSocket{socket: session.Get("client")})
}
