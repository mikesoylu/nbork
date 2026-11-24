package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"golang.org/x/crypto/ssh"
)

var channelName = regexp.MustCompile("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")

// Pin on first contact, before forwarding any password. Publishing via Link
// prevents two simultaneous first connections from trusting different hosts.
func pinHost(path string, key ssh.PublicKey) error {
	want := ssh.MarshalAuthorizedKey(key)
	check := func() error {
		saved, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		pinned, _, _, rest, err := ssh.ParseAuthorizedKey(saved)
		if err != nil || len(bytes.TrimSpace(rest)) != 0 {
			return errors.New("invalid channel host key file")
		}
		if !bytes.Equal(pinned.Marshal(), key.Marshal()) {
			return errors.New("channel host key changed")
		}
		return nil
	}
	if err := check(); !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".key-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.Write(want)
	closeErr := tmp.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Link(tmp.Name(), path); !errors.Is(err, os.ErrExist) {
		return err
	}
	return check()
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

// The deployed Worker may send its CONNECT response and the host's SSH
// identification together. Keep any bytes already buffered for the handshake.
func (g *gateway) login(ctx context.Context, name string, password []byte) (*ssh.Client, error) {
	if !channelName.MatchString(name) {
		return nil, errors.New("invalid channel name")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{Timeout: 8 * time.Second}).DialContext(ctx, "tcp", g.endpoint)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			raw.Close()
		}
	}()
	deadline, _ := ctx.Deadline()
	raw.SetDeadline(deadline)
	target := net.JoinHostPort(name+"."+g.domain, "22")
	if _, err := fmt.Fprintf(raw, "CONNECT %s HTTP/1.0\r\n\r\n", target); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(raw)
	// This is the fixed response produced by src/ssh-route.ts.
	const ok = "HTTP/1.0 200 Connection established\r\n\r\n"
	header := make([]byte, len(ok))
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}
	if string(header) != ok {
		return nil, errors.New("Spectrum origin rejected the channel")
	}

	requested := false
	config := &ssh.ClientConfig{
		User:            name,
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error { return pinHost(filepath.Join(g.pins, name), key) },
		Auth: []ssh.AuthMethod{ssh.PasswordCallback(func() (string, error) {
			requested = true
			if password == nil {
				return "", errPassword
			}
			return string(password), nil
		})},
	}
	conn, chans, reqs, err := ssh.NewClientConn(bufferedConn{raw, reader}, target, config)
	if err != nil {
		if requested {
			return nil, errPassword
		}
		return nil, err
	}
	raw.SetDeadline(time.Time{})
	keep = true
	return ssh.NewClient(conn, chans, reqs), nil
}
