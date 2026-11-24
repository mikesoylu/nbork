package main

import (
	"context"
	"errors"
	"log"
	"net"
	"regexp"
	"time"

	"golang.org/x/crypto/ssh"
)

var channelName = regexp.MustCompile("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")

type gateway struct {
	dial   func(context.Context, string) (net.Conn, error)
	pin    func(string, ssh.PublicKey) error
	signer ssh.Signer
}

func (g *gateway) serve(socket net.Conn, handshakeDone func()) {
	defer socket.Close()
	deadline := time.Now().Add(90 * time.Second)
	socket.SetDeadline(deadline)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var backend *ssh.Client
	config := &ssh.ServerConfig{NoClientAuth: true, MaxAuthTries: 4}
	login := func(meta ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
		host, err := g.login(ctx, meta.User(), password)
		if err != nil {
			if !errors.Is(err, errPassword) {
				log.Printf("SSH backend unavailable for %.63q: %v", meta.User(), err)
				socket.Close()
			}
			return nil, err
		}
		backend = host
		return nil, nil
	}
	config.NoClientAuthCallback = func(meta ssh.ConnMetadata) (*ssh.Permissions, error) { return login(meta, nil) }
	config.PasswordCallback = login
	config.AddHostKey(g.signer)
	outer, channels, requests, err := ssh.NewServerConn(socket, config)
	handshakeDone()
	if backend != nil {
		defer backend.Close()
	}
	if err != nil {
		return
	}
	defer outer.Close()
	socket.SetDeadline(time.Time{})
	go ssh.DiscardRequests(requests)
	opened := false
	for ch := range channels {
		if opened || ch.ChannelType() != "session" {
			ch.Reject(ssh.Prohibited, "nbork supports one shell per connection")
			continue
		}
		opened = true
		go relaySession(ch, backend)
	}
}

var errPassword = errors.New("password authentication required")

func (g *gateway) login(ctx context.Context, name string, password []byte) (*ssh.Client, error) {
	if !channelName.MatchString(name) {
		return nil, errors.New("invalid channel name")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	raw, err := g.dial(ctx, name)
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
	requested := false
	config := &ssh.ClientConfig{
		User:            name,
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error { return g.pin(name, key) },
		Auth: []ssh.AuthMethod{ssh.PasswordCallback(func() (string, error) {
			requested = true
			if password == nil {
				return "", errPassword
			}
			return string(password), nil
		})},
	}
	conn, chans, reqs, err := ssh.NewClientConn(raw, name, config)
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
