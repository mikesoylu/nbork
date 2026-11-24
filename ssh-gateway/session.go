package main

import (
	"io"
	"sync"

	"golang.org/x/crypto/ssh"
)

func copyData(dst, src ssh.Channel) {
	io.Copy(dst, src)
	dst.CloseWrite()
}

func requests(dst ssh.Channel, incoming <-chan *ssh.Request, fromHost bool) {
	for r := range incoming {
		var allowed bool
		if fromHost {
			allowed = r.Type == "exit-status" || r.Type == "exit-signal"
		} else {
			allowed = r.Type == "shell" || r.Type == "pty-req" || r.Type == "window-change"
		}
		ok := false
		if allowed {
			ok, _ = dst.SendRequest(r.Type, r.WantReply, r.Payload)
		}
		if r.WantReply {
			r.Reply(ok, nil)
		}
	}
}

func relaySession(incoming ssh.NewChannel, backend *ssh.Client) {
	host, hostRequests, err := backend.OpenChannel("session", nil)
	if err != nil {
		incoming.Reject(ssh.ConnectionFailed, "host shell unavailable")
		return
	}
	defer host.Close()
	client, clientRequests, err := incoming.Accept()
	if err != nil {
		return
	}
	defer client.Close()
	go func() {
		requests(host, clientRequests, false)
		host.Close()
	}()
	go copyData(host, client)
	// Drain both output and the exit status before closing the client channel.
	var drain sync.WaitGroup
	drain.Go(func() { requests(client, hostRequests, true) })
	drain.Go(func() { io.Copy(client.Stderr(), host.Stderr()) })
	io.Copy(client, host)
	drain.Wait()
}
