// Command ingress serves the public nbork apex on SSH, HTTP and HTTPS.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/acme/autocert"
	"golang.org/x/crypto/ssh"
)

func main() {
	domain := flag.String("domain", "nbork.com", "public hostname")
	broker := flag.String("web-origin", "https://broker.nbork.com", "Worker HTTP origin")
	spectrum := flag.String("ssh-origin", "ssh.nbork.com:22", "Worker Spectrum endpoint")
	state := flag.String("state", "/var/lib/nbork-ingress", "persistent host pins and certificate cache")
	key := flag.String("host-key", "", "gateway SSH private key file")
	cert := flag.String("tls-cert", "", "pre-provisioned TLS certificate chain (otherwise use ACME)")
	tlsKey := flag.String("tls-key", "", "private key for -tls-cert")
	httpAddr := flag.String("http-listen", ":80", "HTTP listener")
	httpsAddr := flag.String("https-listen", ":443", "HTTPS listener")
	sshAddr := flag.String("ssh-listen", ":22", "SSH listener")
	flag.Parse()

	upstream, err := url.Parse(*broker)
	if err != nil || upstream.Host == "" || (upstream.Scheme != "https" && upstream.Scheme != "http") || upstream.Hostname() == *domain {
		log.Fatal("web-origin must be an HTTP(S) origin distinct from the public hostname")
	}
	if (*cert == "") != (*tlsKey == "") {
		log.Fatal("supply both -tls-cert and -tls-key")
	}
	privateKey, err := os.ReadFile(*key)
	if err != nil {
		log.Fatal("read -host-key: ", err)
	}
	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		log.Fatal("parse -host-key: ", err)
	}
	pins := filepath.Join(*state, "hosts")
	if err := os.MkdirAll(pins, 0700); err != nil {
		log.Fatal(err)
	}

	gateway := gateway{domain: *domain, endpoint: *spectrum, pins: pins, signer: signer}
	proxy := &httputil.ReverseProxy{Rewrite: func(req *httputil.ProxyRequest) {
		req.SetURL(upstream)
		req.SetXForwarded()
	}}
	web := http.NewServeMux()
	web.Handle(*domain+"/", proxy)
	redirect := http.NewServeMux()
	redirect.HandleFunc(*domain+"/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://"+*domain+r.URL.RequestURI(), http.StatusPermanentRedirect)
	})
	var plain http.Handler = redirect
	secure := &http.Server{Addr: *httpsAddr, Handler: web, ReadHeaderTimeout: 10 * time.Second}
	if *cert == "" {
		manager := &autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			Cache:      autocert.DirCache(filepath.Join(*state, "certificates")),
			HostPolicy: autocert.HostWhitelist(*domain),
		}
		secure.TLSConfig = manager.TLSConfig()
		plain = manager.HTTPHandler(redirect)
	} else {
		secure.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	clear := &http.Server{Addr: *httpAddr, Handler: plain, ReadHeaderTimeout: 10 * time.Second}
	listener, err := net.Listen("tcp", *sshAddr)
	if err != nil {
		log.Fatal(err)
	}
	httpListener, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		log.Fatal(err)
	}
	httpsListener, err := net.Listen("tcp", *httpsAddr)
	if err != nil {
		log.Fatal(err)
	}
	go func() { log.Fatal(clear.Serve(httpListener)) }()
	go func() { log.Fatal(secure.ServeTLS(httpsListener, *cert, *tlsKey)) }()
	log.Printf("serving SSH on %s and HTTPS on %s", listener.Addr(), *httpsAddr)
	handshakes := make(chan struct{}, 128)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		select {
		case handshakes <- struct{}{}:
			go gateway.serve(conn, func() { <-handshakes })
		default:
			conn.Close()
		}
	}
}

type gateway struct {
	domain, endpoint, pins string
	signer                 ssh.Signer
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
