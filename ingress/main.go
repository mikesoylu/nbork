//go:build !js

// Command ingress serves the public nbork apex on SSH, HTTP and HTTPS.
package main

import (
	"crypto/tls"

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

	gateway := gateway{
		signer: signer,
		dial:   spectrumDialer(*spectrum, *domain),
		pin:    func(name string, key ssh.PublicKey) error { return pinHost(filepath.Join(pins, name), key) },
	}
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
