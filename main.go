package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opencensus.io/examples/exporter"
	"go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.opencensus.io/zpages"
)

const (
	contentMediaType = "application/vnd.git-lfs"
	metaMediaType    = contentMediaType + "+json"
	version          = "0.4.0"
)

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}

func wrapHttps(l net.Listener, cert, key string) (net.Listener, error) {
	var err error

	config := &tls.Config{}

	if config.NextProtos == nil {
		config.NextProtos = []string{"http/1.1"}
	}

	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}

	netListener := l.(*TrackingListener).Listener

	tlsListener := tls.NewListener(tcpKeepAliveListener{netListener.(*net.TCPListener)}, config)
	return tlsListener, nil
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "-v" {
		fmt.Println(version)
		os.Exit(0)
	}

	var listener net.Listener

	tl, err := NewTrackingListener(Config.Listen)
	if err != nil {
		log.WithFields(log.Fields{"fn": "main", "err": "Could not create listener"}).Fatal(err.Error())
	}

	listener = tl

	if Config.Cert != "" && Config.Key != "" {
		log.WithFields(log.Fields{"fn": "main", "msg": "Using https"}).Info()
		listener, err = wrapHttps(tl, Config.Cert, Config.Key)
		if err != nil {
			log.WithFields(log.Fields{"fn": "main", "err": "Could not create https listener"}).Fatal(err.Error())
		}
	}

	switch strings.ToLower(Config.LogLevel) {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "panic":
		log.SetLevel(log.PanicLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	metaStore := NewS3MetaStore()
	contentStore := NewS3ContentStore()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	go func(c chan os.Signal, listener net.Listener) {
		for {
			sig := <-c
			switch sig {
			case syscall.SIGHUP: // Graceful shutdown
				tl.Close()
			}
		}
	}(c, tl)

	log.WithFields(
		log.Fields{
			"fn":      "main",
			"msg":     "listening",
			"pid":     os.Getpid(),
			"addr":    Config.Listen,
			"version": version,
		}).Info("Starting up")

	pe, err := prometheus.NewExporter(prometheus.Options{
		Namespace: "edgestore",
	})
	if err != nil {
		log.Fatalf("Failed to create the Prometheus stats exporter: %v", err)
	}
	view.RegisterExporter(pe)

	//FIXME: Register stats and trace exporters to export the collected data.
	exporter := &exporter.PrintExporter{}
	trace.RegisterExporter(exporter)

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	// Report stats at every second.
	view.SetReportingPeriod(1 * time.Second)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", pe)
		zpages.Handle(mux, "/")
		log.WithField("address", ":8888").Info("Starting metrics server")
		if err := http.ListenAndServe(":8888", mux); err != nil {
			log.Fatalf(err.Error())
		}
	}()
	app := NewApp(contentStore, metaStore)
	if Config.IsUsingTus() {
		tusServer.Start()
	}
	app.Serve(listener)
	tl.WaitForChildren()
	if Config.IsUsingTus() {
		tusServer.Stop()
	}
}
