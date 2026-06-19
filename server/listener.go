package server

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/soheilhy/cmux"
	"github.com/pucora/lura/v2/config"
	serverhttp "github.com/pucora/lura/v2/transport/http/server"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	grpcconfig "github.com/pucora/pucora-grpc/v2/config"
)

func grpcServerOptions(serverCfg *grpcconfig.ServerConfig) []grpc.ServerOption {
	if serverCfg == nil || (serverCfg.DisableMetrics && serverCfg.DisableTraces) {
		return nil
	}
	mp := otel.GetMeterProvider()
	if serverCfg.DisableMetrics {
		mp = noopmetric.NewMeterProvider()
	}
	tp := otel.GetTracerProvider()
	if serverCfg.DisableTraces {
		tp = nooptrace.NewTracerProvider()
	}
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithMeterProvider(mp),
			otelgrpc.WithTracerProvider(tp),
		)),
	}
}

func multiplex(listener net.Listener, cfg config.ServiceConfig) (cmux.CMux, net.Listener, net.Listener) {
	mux := cmux.New(listener)
	if tlsEnabled(cfg) {
		return mux, mux.Match(cmux.HTTP2()), mux.Match(cmux.HTTP1Fast())
	}
	grpcListener := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpListener := mux.Match(cmux.Any())
	return mux, grpcListener, httpListener
}

func wrapTLS(listener net.Listener, cfg config.ServiceConfig) (net.Listener, error) {
	if !tlsEnabled(cfg) {
		return listener, nil
	}
	tlsCfg := serverhttp.ParseTLSConfig(cfg.TLS)
	if cfg.TLS.PublicKey != "" || cfg.TLS.PrivateKey != "" {
		cfg.TLS.Keys = append(cfg.TLS.Keys, config.TLSKeyPair{
			PublicKey:  cfg.TLS.PublicKey,
			PrivateKey: cfg.TLS.PrivateKey,
		})
	}
	for _, k := range cfg.TLS.Keys {
		cert, err := tls.LoadX509KeyPair(k.PublicKey, k.PrivateKey)
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = append(tlsCfg.Certificates, cert)
	}
	return tls.NewListener(listener, tlsCfg), nil
}

func tlsEnabled(cfg config.ServiceConfig) bool {
	if cfg.TLS == nil {
		return false
	}
	if cfg.TLS.PublicKey != "" && cfg.TLS.PrivateKey != "" {
		return true
	}
	return len(cfg.TLS.Keys) > 0
}

func listenAddr(cfg config.ServiceConfig) string {
	addr := cfg.Address
	if addr == "" {
		addr = "0.0.0.0"
	}
	return fmt.Sprintf("%s:%d", addr, cfg.Port)
}
