package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	grpcconfig "github.com/pucora/pucora-grpc/v2/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type connEntry struct {
	conn     *grpc.ClientConn
	lastUsed time.Time
}

type connPool struct {
	mu      sync.Mutex
	entries map[string]*connEntry
	idleTTL time.Duration
	opts    []grpc.DialOption
}

func newConnPool(cfg *grpcconfig.BackendConfig) (*connPool, error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxCallRecvMsgSize)),
	}
	if cfg.ReadBufferSize > 0 {
		opts = append(opts, grpc.WithReadBufferSize(cfg.ReadBufferSize))
	}
	if cfg.ClientTLS != nil {
		tlsCfg, err := buildTLS(cfg.ClientTLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return &connPool{
		entries: map[string]*connEntry{},
		idleTTL: cfg.IdleConnDisconnectTime,
		opts:    opts,
	}, nil
}

func (p *connPool) get(ctx context.Context, host string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[host]; ok {
		state := entry.conn.GetState()
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			_ = entry.conn.Close()
			delete(p.entries, host)
		} else if p.idleTTL <= 0 || time.Since(entry.lastUsed) < p.idleTTL {
			entry.lastUsed = time.Now()
			return entry.conn, nil
		} else {
			_ = entry.conn.Close()
			delete(p.entries, host)
		}
	}
	conn, err := grpc.NewClient(host, p.opts...)
	if err != nil {
		return nil, err
	}
	p.entries[host] = &connEntry{conn: conn, lastUsed: time.Now()}
	return conn, nil
}

func (p *connPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for host, entry := range p.entries {
		_ = entry.conn.Close()
		delete(p.entries, host)
	}
}

func buildTLS(raw map[string]interface{}) (*tls.Config, error) {
	cfg := &tls.Config{}
	if v, ok := raw["allow_insecure_connections"].(bool); ok {
		cfg.InsecureSkipVerify = v
	}
	if v, ok := raw["disable_system_ca_pool"].(bool); ok && v {
		cfg.RootCAs = x509.NewCertPool()
	} else {
		cfg.RootCAs, _ = x509.SystemCertPool()
		if cfg.RootCAs == nil {
			cfg.RootCAs = x509.NewCertPool()
		}
	}
	if ca, ok := raw["ca_certs"].([]interface{}); ok {
		for _, item := range ca {
			path, ok := item.(string)
			if !ok {
				continue
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			cfg.RootCAs.AppendCertsFromPEM(data)
		}
	}
	if cert, ok := raw["client_cert"].(string); ok {
		key, _ := raw["client_key"].(string)
		if cert != "" && key != "" {
			tlsCert, err := tls.LoadX509KeyPair(cert, key)
			if err != nil {
				return nil, err
			}
			cfg.Certificates = []tls.Certificate{tlsCert}
		}
	}
	if sn, ok := raw["server_name"].(string); ok {
		cfg.ServerName = sn
	}
	if cfg.RootCAs == nil {
		return nil, fmt.Errorf("grpc client tls: unable to load CA pool")
	}
	return cfg, nil
}

func pickHost(hosts []string, idx int) string {
	if len(hosts) == 0 {
		return ""
	}
	return cleanHost(hosts[idx%len(hosts)])
}

func cleanHost(host string) string {
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	return host
}

func invokeWithHosts(ctx context.Context, pool *connPool, hosts []string, method string, req, resp interface{}, md map[string]string, allowRetry bool) error {
	var lastErr error
	attempts := 1
	if allowRetry && len(hosts) > 1 {
		attempts = len(hosts)
	}
	for i := 0; i < attempts; i++ {
		host := pickHost(hosts, i)
		conn, err := pool.get(ctx, host)
		if err != nil {
			lastErr = err
			continue
		}
		if allowRetry {
			state := conn.GetState()
			if state == connectivity.TransientFailure || state == connectivity.Shutdown {
				lastErr = fmt.Errorf("connection in bad state: %s", state)
				continue
			}
		}
		callCtx := ctx
		if len(md) > 0 {
			pairs := make([]string, 0, len(md)*2)
			for k, v := range md {
				pairs = append(pairs, k, v)
			}
			callCtx = metadataAppend(ctx, pairs...)
		}
		lastErr = conn.Invoke(callCtx, method, req, resp)
		if lastErr == nil {
			return nil
		}
	}
	return lastErr
}

func metadataAppend(ctx context.Context, pairs ...string) context.Context {
	md := metadata.Pairs(pairs...)
	return metadata.NewOutgoingContext(ctx, md)
}
