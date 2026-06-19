package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"github.com/pucora/lura/v2/proxy"
	maingrpc "github.com/pucora/pucora-grpc/v2"
	"github.com/pucora/pucora-grpc/v2/codec"
	grpcconfig "github.com/pucora/pucora-grpc/v2/config"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ErrorProxy returns a proxy that always fails with the given initialization error.
func ErrorProxy(err error) proxy.Proxy {
	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		return nil, err
	}
}

// BackendFactory returns a proxy.BackendFactory that handles backend/grpc backends.
func BackendFactory(logger logging.Logger, bf proxy.BackendFactory) proxy.BackendFactory {
	return func(remote *config.Backend) proxy.Proxy {
		logPrefix := "[BACKEND: " + remote.URLPattern + "][gRPC]"
		cfg, err := grpcconfig.ParseBackendConfigFromBackend(remote)
		if errors.Is(err, grpcconfig.ErrNoConfig) {
			return bf(remote)
		}
		if err != nil {
			logger.Error(logPrefix, err)
			return ErrorProxy(err)
		}
		registry := maingrpc.GlobalRegistry()
		if registry == nil {
			err := fmt.Errorf("grpc catalog not loaded")
			logger.Error(logPrefix, err)
			return ErrorProxy(err)
		}
		method, err := registry.LookupMethod(remote.URLPattern)
		if err != nil {
			logger.Error(logPrefix, err)
			return ErrorProxy(err)
		}
		pool, err := newConnPool(cfg)
		if err != nil {
			logger.Error(logPrefix, err)
			return ErrorProxy(err)
		}
		logger.Debug(logPrefix, "Component enabled")
		fullMethod := ensureLeadingSlash(remote.URLPattern)

		return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
			reqMsg, err := codec.BuildRequestMessage(r, method, cfg)
			if err != nil {
				return nil, err
			}
			respMsg := dynamicpb.NewMessage(method.Output())
			md := codec.BuildMetadata(r, cfg.HeaderMapping)
			err = invokeWithHosts(ctx, pool, remote.Host, fullMethod, reqMsg, respMsg, md, cfg.UseAlternateHostOnError)
			if err != nil {
				return nil, err
			}
			body, err := codec.MessageToJSON(respMsg, cfg)
			if err != nil {
				return nil, err
			}
			var data map[string]interface{}
			if err := json.Unmarshal(body, &data); err != nil {
				return nil, err
			}
			return &proxy.Response{
				Data:       data,
				IsComplete: true,
				Metadata: proxy.Metadata{
					StatusCode: 200,
					Headers: map[string][]string{
						"Content-Type": {"application/json"},
					},
				},
			}, nil
		}
	}
}

// InvokeProto performs a unary gRPC call with raw protobuf messages (passthrough mode).
func InvokeProto(ctx context.Context, remote *config.Backend, cfg *grpcconfig.BackendConfig, req, resp proto.Message, md map[string]string) error {
	registry := maingrpc.GlobalRegistry()
	if registry == nil {
		return fmt.Errorf("grpc catalog not loaded")
	}
	method, err := registry.LookupMethod(remote.URLPattern)
	if err != nil {
		return err
	}
	if req == nil {
		req = dynamicpb.NewMessage(method.Input())
	}
	if resp == nil {
		resp = dynamicpb.NewMessage(method.Output())
	}
	pool, err := newConnPool(cfg)
	if err != nil {
		return err
	}
	defer pool.close()
	return invokeWithHosts(ctx, pool, remote.Host, ensureLeadingSlash(remote.URLPattern), req, resp, md, cfg.UseAlternateHostOnError)
}

func ensureLeadingSlash(method string) string {
	if strings.HasPrefix(method, "/") {
		return method
	}
	return "/" + method
}
