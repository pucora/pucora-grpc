package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"github.com/pucora/lura/v2/proxy"
	router "github.com/pucora/lura/v2/router/gin"
	pucorajose "github.com/pucora/pucora-jose/v2"
	"github.com/pucora/pucora-grpc/v2/catalog"
	"github.com/pucora/pucora-grpc/v2/client"
	grpcconfig "github.com/pucora/pucora-grpc/v2/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// RunServer wraps the HTTP server to multiplex gRPC on the same port when grpc.server is configured.
func RunServer(
	logger logging.Logger,
	registry *catalog.Registry,
	serviceCfg *grpcconfig.ServiceConfig,
	pf proxy.Factory,
	rejecterF pucorajose.RejecterFactory,
	next router.RunServerFunc,
) router.RunServerFunc {
	if serviceCfg == nil || serviceCfg.Server == nil || len(serviceCfg.Server.Services) == 0 {
		return next
	}
	return func(ctx context.Context, cfg config.ServiceConfig, handler http.Handler) error {
		listener, err := net.Listen("tcp", listenAddr(cfg))
		if err != nil {
			return err
		}
		listener, err = wrapTLS(listener, cfg)
		if err != nil {
			return err
		}

		opts := grpcServerOptions(serviceCfg.Server)
		grpcServer := grpc.NewServer(opts...)
		reflection.Register(grpcServer)
		if err := registerServices(grpcServer, registry, serviceCfg, pf, logger, rejecterF); err != nil {
			return err
		}

		mux, grpcListener, httpListener := multiplex(listener, cfg)
		httpServer := &http.Server{Handler: handler}
		var wg sync.WaitGroup
		var shutdownOnce sync.Once
		errCh := make(chan error, 3)

		shutdown := func() {
			shutdownOnce.Do(func() {
				grpcServer.GracefulStop()
				_ = httpServer.Shutdown(context.Background())
				_ = listener.Close()
			})
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := grpcServer.Serve(grpcListener); err != nil {
				errCh <- err
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpServer.Serve(httpListener); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := mux.Serve(); err != nil {
				errCh <- err
			}
		}()

		go func() {
			<-ctx.Done()
			shutdown()
		}()

		logger.Info("[SERVICE: gRPC]", "serving gRPC on", listenAddr(cfg))
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case err := <-errCh:
			shutdown()
			wg.Wait()
			return err
		}
	}
}

func registerServices(
	grpcServer *grpc.Server,
	registry *catalog.Registry,
	serviceCfg *grpcconfig.ServiceConfig,
	pf proxy.Factory,
	logger logging.Logger,
	rejecterF pucorajose.RejecterFactory,
) error {
	for _, svc := range serviceCfg.Server.Services {
		desc, err := registry.LookupService(svc.Name)
		if err != nil {
			return err
		}
		methods := map[string]grpcconfig.MethodPublishConfig{}
		for _, m := range svc.Methods {
			methods[m.Name] = m
		}
		sd := &grpc.ServiceDesc{
			ServiceName: svc.Name,
			Methods:     []grpc.MethodDesc{},
			Streams:     []grpc.StreamDesc{},
		}
		for i := 0; i < desc.Methods().Len(); i++ {
			methodDesc := desc.Methods().Get(i)
			pub, ok := methods[string(methodDesc.Name())]
			if !ok {
				continue
			}
			methodName := string(methodDesc.Name())
			sd.Methods = append(sd.Methods, grpc.MethodDesc{
				MethodName: methodName,
				Handler:    makeUnaryHandler(registry, methodDesc, pub, pf, logger, rejecterF),
			})
		}
		if len(sd.Methods) == 0 {
			continue
		}
		grpcServer.RegisterService(sd, nil)
	}
	return nil
}

func makeUnaryHandler(
	registry *catalog.Registry,
	methodDesc protoreflect.MethodDescriptor,
	pub grpcconfig.MethodPublishConfig,
	pf proxy.Factory,
	logger logging.Logger,
	rejecterF pucorajose.RejecterFactory,
) func(interface{}, context.Context, func(interface{}) error, grpc.UnaryServerInterceptor) (interface{}, error) {
	methodAuth, authErr := buildMethodAuth(logger, rejecterF, pub.ExtraConfig)
	return func(_ interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
		if authErr != nil {
			return nil, status.Errorf(codes.Internal, "jwt config: %v", authErr)
		}
		if err := methodAuth.validate(ctx); err != nil {
			return nil, err
		}
		in := dynamicpb.NewMessage(methodDesc.Input())
		if err := dec(in); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "decode request: %v", err)
		}
		out := dynamicpb.NewMessage(methodDesc.Output())
		if grpcconfig.IsPassthroughMethod(pub, registry) {
			backend := pub.Backends[0]
			cfg, err := grpcconfig.ParseBackendConfigFromBackend(backend)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "backend config: %v", err)
			}
			md := metadataFromContext(ctx, pub)
			if err := client.InvokeProto(ctx, backend, cfg, in, out, md); err != nil {
				return nil, status.Errorf(codes.Internal, "passthrough: %v", err)
			}
			return out, nil
		}

		req, err := buildProxyRequest(ctx, in, pub)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "build request: %v", err)
		}
		proxyPipe, err := buildMethodProxy(pub, pf, logger)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "proxy: %v", err)
		}
		resp, err := proxyPipe(ctx, req)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "backend: %v", err)
		}
		if err := fillResponse(out, resp, registry, pub); err != nil {
			return nil, status.Errorf(codes.Internal, "response: %v", err)
		}
		return out, nil
	}
}

func metadataFromContext(ctx context.Context, pub grpcconfig.MethodPublishConfig) map[string]string {
	out := map[string]string{}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return out
	}
	for _, allowed := range pub.InputHeaders {
		if allowed == "*" {
			for k, vals := range md {
				if len(vals) > 0 {
					out[strings.ToLower(k)] = vals[0]
				}
			}
			return out
		}
		if vals := md.Get(strings.ToLower(allowed)); len(vals) > 0 {
			out[strings.ToLower(allowed)] = vals[0]
		}
	}
	return out
}

func buildProxyRequest(ctx context.Context, in proto.Message, pub grpcconfig.MethodPublishConfig) (*proxy.Request, error) {
	raw, err := protojson.Marshal(in)
	if err != nil {
		return nil, err
	}
	params := map[string]string{}
	for field, placeholder := range pub.PayloadParams {
		if v := extractField(in, field); v != "" {
			params[placeholder] = v
		}
	}
	headers := map[string][]string{}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, allowed := range pub.InputHeaders {
			if allowed == "*" {
				for k, vals := range md {
					headers[http.CanonicalHeaderKey(k)] = vals
				}
				break
			}
			if vals := md.Get(strings.ToLower(allowed)); len(vals) > 0 {
				headers[http.CanonicalHeaderKey(allowed)] = vals
			}
		}
	}
	return &proxy.Request{
		Method:  http.MethodPost,
		Body:    io.NopCloser(strings.NewReader(string(raw))),
		Params:  params,
		Headers: headers,
	}, nil
}

func extractField(msg proto.Message, dotPath string) string {
	parts := strings.Split(dotPath, ".")
	cur := msg.ProtoReflect()
	for i, part := range parts {
		fd := cur.Descriptor().Fields().ByName(protoreflect.Name(part))
		if fd == nil {
			return ""
		}
		if i == len(parts)-1 {
			return cur.Get(fd).String()
		}
		if fd.Kind() != protoreflect.MessageKind {
			return ""
		}
		cur = cur.Get(fd).Message()
	}
	return ""
}

func buildMethodProxy(pub grpcconfig.MethodPublishConfig, pf proxy.Factory, logger logging.Logger) (proxy.Proxy, error) {
	if len(pub.Backends) == 0 {
		return nil, fmt.Errorf("no backends configured")
	}
	endpoint := &config.EndpointConfig{
		Endpoint: fmt.Sprintf("/grpc/%s", pub.Name),
		Backend:  pub.Backends,
	}
	return pf.New(endpoint)
}
