package config

import (
	"errors"
	"fmt"

	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/encoding"
	"github.com/pucora/pucora-grpc/v2/catalog"
)

// BackendNamespace is the extra_config key for backend/grpc settings.
const BackendNamespace = "github.com/pucora/pucora-grpc/v2/client"

// BackendNamespaceAlias is the short config alias for backend/grpc.
const BackendNamespaceAlias = "backend/grpc"

// ParseBackendConfigFromBackend reads backend/grpc settings, accepting short and long namespace keys.
func ParseBackendConfigFromBackend(remote *config.Backend) (*BackendConfig, error) {
	if cfg, err := ParseBackendConfig(remote, BackendNamespace); err == nil {
		return cfg, nil
	} else if !errors.Is(err, ErrNoConfig) {
		return nil, err
	}
	return ParseBackendConfig(remote, BackendNamespaceAlias)
}

// IsPassthroughMethod reports whether a published method can forward protobuf without JSON conversion.
func IsPassthroughMethod(pub MethodPublishConfig, registry *catalog.Registry) bool {
	if len(pub.Backends) != 1 || registry == nil {
		return false
	}
	backend := pub.Backends[0]
	if _, err := ParseBackendConfigFromBackend(backend); err != nil {
		return false
	}
	if _, err := registry.LookupMethod(backend.URLPattern); err != nil {
		return false
	}
	return true
}

// ValidateEndpoints rejects REST endpoints that use gRPC backends with no-op output encoding.
func ValidateEndpoints(endpoints []*config.EndpointConfig) error {
	for _, ep := range endpoints {
		if ep.OutputEncoding != encoding.NOOP {
			continue
		}
		for _, b := range ep.Backend {
			_, err := ParseBackendConfigFromBackend(b)
			if err == nil {
				return fmt.Errorf("grpc: endpoint %s cannot use output_encoding no-op with backend/grpc", ep.Endpoint)
			}
			if err != nil && !errors.Is(err, ErrNoConfig) {
				return err
			}
		}
	}
	return nil
}
