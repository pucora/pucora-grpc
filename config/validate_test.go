package config

import (
	"testing"

	"github.com/pucora/lura/v2/config"
	"github.com/pucora/pucora-grpc/v2/catalog"
)

func TestValidateBackends_noGRPC(t *testing.T) {
	if err := ValidateBackends(nil, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestValidateBackends_missingCatalog(t *testing.T) {
	err := ValidateBackends([]*config.EndpointConfig{{
		Endpoint: "/grpc",
		Backend: []*config.Backend{{
			URLPattern: "/package.Service/Method",
			ExtraConfig: config.ExtraConfig{
				BackendNamespace: map[string]interface{}{},
			},
		}},
	}}, nil, nil)
	if err == nil {
		t.Fatal("expected error when grpc catalog is missing")
	}
}

func TestValidateBackends_withCatalog(t *testing.T) {
	registry := catalog.NewRegistry()
	// Empty registry: LookupMethod will fail for unknown method.
	err := ValidateBackends([]*config.EndpointConfig{{
		Endpoint: "/grpc",
		Backend: []*config.Backend{{
			URLPattern: "/package.Service/Method",
			ExtraConfig: config.ExtraConfig{
				BackendNamespace: map[string]interface{}{},
			},
		}},
	}}, nil, registry)
	if err == nil {
		t.Fatal("expected lookup error for unknown method")
	}
}
