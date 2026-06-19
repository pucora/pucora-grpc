package config

import (
	"errors"
	"fmt"

	"github.com/pucora/lura/v2/config"
	"github.com/pucora/pucora-grpc/v2/catalog"
)

// ValidateBackends ensures backend/grpc settings are valid and resolvable against the catalog.
func ValidateBackends(endpoints []*config.EndpointConfig, asyncAgents []*config.AsyncAgent, registry *catalog.Registry) error {
	for _, ep := range endpoints {
		for _, b := range ep.Backend {
			if err := validateGRPCBackend(b, registry); err != nil {
				return fmt.Errorf("endpoint %q backend %q: %w", ep.Endpoint, b.URLPattern, err)
			}
		}
	}
	for _, agent := range asyncAgents {
		for _, b := range agent.Backend {
			if err := validateGRPCBackend(b, registry); err != nil {
				return fmt.Errorf("async_agent %q backend %q: %w", agent.Name, b.URLPattern, err)
			}
		}
	}
	return nil
}

func validateGRPCBackend(b *config.Backend, registry *catalog.Registry) error {
	if b == nil {
		return nil
	}
	cfg, err := ParseBackendConfigFromBackend(b)
	if errors.Is(err, ErrNoConfig) {
		return nil
	}
	if err != nil {
		return err
	}
	_ = cfg
	if registry == nil {
		return fmt.Errorf("grpc catalog not loaded")
	}
	if _, err := registry.LookupMethod(b.URLPattern); err != nil {
		return err
	}
	return nil
}
