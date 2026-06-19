package grpc

import (
	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"github.com/pucora/pucora-grpc/v2/catalog"
	grpcconfig "github.com/pucora/pucora-grpc/v2/config"
)

var globalRegistry = struct {
	v *catalog.Registry
}{}

// SetGlobalRegistry stores the catalog registry for backend and server components.
func SetGlobalRegistry(r *catalog.Registry) {
	globalRegistry.v = r
}

// GlobalRegistry returns the catalog registry set at service startup.
func GlobalRegistry() *catalog.Registry {
	return globalRegistry.v
}

// Bootstrap loads grpc catalog and service config from the gateway configuration.
func Bootstrap(cfg config.ServiceConfig, logger logging.Logger) (*catalog.Registry, *grpcconfig.ServiceConfig, error) {
	svcCfg, err := grpcconfig.ParseServiceConfig(cfg.ExtraConfig)
	if err != nil {
		if err == grpcconfig.ErrNoConfig {
			if err := grpcconfig.ValidateBackends(cfg.Endpoints, cfg.AsyncAgents, nil); err != nil {
				return nil, nil, err
			}
			return nil, nil, nil
		}
		return nil, nil, err
	}
	registry := catalog.NewRegistry()
	if err := registry.Load(svcCfg.Catalog, logger); err != nil {
		return nil, nil, err
	}
	if err := grpcconfig.ValidateEndpoints(cfg.Endpoints); err != nil {
		return nil, nil, err
	}
	if err := grpcconfig.ValidateBackends(cfg.Endpoints, cfg.AsyncAgents, registry); err != nil {
		return nil, nil, err
	}
	SetGlobalRegistry(registry)
	return registry, svcCfg, nil
}
