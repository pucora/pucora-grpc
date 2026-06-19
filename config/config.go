package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/pucora/lura/v2/config"
)

const (
	// ServiceNamespace is the extra_config key for service-level gRPC settings.
	ServiceNamespace = "github.com/pucora/pucora-grpc/v2"
)

var (
	ErrNoConfig  = errors.New("grpc: no extra config defined")
	ErrBadConfig = errors.New("grpc: unable to parse extra config")
)

// ServiceConfig holds parsed service-level grpc settings.
type ServiceConfig struct {
	Catalog []string
	Server  *ServerConfig
}

// ServerConfig holds grpc.server settings.
type ServerConfig struct {
	Services      []ServicePublishConfig
	DisableMetrics bool
	DisableTraces  bool
}

// ServicePublishConfig is a published gRPC service.
type ServicePublishConfig struct {
	Name    string
	Methods []MethodPublishConfig
}

// MethodPublishConfig is a published gRPC method with backends.
type MethodPublishConfig struct {
	Name          string
	InputHeaders  []string
	PayloadParams map[string]string
	Backends      []*config.Backend
	ExtraConfig   config.ExtraConfig
}

// ParseServiceConfig reads extra_config.grpc from service config.
func ParseServiceConfig(extra config.ExtraConfig) (*ServiceConfig, error) {
	v, ok := extra[ServiceNamespace]
	if !ok {
		return nil, ErrNoConfig
	}
	raw, ok := v.(map[string]interface{})
	if !ok {
		return nil, ErrBadConfig
	}
	cfg := &ServiceConfig{}
	if catalog, ok := raw["catalog"].([]interface{}); ok {
		for _, item := range catalog {
			if s, ok := item.(string); ok && s != "" {
				cfg.Catalog = append(cfg.Catalog, s)
			}
		}
	}
	if len(cfg.Catalog) == 0 {
		return nil, fmt.Errorf("grpc: catalog is required")
	}
	if serverRaw, ok := raw["server"].(map[string]interface{}); ok {
		server, err := parseServerConfig(serverRaw)
		if err != nil {
			return nil, err
		}
		cfg.Server = server
	}
	return cfg, nil
}

// CatalogPaths returns catalog paths when grpc config is present.
func CatalogPaths(extra config.ExtraConfig) ([]string, error) {
	cfg, err := ParseServiceConfig(extra)
	if err != nil {
		if errors.Is(err, ErrNoConfig) {
			return nil, nil
		}
		return nil, err
	}
	return cfg.Catalog, nil
}

func parseServerConfig(raw map[string]interface{}) (*ServerConfig, error) {
	server := &ServerConfig{}
	if otel, ok := raw["opentelemetry"].(map[string]interface{}); ok {
		if v, ok := otel["disable_metrics"].(bool); ok {
			server.DisableMetrics = v
		}
		if v, ok := otel["disable_traces"].(bool); ok {
			server.DisableTraces = v
		}
	}
	servicesRaw, ok := raw["services"].([]interface{})
	if !ok {
		return server, nil
	}
	for _, item := range servicesRaw {
		svcMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		svc := ServicePublishConfig{Name: stringVal(svcMap, "name")}
		methodsRaw, _ := svcMap["methods"].([]interface{})
		for _, mItem := range methodsRaw {
			mMap, ok := mItem.(map[string]interface{})
			if !ok {
				continue
			}
			method := MethodPublishConfig{
				Name:          stringVal(mMap, "name"),
				PayloadParams: stringMap(mMap["payload_params"]),
			}
			if headers, ok := mMap["input_headers"].([]interface{}); ok {
				for _, h := range headers {
					if s, ok := h.(string); ok {
						method.InputHeaders = append(method.InputHeaders, s)
					}
				}
			}
			if ec, ok := mMap["extra_config"].(map[string]interface{}); ok {
				method.ExtraConfig = config.ExtraConfig(ec)
			}
			if backends, ok := mMap["backend"].([]interface{}); ok {
				for _, b := range backends {
					bMap, ok := b.(map[string]interface{})
					if !ok {
						continue
					}
					backend := &config.Backend{}
					if hosts, ok := bMap["host"].([]interface{}); ok {
						for _, h := range hosts {
							if s, ok := h.(string); ok {
								backend.Host = append(backend.Host, s)
							}
						}
					}
					if pattern, ok := bMap["url_pattern"].(string); ok {
						backend.URLPattern = pattern
					}
					if method, ok := bMap["method"].(string); ok {
						backend.Method = method
					}
					if encoding, ok := bMap["encoding"].(string); ok {
						backend.Encoding = encoding
					}
					if ec, ok := bMap["extra_config"].(map[string]interface{}); ok {
						backend.ExtraConfig = config.ExtraConfig(ec)
					}
					method.Backends = append(method.Backends, backend)
				}
			}
			svc.Methods = append(svc.Methods, method)
		}
		server.Services = append(server.Services, svc)
	}
	return server, nil
}

func stringVal(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func stringMap(v interface{}) map[string]string {
	raw, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, val := range raw {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// BackendConfig holds parsed backend/grpc settings.
type BackendConfig struct {
	UseRequestBody           bool
	DisableQueryParams       bool
	InputMapping             map[string]string
	HeaderMapping            map[string]string
	RequestNamingConvention  string
	ResponseNamingConvention string
	OutputTimestampAsString  bool
	OutputDurationAsString   bool
	OutputEnumAsString       bool
	OutputRemoveUnsetValues  bool
	InputAssumeBytes         bool
	MaxCallRecvMsgSize       int
	ReadBufferSize           int
	UseAlternateHostOnError  bool
	IdleConnDisconnectTime   time.Duration
	ClientTLS                map[string]interface{}
}

// ParseBackendConfig reads backend/grpc extra_config from a backend.
func ParseBackendConfig(remote *config.Backend, namespace string) (*BackendConfig, error) {
	v, ok := remote.ExtraConfig[namespace]
	if !ok {
		return nil, ErrNoConfig
	}
	raw, ok := v.(map[string]interface{})
	if !ok {
		return nil, ErrBadConfig
	}
	cfg := &BackendConfig{
		RequestNamingConvention:  "snake_case",
		ResponseNamingConvention: "snake_case",
		IdleConnDisconnectTime:   10 * time.Minute,
		MaxCallRecvMsgSize:       4 * 1024 * 1024,
		InputMapping:             stringMap(raw["input_mapping"]),
		HeaderMapping:            stringMap(raw["header_mapping"]),
	}
	if v, ok := raw["use_request_body"].(bool); ok {
		cfg.UseRequestBody = v
	}
	if v, ok := raw["disable_query_params"].(bool); ok {
		cfg.DisableQueryParams = v
	}
	if v, ok := raw["request_naming_convention"].(string); ok && v != "" {
		cfg.RequestNamingConvention = v
	}
	if v, ok := raw["response_naming_convention"].(string); ok && v != "" {
		cfg.ResponseNamingConvention = v
	}
	if v, ok := raw["output_timestamp_as_string"].(bool); ok {
		cfg.OutputTimestampAsString = v
	}
	if v, ok := raw["output_duration_as_string"].(bool); ok {
		cfg.OutputDurationAsString = v
	}
	if v, ok := raw["output_enum_as_string"].(bool); ok {
		cfg.OutputEnumAsString = v
	}
	if v, ok := raw["output_remove_unset_values"].(bool); ok {
		cfg.OutputRemoveUnsetValues = v
	}
	if v, ok := raw["input_assume_bytes"].(bool); ok {
		cfg.InputAssumeBytes = v
	}
	if v, ok := raw["use_alternate_host_on_error"].(bool); ok {
		cfg.UseAlternateHostOnError = v
	}
	if v, ok := raw["max_call_recv_msg_size"].(float64); ok && int(v) > 0 {
		cfg.MaxCallRecvMsgSize = int(v)
	}
	if v, ok := raw["max_call_recv_msg_size"].(int); ok && v > 0 {
		cfg.MaxCallRecvMsgSize = v
	}
	if v, ok := raw["read_buffer_size"].(float64); ok {
		cfg.ReadBufferSize = int(v)
	}
	if v, ok := raw["read_buffer_size"].(int); ok {
		cfg.ReadBufferSize = v
	}
	if s, ok := raw["idle_conn_disconnect_time"].(string); ok && s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, err
		}
		cfg.IdleConnDisconnectTime = d
	}
	if tls, ok := raw["client_tls"].(map[string]interface{}); ok {
		cfg.ClientTLS = tls
	}
	return cfg, nil
}
