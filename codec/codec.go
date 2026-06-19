package codec

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/pucora/lura/v2/proxy"
	grpcconfig "github.com/pucora/pucora-grpc/v2/config"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BuildRequestMessage constructs a protobuf request message from a proxy request.
func BuildRequestMessage(r *proxy.Request, method protoreflect.MethodDescriptor, cfg *grpcconfig.BackendConfig) (proto.Message, error) {
	msg := dynamicpb.NewMessage(method.Input())
	data := map[string]interface{}{}

	if !cfg.DisableQueryParams {
		for k, values := range r.Query {
			if len(values) == 0 {
				continue
			}
			key := applyInputMapping(k, cfg.InputMapping)
			setFlatValue(data, key, expandRepeated(key, values))
		}
		for k, v := range r.Params {
			key := applyInputMapping(capitalizeFirst(k), cfg.InputMapping)
			setFlatValue(data, key, v)
		}
	}

	if cfg.UseRequestBody && r.Body != nil {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if len(body) > 0 {
			var bodyMap map[string]interface{}
			if err := json.Unmarshal(body, &bodyMap); err != nil {
				return nil, err
			}
			mergeMaps(data, bodyMap)
		}
	}

	if len(data) == 0 {
		return msg, nil
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if cfg.RequestNamingConvention == "snake_case" {
		opts.DiscardUnknown = true
	}
	if err := opts.Unmarshal(jsonBytes, msg); err != nil {
		return nil, fmt.Errorf("grpc request decode: %w", err)
	}
	if cfg.InputAssumeBytes {
		if err := assumeBytesFields(msg, msg.ProtoReflect().Descriptor()); err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// MessageToJSON serializes a protobuf message to JSON bytes.
func MessageToJSON(msg proto.Message, cfg *grpcconfig.BackendConfig) ([]byte, error) {
	if cfg.OutputTimestampAsString || cfg.OutputDurationAsString || cfg.OutputEnumAsString {
		converted, err := transformWellKnown(msg, cfg)
		if err != nil {
			return nil, err
		}
		return json.Marshal(converted)
	}
	opts := protojson.MarshalOptions{
		UseProtoNames:   cfg.ResponseNamingConvention == "snake_case",
		EmitUnpopulated: !cfg.OutputRemoveUnsetValues,
	}
	return opts.Marshal(msg)
}

// BuildMetadata maps HTTP headers to gRPC metadata keys.
func BuildMetadata(r *proxy.Request, mapping map[string]string) map[string]string {
	out := map[string]string{}
	for key, values := range r.Headers {
		if len(values) == 0 {
			continue
		}
		target := key
		if mapped, ok := mapping[key]; ok {
			target = mapped
		} else if mapped, ok := mapping[strings.ToLower(key)]; ok {
			target = mapped
		}
		if strings.HasPrefix(strings.ToLower(target), "grpc") {
			target = "in-grpc-" + strings.TrimPrefix(strings.ToLower(target), "grpc")
		}
		out[strings.ToLower(target)] = values[0]
	}
	return out
}

// JSONToMessage unmarshals JSON into a protobuf message.
func JSONToMessage(data []byte, method protoreflect.MethodDescriptor) (proto.Message, error) {
	msg := dynamicpb.NewMessage(method.Input())
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func applyInputMapping(key string, mapping map[string]string) string {
	if mapped, ok := mapping[key]; ok {
		return mapped
	}
	return key
}

func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func setFlatValue(data map[string]interface{}, key string, value interface{}) {
	parts := strings.Split(key, ".")
	cur := data
	for i, part := range parts {
		if i == len(parts)-1 {
			cur[part] = value
			return
		}
		next, ok := cur[part].(map[string]interface{})
		if !ok {
			next = map[string]interface{}{}
			cur[part] = next
		}
		cur = next
	}
}

func mergeMaps(dst, src map[string]interface{}) {
	for k, v := range src {
		if existing, ok := dst[k].(map[string]interface{}); ok {
			if incoming, ok := v.(map[string]interface{}); ok {
				mergeMaps(existing, incoming)
				continue
			}
		}
		dst[k] = v
	}
}

func expandRepeated(_ string, values []string) interface{} {
	if len(values) == 1 {
		return parseScalar(values[0])
	}
	out := make([]interface{}, len(values))
	for i, v := range values {
		out[i] = parseScalar(v)
	}
	return out
}

func parseScalar(v string) interface{} {
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return v
}

func assumeBytesFields(msg proto.Message, desc protoreflect.MessageDescriptor) error {
	return walkMessage(msg.ProtoReflect(), desc, func(fd protoreflect.FieldDescriptor, val protoreflect.Value) error {
		if fd.Kind() == protoreflect.BytesKind && !fd.IsList() {
			if decoded, err := base64.StdEncoding.DecodeString(string(val.Bytes())); err == nil {
				msg.ProtoReflect().Set(fd, protoreflect.ValueOfBytes(decoded))
			}
		}
		return nil
	})
}

func transformWellKnown(msg proto.Message, cfg *grpcconfig.BackendConfig) (map[string]interface{}, error) {
	opts := protojson.MarshalOptions{UseProtoNames: cfg.ResponseNamingConvention == "snake_case"}
	raw, err := opts.Marshal(msg)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return transformMap(msg.ProtoReflect(), out, cfg), nil
}

func transformMap(pr protoreflect.Message, data map[string]interface{}, cfg *grpcconfig.BackendConfig) map[string]interface{} {
	desc := pr.Descriptor()
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := fd.JSONName()
		if cfg.ResponseNamingConvention == "snake_case" {
			name = string(fd.Name())
		}
		val, ok := data[name]
		if !ok {
			continue
		}
		switch {
		case cfg.OutputEnumAsString && fd.Kind() == protoreflect.EnumKind:
			if enumVal := pr.Get(fd).Enum(); enumVal != 0 {
				data[name] = fd.Enum().Values().ByNumber(enumVal).Name()
			}
		case cfg.OutputTimestampAsString && fd.Message() != nil && fd.Message().FullName() == "google.protobuf.Timestamp":
			ts := pr.Get(fd).Message().Interface().(*timestamppb.Timestamp)
			data[name] = ts.AsTime().Format("2006-01-02T15:04:05Z07:00")
		case cfg.OutputDurationAsString && fd.Message() != nil && fd.Message().FullName() == "google.protobuf.Duration":
			d := pr.Get(fd).Message().Interface().(*durationpb.Duration)
			data[name] = fmt.Sprintf("%gs", d.AsDuration().Seconds())
		case fd.Kind() == protoreflect.MessageKind:
			if child, ok := val.(map[string]interface{}); ok && pr.Has(fd) {
				data[name] = transformMap(pr.Get(fd).Message(), child, cfg)
			}
		}
	}
	if cfg.OutputRemoveUnsetValues {
		for k, v := range data {
			if isZeroJSON(v) {
				delete(data, k)
			}
		}
	}
	return data
}

func isZeroJSON(v interface{}) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case float64:
		return t == 0
	case bool:
		return !t
	case []interface{}:
		return len(t) == 0
	case map[string]interface{}:
		return len(t) == 0
	default:
		return false
	}
}

func walkMessage(pr protoreflect.Message, desc protoreflect.MessageDescriptor, fn func(protoreflect.FieldDescriptor, protoreflect.Value) error) error {
	fields := desc.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !pr.Has(fd) {
			continue
		}
		val := pr.Get(fd)
		if err := fn(fd, val); err != nil {
			return err
		}
		if fd.Kind() == protoreflect.MessageKind && fd.Message() != nil {
			if fd.IsList() {
				list := val.List()
				for j := 0; j < list.Len(); j++ {
					if err := walkMessage(list.Get(j).Message(), fd.Message(), fn); err != nil {
						return err
					}
				}
			} else if err := walkMessage(val.Message(), fd.Message(), fn); err != nil {
				return err
			}
		}
	}
	return nil
}
