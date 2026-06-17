package server

import (
	"encoding/json"
	"io"

	"github.com/velonetics/lura/v2/encoding"
	"github.com/velonetics/lura/v2/proxy"
	grpcconfig "github.com/velonetics/velonetics-grpc/v2/config"
	"github.com/velonetics/velonetics-grpc/v2/catalog"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func fillResponse(out proto.Message, resp *proxy.Response, registry *catalog.Registry, pub grpcconfig.MethodPublishConfig) error {
	if resp == nil {
		return nil
	}
	if closer, ok := resp.Io.(io.Closer); ok {
		defer closer.Close()
	}
	if len(resp.Data) > 0 {
		raw, err := json.Marshal(resp.Data)
		if err != nil {
			return err
		}
		return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, out)
	}
	if resp.Io == nil {
		return nil
	}
	data, err := io.ReadAll(resp.Io)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	if backendUsesNoOp(pub) {
		return proto.Unmarshal(data, out)
	}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, out)
}

// TestFillResponse exposes fillResponse for unit tests in the server package.
func TestFillResponse(out proto.Message, resp *proxy.Response, registry *catalog.Registry, pub grpcconfig.MethodPublishConfig) error {
	return fillResponse(out, resp, registry, pub)
}

func backendUsesNoOp(pub grpcconfig.MethodPublishConfig) bool {
	for _, b := range pub.Backends {
		if b.Encoding == encoding.NOOP {
			return true
		}
	}
	return false
}
