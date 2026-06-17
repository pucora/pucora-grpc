package server_test

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/velonetics/lura/v2/config"
	"github.com/velonetics/lura/v2/encoding"
	"github.com/velonetics/lura/v2/logging"
	"github.com/velonetics/lura/v2/proxy"
	"github.com/velonetics/velonetics-grpc/v2/catalog"
	grpcconfig "github.com/velonetics/velonetics-grpc/v2/config"
	"github.com/velonetics/velonetics-grpc/v2/server"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func loadFindFlightOutput(t *testing.T) *dynamicpb.Message {
	t.Helper()
	reg := catalog.NewRegistry()
	pb := filepath.Join("..", "testdata", "contracts", "flights.pb")
	if err := reg.Load([]string{pb}, logging.NoOp); err != nil {
		t.Fatal(err)
	}
	method, err := reg.LookupMethod("/flight_finder.Flights/FindFlight")
	if err != nil {
		t.Fatal(err)
	}
	return dynamicpb.NewMessage(method.Output())
}

func appendFlight(t *testing.T, msg *dynamicpb.Message, id, dest string) {
	t.Helper()
	fd := msg.Descriptor().Fields().ByName("flights")
	flightDesc := fd.Message()
	flight := dynamicpb.NewMessage(flightDesc)
	flight.Set(flightDesc.Fields().ByName("id"), protoreflect.ValueOfString(id))
	flight.Set(flightDesc.Fields().ByName("destination"), protoreflect.ValueOfString(dest))
	list := msg.Mutable(fd).List()
	list.Append(protoreflect.ValueOfMessage(flight))
}

func TestFillResponseFromData(t *testing.T) {
	out := loadFindFlightOutput(t)
	resp := &proxy.Response{
		Data: map[string]interface{}{
			"flights": []interface{}{
				map[string]interface{}{"id": "FL-001", "destination": "NYC"},
			},
		},
	}
	if err := server.TestFillResponse(out, resp, nil, grpcconfig.MethodPublishConfig{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "FL-001") {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestFillResponseFromJSONIo(t *testing.T) {
	out := loadFindFlightOutput(t)
	raw := []byte(`{"flights":[{"id":"FL-002","destination":"LAX"}]}`)
	resp := &proxy.Response{Io: bytes.NewReader(raw)}
	if err := server.TestFillResponse(out, resp, nil, grpcconfig.MethodPublishConfig{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "FL-002") {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestFillResponseFromProtobufIo(t *testing.T) {
	out := loadFindFlightOutput(t)
	src := loadFindFlightOutput(t)
	appendFlight(t, src, "FL-003", "SFO")
	raw, err := proto.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	pub := grpcconfig.MethodPublishConfig{
		Backends: []*config.Backend{{Encoding: encoding.NOOP}},
	}
	resp := &proxy.Response{Io: bytes.NewReader(raw)}
	if err := server.TestFillResponse(out, resp, nil, pub); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "FL-003") {
		t.Fatalf("unexpected output: %s", out)
	}
}

type closeTrackReader struct {
	io.Reader
	closed bool
}

func (r *closeTrackReader) Close() error {
	r.closed = true
	return nil
}

func TestFillResponseClosesIo(t *testing.T) {
	out := loadFindFlightOutput(t)
	raw := []byte(`{"flights":[{"id":"FL-005","destination":"MIA"}]}`)
	tracked := &closeTrackReader{Reader: bytes.NewReader(raw)}
	resp := &proxy.Response{Io: tracked}
	if err := server.TestFillResponse(out, resp, nil, grpcconfig.MethodPublishConfig{}); err != nil {
		t.Fatal(err)
	}
	if !tracked.closed {
		t.Fatal("expected fillResponse to close resp.Io when it implements io.Closer")
	}
	if !strings.Contains(out.String(), "FL-005") {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestFillResponseEmpty(t *testing.T) {
	out := loadFindFlightOutput(t)
	if err := server.TestFillResponse(out, nil, nil, grpcconfig.MethodPublishConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := server.TestFillResponse(out, &proxy.Response{}, nil, grpcconfig.MethodPublishConfig{}); err != nil {
		t.Fatal(err)
	}
}

func TestFillResponseDataViaJSONMarshal(t *testing.T) {
	out := loadFindFlightOutput(t)
	data := map[string]interface{}{
		"flights": []interface{}{
			map[string]interface{}{"id": "FL-004", "destination": "ORD"},
		},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	var round map[string]interface{}
	if err := json.Unmarshal(raw, &round); err != nil {
		t.Fatal(err)
	}
	resp := &proxy.Response{Data: round}
	if err := server.TestFillResponse(out, resp, nil, grpcconfig.MethodPublishConfig{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "FL-004") {
		t.Fatalf("unexpected output: %s", out)
	}
}
