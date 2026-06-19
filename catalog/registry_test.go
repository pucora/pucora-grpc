package catalog_test

import (
	"path/filepath"
	"testing"

	"github.com/pucora/lura/v2/logging"
	"github.com/pucora/pucora-grpc/v2/catalog"
)

func TestRegistryLoadAndLookup(t *testing.T) {
	pb := filepath.Join("..", "testdata", "contracts", "flights.pb")
	reg := catalog.NewRegistry()
	if err := reg.Load([]string{pb}, logging.NoOp); err != nil {
		t.Fatal(err)
	}
	method, err := reg.LookupMethod("/flight_finder.Flights/FindFlight")
	if err != nil {
		t.Fatal(err)
	}
	if method.Name() != "FindFlight" {
		t.Fatalf("unexpected method %s", method.Name())
	}
}

func TestRegistryLoadDirectory(t *testing.T) {
	dir := filepath.Join("..", "testdata", "contracts")
	reg := catalog.NewRegistry()
	if err := reg.Load([]string{dir}, logging.NoOp); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.LookupService("flight_finder.Flights"); err != nil {
		t.Fatal(err)
	}
}
