package gateway_test

import (
	"testing"

	"github.com/gopherex/synclog/pkg/gateway"
)

func TestStaticCodecRegistry(t *testing.T) {
	registry := gateway.NewStaticCodecRegistry(
		gateway.CodecRegistryEntry{PayloadType: "project.event", PayloadVersion: 1},
	)
	if !registry.CanDecode("project.event", 1) {
		t.Fatal("registered codec was not found")
	}
	if registry.CanDecode("project.event", 2) {
		t.Fatal("unexpected codec match for unregistered version")
	}

	registry.Register("project.event", 2)
	if !registry.CanDecode("project.event", 2) {
		t.Fatal("dynamically registered codec was not found")
	}
}
