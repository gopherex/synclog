package gateway

import "sync"

type CodecRegistryEntry struct {
	PayloadType    string
	PayloadVersion uint32
}

type StaticCodecRegistry struct {
	mu      sync.RWMutex
	entries map[string]map[uint32]bool
}

func NewStaticCodecRegistry(entries ...CodecRegistryEntry) *StaticCodecRegistry {
	registry := &StaticCodecRegistry{entries: make(map[string]map[uint32]bool)}
	for _, entry := range entries {
		registry.Register(entry.PayloadType, entry.PayloadVersion)
	}
	return registry
}

func (r *StaticCodecRegistry) Register(payloadType string, payloadVersion uint32) {
	if payloadType == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	versions := r.entries[payloadType]
	if versions == nil {
		versions = make(map[uint32]bool)
		r.entries[payloadType] = versions
	}
	versions[payloadVersion] = true
}

func (r *StaticCodecRegistry) CanDecode(payloadType string, payloadVersion uint32) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.entries[payloadType][payloadVersion]
}
