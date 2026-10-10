// Package capability records the capabilities provided by linked modules.
package capability

import (
	"sort"
	"sync"

	"github.com/xtls/xray-core/core"
)

// Release is the fork suffix, injected with -ldflags '-X github.com/xtls/xray-core/app/capability.Release=synexim.1'.
var Release string

var registry = struct {
	sync.RWMutex
	names map[string]struct{}
}{names: make(map[string]struct{})}

// Register declares a capability; repeated declarations are harmless.
func Register(name string) {
	registry.Lock()
	registry.names[name] = struct{}{}
	registry.Unlock()
}

// List returns a sorted, detached snapshot of registered capabilities.
func List() []string {
	registry.RLock()
	names := make([]string, 0, len(registry.names))
	for name := range registry.names {
		names = append(names, name)
	}
	registry.RUnlock()
	sort.Strings(names)
	return names
}

// Version returns the upstream version with the optional fork release suffix.
func Version() string {
	if Release == "" {
		return core.Version()
	}
	return core.Version() + "-" + Release
}
