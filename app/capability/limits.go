package capability

func RegisterLimit(name string, maximum uint64) {
	registry.Lock()
	registry.names[name] = struct{}{}
	registry.limits[name] = maximum
	registry.Unlock()
}
func Limits() map[string]uint64 {
	registry.RLock()
	defer registry.RUnlock()
	result := make(map[string]uint64, len(registry.limits))
	for name, maximum := range registry.limits {
		result[name] = maximum
	}
	return result
}
