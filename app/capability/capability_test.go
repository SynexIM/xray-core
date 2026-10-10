package capability

import (
	"reflect"
	"sync"
	"testing"

	"github.com/xtls/xray-core/core"
)

func TestRegistry(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); Register("z"); Register("a"); List() }()
	}
	wg.Wait()
	if got := List(); !reflect.DeepEqual(got, []string{"a", "z"}) {
		t.Fatalf("capabilities: %v", got)
	}
	names := List()
	names[0] = "changed"
	if List()[0] != "a" {
		t.Fatal("List leaked registry state")
	}
}

func TestVersion(t *testing.T) {
	old := Release
	t.Cleanup(func() { Release = old })
	Release = ""
	if Version() != core.Version() {
		t.Fatal(Version())
	}
	Release = "synexim.1"
	if Version() != core.Version()+"-synexim.1" {
		t.Fatal(Version())
	}
}
