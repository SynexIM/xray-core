package limit

import (
	"context"
	featurelimit "github.com/xtls/xray-core/features/limit"
	"testing"
	"time"
)

func TestAttachDoesNotWaitForManagementLock(t *testing.T) {
	runtime := testRuntime(t, []*Profile{{Name: "p"}}, []*Rule{{RuleTag: "one", Profile: "p", User: []string{"identity"}}})
	runtime.mu.Lock()
	result := make(chan error, 1)
	go func() {
		_, _, release, err := runtime.Attach(context.Background(), featurelimit.SessionInfo{Email: "identity"})
		if release != nil {
			release()
		}
		result <- err
	}()
	select {
	case err := <-result:
		runtime.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		runtime.mu.Unlock()
		t.Fatal("Attach 等待管理表锁")
	}
}
func TestActiveSetAndOrderedPageSnapshot(t *testing.T) {
	runtime := testRuntime(t, []*Profile{{Name: "p"}}, []*Rule{{RuleTag: "z", Profile: "p", User: []string{"z"}}, {RuleTag: "a", Profile: "p", User: []string{"a"}}, {RuleTag: "m", Profile: "p", User: []string{"m"}}})
	if len(runtime.activeBuckets()) != 0 {
		t.Fatal("未使用的桶进入活跃集")
	}
	_, _, release, err := runtime.Attach(context.Background(), featurelimit.SessionInfo{Email: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.activeBuckets()) != 1 {
		t.Fatal("活跃桶数量不符")
	}
	page, total := runtime.ListRules(1, 1)
	if total != 3 || len(page) != 1 || page[0].RuleTag != "m" {
		t.Fatalf("分页错误: %v %d", page, total)
	}
	page[0].Profile = "changed"
	page, _ = runtime.ListRules(1, 1)
	if page[0].Profile != "p" {
		t.Fatal("分页调用方改坏快照")
	}
	release()
	runtime.tick(time.Now().Add(time.Second))
	if len(runtime.activeBuckets()) != 0 {
		t.Fatal("回满的闲置桶未退出活跃集")
	}
}
