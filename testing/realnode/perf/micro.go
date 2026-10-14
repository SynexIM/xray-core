package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/app/limit"
	"github.com/xtls/xray-core/app/router"
	appstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/uuid"
	featurelimit "github.com/xtls/xray-core/features/limit"
	featurestats "github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/proxy/vmess"
	"github.com/xtls/xray-core/proxy/vmess/aead"
)

func identity(index int) uuid.UUID {
	var value uuid.UUID
	binary.BigEndian.PutUint64(value[8:], uint64(index+1))
	return value
}

func authBenchmark() ([]measurement, error) {
	validator := vmess.NewTimedUserValidator()
	keys := make([][16]byte, 20000)
	for index := range keys {
		account := &vmess.MemoryAccount{ID: protocol.NewID(identity(index))}
		copy(keys[index][:], account.ID.CmdKey())
		if err := validator.Add(&protocol.MemoryUser{Email: fmt.Sprintf("v%d", index), Account: account}); err != nil {
			return nil, err
		}
	}
	var rows []measurement
	for _, scenario := range []string{"vmess_uniform_20000", "vmess_recent_20000", "vmess_source_cache_20000"} {
		values := make([]float64, 1000)
		for sample := range values {
			index := (sample * 7919) % len(keys)
			if scenario != "vmess_uniform_20000" {
				index = len(keys) - 1
			}
			authID := aead.CreateAuthID(keys[index][:], time.Now().Unix())
			start := time.Now()
			_, found, err := validator.GetAEAD(authID[:])
			values[sample] = float64(time.Since(start).Nanoseconds()) / 1000
			if err != nil || !found {
				return nil, fmt.Errorf("VMess 认证: %v", err)
			}
		}
		rows = append(rows, distribution(scenario, "µs", values))
	}
	vlessValidator := &vless.MemoryValidator{}
	for index := 0; index < 1000000; index++ {
		if err := vlessValidator.Add(&protocol.MemoryUser{Email: fmt.Sprintf("v%d", index), Account: &vless.MemoryAccount{ID: protocol.NewID(identity(index))}}); err != nil {
			return nil, err
		}
	}
	values := make([]float64, 20000)
	for sample := range values {
		id := identity((sample * 7919) % 1000000)
		start := time.Now()
		user := vlessValidator.Get(id)
		values[sample] = float64(time.Since(start).Nanoseconds()) / 1000
		if user == nil {
			return nil, fmt.Errorf("VLESS 缺少测试身份")
		}
	}
	return append(rows, distribution("vless_1000000", "µs", values)), nil
}

func memoryUsage() (uint64, uint64, error) {
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			kilobytes, err := strconv.ParseUint(fields[1], 10, 64)
			return memory.HeapAlloc, kilobytes * 1024, err
		}
	}
	return 0, 0, fmt.Errorf("/proc/self/status 没有 VmRSS")
}
func memoryBenchmark(count int) ([]measurement, error) {
	manager, err := appstats.NewManager(context.Background(), &appstats.Config{})
	if err != nil {
		return nil, err
	}
	validator := &vless.MemoryValidator{}
	heapBefore, rssBefore, err := memoryUsage()
	if err != nil {
		return nil, err
	}
	for index := 0; index < count; index++ {
		email := fmt.Sprintf("v%d", index)
		user := &protocol.MemoryUser{Email: email, Account: &vless.MemoryAccount{ID: protocol.NewID(identity(index))}}

		if err := validator.Add(user); err != nil {
			return nil, err
		}
		for _, direction := range []string{"uplink", "downlink"} {
			counter, err := manager.GetOrRegisterCounter("user>>>" + email + ">>>traffic>>>" + direction)
			if err != nil {
				return nil, err
			}
			counter.Add(1)
		}
		if _, err := manager.GetOrRegisterOnlineMap("user>>>" + email + ">>>online"); err != nil {
			return nil, err
		}
	}
	heapLoaded, rssLoaded, err := memoryUsage()
	if err != nil {
		return nil, err
	}
	for index := 0; index < count; index++ {
		email := fmt.Sprintf("v%d", index)
		user := validator.GetByEmail(email)
		user.RevokeRuntimeSessions()
		if err := validator.Del(email); err != nil {
			return nil, err
		}
	}
	heapDeleted, rssDeleted, err := memoryUsage()
	if err != nil {
		return nil, err
	}
	counters := 0
	manager.VisitCounters(func(_ string, _ featurestats.Counter) bool { counters++; return true })
	runtime.KeepAlive(validator)
	label := strconv.Itoa(count)
	return []measurement{scalar("heap_per_identity_"+label, "B", float64(heapLoaded-heapBefore)/float64(count)), scalar("rss_per_identity_"+label, "B", float64(rssLoaded-rssBefore)/float64(count)), scalar("heap_after_delete_"+label, "B", float64(heapDeleted)), scalar("rss_after_delete_"+label, "B", float64(rssDeleted)), scalar("counters_after_delete_"+label, "count", float64(counters))}, nil
}

func runtimeBenchmark(seconds int) ([]measurement, error) {
	rules := make([]*limit.Rule, 1000000)
	for index := range rules {
		tag := fmt.Sprintf("r%07d", index)
		rules[index] = &limit.Rule{RuleTag: tag, Profile: "p", User: []string{tag}}
	}
	runtime, err := limit.New(&limit.Config{Profiles: []*limit.Profile{{Name: "p", Up: &limit.Ladder{Steps: []*limit.Step{{RateBps: 10000000}}}}}, Rules: rules})
	if err != nil {
		return nil, err
	}
	rules = nil
	defer runtime.Close()
	for index := 0; index < 20000; index++ {
		_, _, _, err := runtime.Attach(context.Background(), featurelimit.SessionInfo{Email: fmt.Sprintf("r%07d", index)})
		if err != nil {
			return nil, err
		}
	}
	if err := runtime.Start(); err != nil {
		return nil, err
	}
	var values []float64
	var samplesLock sync.Mutex
	var requests sync.WaitGroup
	var firstError error
	var failureLock sync.Mutex
	deadline := time.NewTimer(time.Duration(seconds) * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	emails := make([]string, 20000)
	for index := range emails {
		emails[index] = fmt.Sprintf("r%07d", index)
	}
	nextIdentity := 0
measure:
	for {
		select {
		case <-deadline.C:
			break measure
		case <-ticker.C:
			for request := 0; request < 20; request++ {
				email := emails[nextIdentity%len(emails)]
				nextIdentity++
				arrived := time.Now()
				requests.Add(1)
				go func() {
					defer requests.Done()
					_, _, release, err := runtime.Attach(context.Background(), featurelimit.SessionInfo{Email: email})
					elapsed := float64(time.Since(arrived).Nanoseconds()) / 1e6
					if err != nil {
						failureLock.Lock()
						if firstError == nil {
							firstError = err
						}
						failureLock.Unlock()
						return
					}
					release()
					samplesLock.Lock()
					values = append(values, elapsed)
					samplesLock.Unlock()
				}()
			}
		}
	}
	requests.Wait()
	if firstError != nil {
		return nil, firstError
	}

	pageValues := make([]float64, 100)
	for index := range pageValues {
		start := time.Now()
		page, total := runtime.ListRules(50000, 50)
		pageValues[index] = float64(time.Since(start).Nanoseconds()) / 1e6
		if len(page) != 50 || total != 1000000 {
			return nil, fmt.Errorf("规则分页数量不符")
		}
	}
	page := distribution("limit_page_1000000", "ms", pageValues)
	page.Note = "百万规则（比目标十万更大）；一页 50 条"
	return []measurement{distribution("attach_during_tick_1000000_20000_active", "ms", values), page}, nil
}

func routeBenchmark() ([]measurement, error) {
	config := &router.Config{}
	for index := 0; index < 100000; index++ {
		config.Rule = append(config.Rule, &router.RoutingRule{RuleTag: fmt.Sprintf("r%d", index), UserEmail: []string{fmt.Sprintf("v%d", index)}, TargetTag: &router.RoutingRule_Tag{Tag: "direct"}})
	}
	var runtime router.Router
	if err := runtime.Init(context.Background(), config, nil, nil, nil); err != nil {
		return nil, err
	}
	defer runtime.Close()
	var adds, removes []float64
	for index := 0; index < 100; index++ {
		tag := fmt.Sprintf("new%d", index)
		message := serial.ToTypedMessage(&router.Config{Rule: []*router.RoutingRule{{RuleTag: tag, UserEmail: []string{tag}, TargetTag: &router.RoutingRule_Tag{Tag: "direct"}}}})
		start := time.Now()
		err := runtime.AddRule(message, true)
		adds = append(adds, float64(time.Since(start).Nanoseconds())/1e6)
		if err != nil {
			return nil, err
		}
		start = time.Now()
		err = runtime.RemoveRule(tag)
		removes = append(removes, float64(time.Since(start).Nanoseconds())/1e6)
		if err != nil {
			return nil, err
		}
	}
	return []measurement{distribution("route_add_100000", "ms", adds), distribution("route_remove_100000", "ms", removes)}, nil
}

func pageBenchmark() ([]measurement, error) {
	rules := make([]*limit.Rule, 100000)
	for index := range rules {
		tag := fmt.Sprintf("r%07d", index)
		rules[index] = &limit.Rule{RuleTag: tag, Profile: "p", User: []string{tag}}
	}
	runtime, err := limit.New(&limit.Config{Profiles: []*limit.Profile{{Name: "p"}}, Rules: rules})
	if err != nil {
		return nil, err
	}
	defer runtime.Close()
	values := make([]float64, 100)
	for index := range values {
		start := time.Now()
		page, total := runtime.ListRules(50000, 50)
		values[index] = float64(time.Since(start).Nanoseconds()) / 1e6
		if len(page) != 50 || total != 100000 {
			return nil, fmt.Errorf("规则分页数量不符")
		}
	}
	return []measurement{distribution("limit_page_100000", "ms", values)}, nil
}
