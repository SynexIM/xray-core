package conf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/xtls/xray-core/app/limit"
)

type LimitStep struct {
	Rate Bandwidth `json:"rate"`
	For  string    `json:"for"`
}
type LimitHeavy struct {
	WindowSeconds uint32 `json:"windowSeconds"`
	Percent       uint32 `json:"percent"`
}
type LimitCongested struct {
	Weight uint32      `json:"weight"`
	Floor  Bandwidth   `json:"floor"`
	Heavy  *LimitHeavy `json:"heavy"`
}
type LimitProfile struct {
	Up             []LimitStep     `json:"up"`
	Down           []LimitStep     `json:"down"`
	Congested      *LimitCongested `json:"congested"`
	MaxConnections uint32          `json:"maxConnections"`
	MaxSourceIps   uint32          `json:"maxSourceIps"`
}
type LimitCongestion struct {
	Enter       uint32 `json:"enter"`
	Exit        uint32 `json:"exit"`
	HoldSeconds uint32 `json:"holdSeconds"`
}
type LimitNode struct {
	Capacity   Bandwidth        `json:"capacity"`
	Congestion *LimitCongestion `json:"congestion"`
}
type LimitRule struct {
	RuleTag     string   `json:"ruleTag"`
	User        []string `json:"user"`
	InboundTag  []string `json:"inboundTag"`
	OutboundTag []string `json:"outboundTag"`
	Profile     string   `json:"profile"`
}
type LimitProfiles map[string]*LimitProfile

// JSON 对象的重复键不能被 map 静默覆盖，否则同名 profile 的准入校验失效。
func (p *LimitProfiles) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*p = nil
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("limit.profiles: 必须是对象")
	}
	result := LimitProfiles{}
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return err
		}
		name := token.(string)
		if _, exists := result[name]; exists {
			return fmt.Errorf("limit.profiles.%s: 名称重复", name)
		}
		var profile *LimitProfile
		if err = d.Decode(&profile); err != nil {
			return fmt.Errorf("limit.profiles.%s: %w", name, err)
		}
		result[name] = profile
	}
	if _, err = d.Token(); err != nil {
		return err
	}
	*p = result
	return nil
}

type LimitConfig struct {
	Node     *LimitNode    `json:"node"`
	Profiles LimitProfiles `json:"profiles"`
	Rules    []*LimitRule  `json:"rules"`
}

// Hysteria 的解析器返回 byte/s；proto 契约保持 bit/s，不能直接写入。
func limitBits(b Bandwidth, path string) (uint64, error) {
	n, err := b.Bps()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", path, err)
	}
	if n > math.MaxUint64/8 {
		return 0, fmt.Errorf("%s: 速率超出范围", path)
	}
	return n * 8, nil
}
func buildLimitLadder(steps []LimitStep, path string) (*limit.Ladder, error) {
	if steps == nil {
		return nil, nil
	}
	l := &limit.Ladder{}
	for i, s := range steps {
		p := fmt.Sprintf("%s[%d]", path, i)
		rate, err := limitBits(s.Rate, p+".rate")
		if err != nil {
			return nil, err
		}
		var ms uint64
		if s.For != "" {
			duration, err := time.ParseDuration(s.For)
			if err != nil || duration <= 0 || duration%time.Millisecond != 0 {
				return nil, fmt.Errorf("%s.for: 时长必须是正整数毫秒", p)
			}
			if i == len(steps)-1 {
				return nil, fmt.Errorf("%s.for: 最后一档不能带时长", p)
			}
			ms = uint64(duration / time.Millisecond)
		}
		l.Steps = append(l.Steps, &limit.Step{RateBps: rate, ForMs: ms})
	}
	if err := limit.ValidateLadder(l, path); err != nil {
		return nil, err
	}
	return l, nil
}
func (n *LimitNode) Build() (*limit.NodeLimit, error) {
	if n == nil {
		return nil, nil
	}
	rate, err := limitBits(n.Capacity, "limit.node.capacity")
	if err != nil {
		return nil, err
	}
	result := &limit.NodeLimit{CapacityBps: rate}
	if n.Congestion != nil {
		result.EnterPercent = n.Congestion.Enter
		result.ExitPercent = n.Congestion.Exit
		result.HoldSeconds = n.Congestion.HoldSeconds
	}
	return result, limit.ValidateNode(result)
}
func (p *LimitProfile) Build(name string) (*limit.Profile, error) {
	if p == nil {
		return nil, fmt.Errorf("limit.profiles.%s: 不能为空", name)
	}
	path := "limit.profiles." + name
	up, err := buildLimitLadder(p.Up, path+".up")
	if err != nil {
		return nil, err
	}
	down, err := buildLimitLadder(p.Down, path+".down")
	if err != nil {
		return nil, err
	}
	result := &limit.Profile{Name: name, Up: up, Down: down, MaxConnections: p.MaxConnections, MaxSourceIps: p.MaxSourceIps}
	if p.Congested != nil {
		floor, err := limitBits(p.Congested.Floor, path+".congested.floor")
		if err != nil {
			return nil, err
		}
		result.Congested = &limit.Congested{Weight: p.Congested.Weight, FloorBps: floor}
		if p.Congested.Heavy != nil {
			result.Congested.HeavyWindowSeconds = p.Congested.Heavy.WindowSeconds
			result.Congested.HeavyPercent = p.Congested.Heavy.Percent
		}
	}
	return result, limit.ValidateProfile(result)
}
func (c *LimitConfig) Build() (*limit.Config, error) {
	node, err := c.Node.Build()
	if err != nil {
		return nil, err
	}
	result := &limit.Config{Node: node}
	names := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		profile, err := c.Profiles[name].Build(name)
		if err != nil {
			return nil, err
		}
		result.Profiles = append(result.Profiles, profile)
	}
	for _, r := range c.Rules {
		if r == nil {
			return nil, fmt.Errorf("limit.rules: 规则不能为空")
		}
		result.Rules = append(result.Rules, &limit.Rule{RuleTag: r.RuleTag, User: r.User, InboundTag: r.InboundTag, OutboundTag: r.OutboundTag, Profile: r.Profile})
	}
	return result, limit.ValidateConfig(result)
}
