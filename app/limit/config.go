package limit

import (
	"fmt"
	"math"
)

// 配置和管理接口共用准入校验，热路径只处理已验证参数。
type ValidationError struct{ Path, Message string }

func (e *ValidationError) Error() string { return e.Path + ": " + e.Message }
func invalid(path, message string) error { return &ValidationError{path, message} }

func ValidateLadder(l *Ladder, path string) error {
	if l == nil {
		return nil
	}
	if len(l.Steps) == 0 {
		return invalid(path, "阶梯至少需要一档")
	}
	var previous uint64
	var totalMs uint64
	for i, s := range l.Steps {
		p := fmt.Sprintf("%s[%d]", path, i)
		if s == nil || s.RateBps == 0 {
			return invalid(p+".rate", "速率必须大于零")
		}
		if i > 0 && s.RateBps >= previous {
			return invalid(p+".rate", fmt.Sprintf("第 %d 档速率不小于第 %d 档", i+1, i))
		}
		if (i == len(l.Steps)-1) != (s.ForMs == 0) {
			return invalid(p+".for", "只有最后一档没有时长，其余档时长必须大于零")
		}
		// 避免毫秒转换成 time.Duration 时溢出。
		if s.ForMs > uint64(math.MaxInt64/1_000_000)-totalMs {
			return invalid(p+".for", "阶梯累计时长超出范围")
		}
		totalMs += s.ForMs
		previous = s.RateBps
	}
	return nil
}
func ValidateProfile(p *Profile) error {
	if p == nil || p.Name == "" {
		return invalid("profile.name", "不能为空")
	}
	path := "profiles." + p.Name
	if err := ValidateLadder(p.Up, path+".up"); err != nil {
		return err
	}
	if err := ValidateLadder(p.Down, path+".down"); err != nil {
		return err
	}
	if p.Congested != nil && p.Congested.HeavyPercent > 100 {
		return invalid(path+".congested.heavy.percent", "不能超过 100")
	}
	return nil
}
func ValidateNode(n *NodeLimit) error {
	if n == nil {
		return nil
	}
	if n.EnterPercent > 100 || n.ExitPercent > n.EnterPercent {
		return invalid("node.congestion", "阈值必须满足 0 ≤ exit ≤ enter ≤ 100")
	}
	return nil
}
func ValidateRule(r *Rule) error {
	if r == nil || r.RuleTag == "" {
		return invalid("rule.ruleTag", "不能为空")
	}
	path := "rules." + r.RuleTag
	if r.Profile == "" {
		return invalid(path+".profile", "不能为空")
	}
	if len(r.User)+len(r.InboundTag)+len(r.OutboundTag) == 0 {
		return invalid(path, "至少需要一个匹配条件")
	}
	for _, list := range [][]string{r.User, r.InboundTag, r.OutboundTag} {
		for _, s := range list {
			if s == "" {
				return invalid(path, "匹配条件不能含空字符串")
			}
		}
	}
	return nil
}
func ValidateConfig(c *Config) error {
	if c == nil {
		return invalid("limit", "不能为空")
	}
	if err := ValidateNode(c.Node); err != nil {
		return err
	}
	names := map[string]bool{}
	for _, p := range c.Profiles {
		if err := ValidateProfile(p); err != nil {
			return err
		}
		if names[p.Name] {
			return invalid("profiles."+p.Name, "名称重复")
		}
		names[p.Name] = true
	}
	tags := map[string]bool{}
	for _, r := range c.Rules {
		if err := ValidateRule(r); err != nil {
			return err
		}
		if !names[r.Profile] {
			return invalid("rules."+r.RuleTag+".profile", "引用的 profile 不存在: "+r.Profile)
		}
		if tags[r.RuleTag] {
			return invalid("rules."+r.RuleTag+".ruleTag", "重复")
		}
		tags[r.RuleTag] = true
	}
	return nil
}
