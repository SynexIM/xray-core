package conf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/xtls/xray-core/proxy/vless"
)

// DecodeStrict 只供控制面准入使用，保留命令行现有的宽松解析行为。
func DecodeStrict(data []byte) (*Config, error) {
	c := new(Config)
	if err := DecodeStrictValue(data, c); err != nil {
		return nil, err
	}
	for i, in := range c.InboundConfigs {
		if err := strictSettings(in.Settings, in.Protocol, inboundConfigLoader, fmt.Sprintf("$.inbounds[%d].settings", i)); err != nil {
			return nil, err
		}
	}
	for i, out := range c.OutboundConfigs {
		if err := strictSettings(out.Settings, out.Protocol, outboundConfigLoader, fmt.Sprintf("$.outbounds[%d].settings", i)); err != nil {
			return nil, err
		}
	}
	if c.RouterConfig != nil {
		for i, rule := range c.RouterConfig.RuleList {
			if err := strictShape(rule, reflect.TypeFor[strictRoutingRule](), fmt.Sprintf("$.routing.rules[%d]", i)); err != nil {
				return nil, err
			}
		}
	}
	return c, nil
}

// DecodeStrictValue 同时检查嵌套路径；RawMessage 的协议形状由 DecodeStrict 再检查。
func DecodeStrictValue(data []byte, target any) error {
	t := reflect.TypeOf(target)
	if t == nil || t.Kind() != reflect.Pointer {
		return fmt.Errorf("$: 解码目标必须是指针")
	}
	if err := strictShape(data, t.Elem(), "$"); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return fmt.Errorf("$: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("$: JSON 后有多余内容")
	}
	return nil
}

var rawType = reflect.TypeFor[json.RawMessage]()

func jsonFields(t reflect.Type, fields map[string]reflect.Type) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name == "-" {
			continue
		}
		if f.Anonymous && name == "" {
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				jsonFields(ft, fields)
				continue
			}
		}
		if name == "" {
			name = f.Name
		}
		fields[name] = f.Type
	}
}

func strictShape(data []byte, t reflect.Type, path string) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == rawType || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	switch t.Kind() {
	case reflect.Struct:
		// Address、PortRange 等原生类型接受字符串或数字；仍交给其原生解码器检查值。
		if len(bytes.TrimSpace(data)) == 0 || bytes.TrimSpace(data)[0] != '{' {
			return nil
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(data, &obj); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		fields := map[string]reflect.Type{}
		jsonFields(t, fields)
		for key, value := range obj {
			ft, ok := fields[key]
			if !ok {
				return fmt.Errorf("%s.%s: json: unknown field %q", path, key, key)
			}
			if err := strictShape(value, ft, path+"."+key); err != nil {
				return err
			}
		}
	case reflect.Map:
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(data, &obj); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for key, value := range obj {
			if err := strictShape(value, t.Elem(), path+"."+key); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if len(bytes.TrimSpace(data)) == 0 || bytes.TrimSpace(data)[0] != '[' {
			return nil
		}
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for i, value := range items {
			if err := strictShape(value, t.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// 协议对象沿用 loader 的注册表，新增协议不会落入另一个分支表。
func strictSettings(raw *json.RawMessage, protocol string, loader *JSONConfigLoader, path string) error {
	if raw == nil {
		return nil
	}
	target, err := loader.cache.CreateConfig(strings.ToLower(protocol))
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if err := strictShape(*raw, reflect.TypeOf(target), path); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(*raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	// VLESS/VMess 的账户经过 RawMessage 二次解码，必须显式检查合并后的 User 外形。
	accountTypes := map[string]reflect.Type{"vless": reflect.TypeFor[vless.Account](), "vmess": reflect.TypeFor[VMessAccount](), "socks": reflect.TypeFor[SocksAccount](), "http": reflect.TypeFor[HTTPAccount]()}
	accountType, ok := accountTypes[strings.ToLower(protocol)]
	if !ok {
		return nil
	}
	fields := map[string]reflect.Type{"email": reflect.TypeFor[string](), "level": reflect.TypeFor[uint32]()}
	jsonFields(accountType, fields)
	return strictAccounts(*raw, fields, path)
}

func strictAccounts(data []byte, fields map[string]reflect.Type, path string) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	for _, name := range []string{"users", "clients", "accounts"} {
		if raw, ok := obj[name]; ok {
			var users []map[string]json.RawMessage
			if err := json.Unmarshal(raw, &users); err != nil {
				return fmt.Errorf("%s.%s: %w", path, name, err)
			}
			for i, u := range users {
				for key, value := range u {
					ft, exists := fields[key]
					p := fmt.Sprintf("%s.%s[%d].%s", path, name, i, key)
					if !exists {
						return fmt.Errorf("%s: json: unknown field %q", p, key)
					}
					if err := strictShape(value, ft, p); err != nil {
						return err
					}
				}
			}
		}
	}
	for _, name := range []string{"vnext", "servers"} {
		if raw, ok := obj[name]; ok {
			var items []json.RawMessage
			if err := json.Unmarshal(raw, &items); err != nil {
				return err
			}
			for i, item := range items {
				if err := strictAccounts(item, fields, fmt.Sprintf("%s.%s[%d]", path, name, i)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type strictRoutingRule struct {
	RouterRule
	Type       string             `json:"type"`
	Domain     *StringList        `json:"domain"`
	Domains    *StringList        `json:"domains"`
	IP         *StringList        `json:"ip"`
	Port       *PortList          `json:"port"`
	Network    *NetworkList       `json:"network"`
	SourceIP   *StringList        `json:"sourceIP"`
	Source     *StringList        `json:"source"`
	SourcePort *PortList          `json:"sourcePort"`
	User       *StringList        `json:"user"`
	VlessRoute *PortList          `json:"vlessRoute"`
	InboundTag *StringList        `json:"inboundTag"`
	Protocols  *StringList        `json:"protocol"`
	Attributes map[string]string  `json:"attrs"`
	LocalIP    *StringList        `json:"localIP"`
	LocalPort  *PortList          `json:"localPort"`
	Process    *StringList        `json:"process"`
	LocalOS    *StringList        `json:"localOS"`
	Webhook    *WebhookRuleConfig `json:"webhook"`
}
