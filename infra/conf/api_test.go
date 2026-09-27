package conf_test

import (
	"testing"

	"github.com/xtls/xray-core/infra/conf"
)

// api 的 services 是一串字符串对照表。写错一个字母不会报错：
// 服务只是没注册，API 调用方收到 "unimplemented"，而配置看起来完全正常。
// 所以把每个名字都过一遍，确认它真的映射出了一个服务。
func TestAPIServiceNames(t *testing.T) {
	names := []string{
		"reflectionservice",
		"handlerservice",
		"loggerservice",
		"statsservice",
		"observatoryservice",
		"routingservice",
		"accesslogservice",
		"fairshareservice",
		"reverseservice",
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			cfg := &conf.APIConfig{Tag: "api", Services: []string{name}}
			built, err := cfg.Build()
			if err != nil {
				t.Fatalf("Build 失败：%v", err)
			}
			if len(built.Service) != 1 {
				t.Fatalf("%q 没映射出服务——API 调用方会收到 unimplemented", name)
			}
			if built.Service[0].Type == "" {
				t.Fatalf("%q 映射出了一个空类型", name)
			}
		})
	}

	// 反面：不认识的名字要被丢掉，不能凭空冒出一个服务。
	cfg := &conf.APIConfig{Tag: "api", Services: []string{"nosuchservice"}}
	built, err := cfg.Build()
	if err != nil {
		t.Fatalf("Build 失败：%v", err)
	}
	if len(built.Service) != 0 {
		t.Errorf("不认识的服务名却建出了 %d 个服务", len(built.Service))
	}
}
