package conf_test

import (
	"testing"

	"github.com/xtls/xray-core/infra/conf"
)

// api 的 services 是一串字符串对照表。每个名字都要真的映射出一个服务；
// 写错的名字必须报错，不能静默忽略（否则调用方以为开了，节点上其实没有）。
func TestAPIServiceNames(t *testing.T) {
	names := []string{
		"reflectionservice",
		"handlerservice",
		"limitservice",
		"loggerservice",
		"statsservice",
		"observatoryservice",
		"routingservice",
		"accesslogservice",
		"reverseservice",
		"policyservice",
		"capabilityservice",
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

	// 反面：不认识的名字直接报错。
	cfg := &conf.APIConfig{Tag: "api", Services: []string{"nosuchservice"}}
	if _, err := cfg.Build(); err == nil {
		t.Fatal("不认识的服务名没有报错")
	}
}
