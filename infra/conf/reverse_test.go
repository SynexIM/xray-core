package conf_test

import (
	"testing"

	"github.com/xtls/xray-core/app/reverse"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/infra/conf"
)

func TestReverseConfig(t *testing.T) {
	creator := func() conf.Buildable {
		return new(conf.ReverseConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"bridges": [{
					"tag": "test",
					"domain": "test.example.com"
				}]
			}`,
			Parser: loadJSON(creator),
			Output: &reverse.Config{
				BridgeConfig: []*reverse.BridgeConfig{
					{Tag: "test", Domain: "test.example.com"},
				},
			},
		},
		{
			Input: `{
				"portals": [{
					"tag": "test",
					"domain": "test.example.com"
				}]
			}`,
			Parser: loadJSON(creator),
			Output: &reverse.Config{
				PortalConfig: []*reverse.PortalConfig{
					{Tag: "test", Domain: "test.example.com"},
				},
			},
		},
	})
}

func TestTopLevelReverseConfigBuildsRuntimeApp(t *testing.T) {
	cfg := &conf.Config{
		Reverse: &conf.ReverseConfig{
			Bridges: []conf.BridgeConfig{{Tag: "bridge-a", Domain: "a.reverse"}},
			Portals: []conf.PortalConfig{{Tag: "portal-a", Domain: "a.reverse"}},
		},
	}
	built, err := cfg.Build()
	common.Must(err)

	for _, app := range built.App {
		if app.Type != serial.GetMessageType(&reverse.Config{}) {
			continue
		}
		message, err := app.GetInstance()
		common.Must(err)
		got := message.(*reverse.Config)
		if len(got.BridgeConfig) != 1 || got.BridgeConfig[0].Tag != "bridge-a" {
			t.Fatalf("reverse bridge missing from runtime app: %+v", got.BridgeConfig)
		}
		if len(got.PortalConfig) != 1 || got.PortalConfig[0].Tag != "portal-a" {
			t.Fatalf("reverse portal missing from runtime app: %+v", got.PortalConfig)
		}
		return
	}
	t.Fatal("top-level reverse config did not produce a runtime reverse app")
}
