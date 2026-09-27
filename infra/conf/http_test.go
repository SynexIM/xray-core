package conf_test

import (
	"testing"

	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/http"
)

func TestHTTPServerConfig(t *testing.T) {
	creator := func() Buildable {
		return new(HTTPServerConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"accounts": [
					{
						"user": "my-username",
						"pass": "my-password"
					}
				],
				"allowTransparent": true,
				"userLevel": 1
			}`,
			Parser: loadJSON(creator),
			// 这个 fork 把静态账号从 `accounts`（map<string,string>）搬到了
			// `user_accounts`（带限速字段的 repeated UserAccount）——map 里
			// 塞不下 bandwidth_bps 之类的东西，静态账号就永远限不了速。
			// proto 上 `accounts` 还留着（兼容老配置），但解析器一律产出
			// user_accounts，所以这里的期望值跟着改。
			Output: &http.ServerConfig{
				UserAccounts: []*http.UserAccount{
					{Username: "my-username", Password: "my-password"},
				},
				AllowTransparent: true,
				UserLevel:        1,
			},
		},
	})
}
