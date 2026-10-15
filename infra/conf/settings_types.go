package conf

// fork 能力：按协议名列出入站、出站 settings 的配置结构（各一个零值），供控制面从同一份
// 注册表反射出带类型的配置外形。协议表与运行时加载用的是同一张，fork 增减协议时外形自动跟随。

// InboundSettingsTypes 返回入站协议名 → settings 结构的零值。
func InboundSettingsTypes() map[string]any { return inboundConfigLoader.cache.types() }

// OutboundSettingsTypes 返回出站协议名 → settings 结构的零值。
func OutboundSettingsTypes() map[string]any { return outboundConfigLoader.cache.types() }

func (v ConfigCreatorCache) types() map[string]any {
	out := make(map[string]any, len(v))
	for id, create := range v {
		out[id] = create()
	}
	return out
}
