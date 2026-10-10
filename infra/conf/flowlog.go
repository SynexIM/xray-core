package conf

import (
	"github.com/xtls/xray-core/app/flowlog"
	"github.com/xtls/xray-core/common/errors"
)

// FlowLogConfig 是 "flowLog" 段：每条连接一条审计记录写到本地 JSON 行文件。
type FlowLogConfig struct {
	Path           string `json:"path"`
	MaxSizeMB      uint32 `json:"maxSizeMB"`
	MaxFiles       uint32 `json:"maxFiles"`
	InterimSeconds uint32 `json:"interimSeconds"`
}

func (c *FlowLogConfig) Build() (*flowlog.Config, error) {
	if c.Path == "" {
		return nil, errors.New("flowLog.path 必填")
	}
	return &flowlog.Config{Path: c.Path, MaxSizeMb: c.MaxSizeMB, MaxFiles: c.MaxFiles, InterimSeconds: c.InterimSeconds}, nil
}
