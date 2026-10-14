// 固定场景的真实进程基准；每个规模场景在独立进程中运行，避免上一场景的堆影响内存数字。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/xtls/xray-core/common/log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type measurement struct {
	Name    string   `json:"name"`
	Unit    string   `json:"unit"`
	Value   *float64 `json:"value,omitempty"`
	P50     *float64 `json:"p50,omitempty"`
	P99     *float64 `json:"p99,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	Samples int      `json:"samples,omitempty"`
	Error   string   `json:"error,omitempty"`
	Note    string   `json:"note,omitempty"`
}
type report struct {
	Time         string        `json:"time"`
	Platform     string        `json:"platform"`
	Go           string        `json:"go"`
	CPUs         int           `json:"cpus"`
	Revision     string        `json:"revision"`
	Measurements []measurement `json:"measurements"`
}
type options struct {
	mode, output, binary, source, profiles string
	seconds, users, concurrency            int
}

func number(value float64) *float64 { return &value }
func distribution(name, unit string, values []float64) measurement {
	sort.Float64s(values)
	if len(values) == 0 {
		return measurement{Name: name, Unit: unit, Error: "没有成功样本"}
	}
	return measurement{Name: name, Unit: unit, P50: number(values[len(values)/2]), P99: number(values[min(len(values)-1, len(values)*99/100)]), Max: number(values[len(values)-1]), Samples: len(values)}
}
func scalar(name, unit string, value float64) measurement {
	return measurement{Name: name, Unit: unit, Value: number(value)}
}

func writeReport(path string, rows []measurement) error {
	revision := os.Getenv("XRAY_PERF_REVISION")
	if revision == "" {
		return fmt.Errorf("必须设置 XRAY_PERF_REVISION 为受测提交")
	}
	result := report{Time: time.Now().UTC().Format(time.RFC3339), Platform: runtime.GOOS + "/" + runtime.GOARCH, Go: runtime.Version(), CPUs: runtime.NumCPU(), Revision: strings.TrimSpace(revision), Measurements: rows}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, "baseline.json"), append(data, '\n'), 0644); err != nil {
		return err
	}
	var table strings.Builder
	fmt.Fprintf(&table, "# 数据面基准\n\n%s · %s · %s · %d CPU · `%s`\n\n| 指标 | 数值 / p50 | p99 | max | 单位 | 样本 / 限制 |\n|---|---:|---:|---:|---|---|\n", result.Time, result.Platform, result.Go, result.CPUs, result.Revision)
	format := func(value *float64) string {
		if value == nil {
			return "—"
		}
		return fmt.Sprintf("%.4f", *value)
	}
	for _, row := range rows {
		value := row.Value
		if value == nil {
			value = row.P50
		}
		note := row.Note
		if row.Error != "" {
			note = "未测成：" + row.Error
		}
		fmt.Fprintf(&table, "| %s | %s | %s | %s | %s | %d %s |\n", row.Name, format(value), format(row.P99), format(row.Max), row.Unit, row.Samples, note)
	}
	return os.WriteFile(filepath.Join(path, "baseline.md"), []byte(table.String()), 0644)
}

func run(options options) ([]measurement, error) {
	switch options.mode {
	case "auth":
		return authBenchmark()
	case "memory":
		return memoryBenchmark(options.users)
	case "runtime":
		return runtimeBenchmark(options.seconds)
	case "route":
		return routeBenchmark()
	case "page":
		return pageBenchmark()
	case "combine":
		var rows []measurement
		for _, name := range []string{"auth", "memory-100000", "memory-1000000", "route", "runtime", "page", "network"} {
			data, err := os.ReadFile(filepath.Join(options.output, "-mode-"+name+".json"))
			if err != nil {
				return nil, err
			}
			var measured []measurement
			if err := json.Unmarshal(data, &measured); err != nil {
				return nil, err
			}
			rows = append(rows, measured...)
		}
		return rows, nil
	case "network":
		return networkBenchmark(options)
	case "client":
		return clientBenchmark(options)
	case "profile":
		return nil, collectNodeProfiles(options)
	case "target":
		return nil, serveTarget()
	case "all":
		var rows []measurement
		for _, arguments := range [][]string{{"-mode", "auth"}, {"-mode", "memory", "-users", "100000"}, {"-mode", "memory", "-users", "1000000"}, {"-mode", "route"}, {"-mode", "runtime", "-seconds", fmt.Sprint(options.seconds)}, {"-mode", "page"}, {"-mode", "network", "-xray", options.binary, "-seconds", fmt.Sprint(options.seconds)}} {
			fmt.Fprintln(os.Stderr, "运行", strings.Join(arguments, " "))
			command := exec.CommandContext(context.Background(), os.Args[0], arguments...)
			command.Stderr = os.Stderr
			data, err := command.Output()
			if err != nil {
				return nil, fmt.Errorf("%v: %w", arguments, err)
			}
			var measured []measurement
			if err := json.Unmarshal(data, &measured); err != nil {
				return nil, err
			}
			if options.output != "" {
				if err := os.MkdirAll(options.output, 0755); err != nil {
					return nil, err
				}
				name := strings.Join(arguments[:2], "-")
				if arguments[1] == "memory" {
					name += "-" + arguments[3]
				}
				if err := os.WriteFile(filepath.Join(options.output, name+".json"), append(data, '\n'), 0644); err != nil {
					return nil, err
				}
			}
			rows = append(rows, measured...)
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("未知场景 %q", options.mode)
	}
}
func main() {
	log.ReplaceWithSeverityLogger(log.Severity_Warning)
	var options options
	flag.StringVar(&options.mode, "mode", "all", "all/auth/memory/runtime/route/network/client/target")
	flag.StringVar(&options.output, "output", "", "结果目录；省略时输出 JSON")
	flag.StringVar(&options.binary, "xray", "/work/.perf-xray-linux", "真实 Xray")
	flag.StringVar(&options.source, "source", "", "客户端子场景")
	flag.StringVar(&options.profiles, "profiles", "", "建连两侧 profile 目录；节点使用 profiled 二进制")
	flag.IntVar(&options.seconds, "seconds", 60, "测量秒数")
	flag.IntVar(&options.users, "users", 1000000, "内存装载人数")
	flag.IntVar(&options.concurrency, "concurrency", 2048, "新建连接并发")
	flag.Parse()
	if options.seconds < 1 || options.users < 1 || options.concurrency < 1 {
		fmt.Fprintln(os.Stderr, "参数必须为正数")
		os.Exit(1)
	}
	rows, err := run(options)
	if err == nil && options.output != "" {
		err = writeReport(options.output, rows)
	} else if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(rows)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
